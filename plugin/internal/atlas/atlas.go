// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

// Package atlas is a thin HTTP-digest-auth client for Atlas's v1.0
// Database Users REST surface. sc-plugin-mongodb's peer-invoke handlers
// (mint-scoped-user, delete-scoped-user) use it to mint scoped users on
// behalf of calling plugins (e.g. sc-component-backup):
//
//   - CreateDatabaseUser(projectID, user) — upsert a user with
//     readWrite on a single DB, auto-generated 32-byte password.
//   - DeleteDatabaseUser(projectID, username, authDB) — tear down when
//     the consumer is destroyed.
//
// NOT an attempt at a full Atlas SDK. Minimum calls, minimum imports.
// mongodb's own Pulumi provider handles cluster + admin-user lifecycle
// at PROVISION time; this REST client is for RUNTIME (per-consumer)
// mint/delete triggered by peer plugins via SCCloudPlugin.PluginInvoke.
//
// Auth: Atlas API-key digest. Key id (public) + private secret come
// from the AtlasConfig this plugin receives during Provision +
// ComputeProcess; the peer.credCache stashes them so the peer-invoke
// handlers can access them on a later RPC from a different plugin.
//
// Originally copied from sc-component-backup's
// `plugin/internal/atlas/atlas.go` as part of the Tier-4 migration
// (sc-components-plugin-peer-invoke): the atlas-admin credential moves
// from the backup component to the mongodb plugin, so the client has
// to live where the credential lives.
package atlas

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"
	"time"
)

// Client is a scoped digest-auth client for one Atlas project.
type Client struct {
	// BaseURL defaults to https://cloud.mongodb.com; override for testing.
	BaseURL string
	// PublicKey + PrivateKey are the API-key id + secret pair.
	PublicKey  string
	PrivateKey string
	// HTTPClient defaults to a sensible one with a 20s per-call timeout.
	HTTPClient *http.Client
}

// NewClient builds a Client with defaults.
func NewClient(publicKey, privateKey string) *Client {
	return &Client{
		BaseURL:    "https://cloud.mongodb.com",
		PublicKey:  publicKey,
		PrivateKey: privateKey,
		HTTPClient: &http.Client{Timeout: 20 * time.Second},
	}
}

// DatabaseUser is the subset of Atlas's DatabaseUser shape we need.
type DatabaseUser struct {
	Username     string `json:"username"`
	Password     string `json:"password,omitempty"`
	DatabaseName string `json:"databaseName"` // auth db, almost always "admin"
	Roles        []Role `json:"roles"`
	GroupID      string `json:"groupId,omitempty"` // Atlas uses groupId as projectId
}

// Role grants a database-scoped role to the user. For a readWrite-on-one-db
// user, use {RoleName: "readWrite", DatabaseName: "<tenant-db>"}.
type Role struct {
	DatabaseName   string `json:"databaseName"`
	RoleName       string `json:"roleName"`
	CollectionName string `json:"collectionName,omitempty"`
}

// CreateDatabaseUser upserts a DatabaseUser in the Atlas project. If
// `user.Password` is empty, generates a cryptographically random 24-char
// password and sets it on the returned struct — the caller is responsible
// for the one-time push of that password to the backup service's tenant
// row. Atlas does NOT return the password on subsequent GETs, so the
// one-shot-create-then-push flow is the design.
//
// Returns the created user (with the generated password populated) or an
// error from Atlas. 409 (user already exists) is NOT auto-recovered; the
// caller should Delete + re-create if they need a fresh password.
func (c *Client) CreateDatabaseUser(ctx context.Context, projectID string, user DatabaseUser) (*DatabaseUser, error) {
	if user.Password == "" {
		pw, err := generatePassword(24)
		if err != nil {
			return nil, fmt.Errorf("generate password: %w", err)
		}
		user.Password = pw
	}
	if user.DatabaseName == "" {
		user.DatabaseName = "admin"
	}
	user.GroupID = projectID
	path := fmt.Sprintf("/api/atlas/v1.0/groups/%s/databaseUsers", projectID)

	body, err := json.Marshal(user)
	if err != nil {
		return nil, fmt.Errorf("marshal user: %w", err)
	}
	resp, err := c.doDigest(ctx, http.MethodPost, path, body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("atlas CreateDatabaseUser: HTTP %d: %s", resp.StatusCode, truncate(string(respBody), 512))
	}
	var created DatabaseUser
	if err := json.Unmarshal(respBody, &created); err != nil {
		return nil, fmt.Errorf("unmarshal atlas response: %w", err)
	}
	// Atlas's response doesn't echo the password; re-attach so the caller
	// has the full create-then-push state.
	created.Password = user.Password
	return &created, nil
}

// DeleteDatabaseUser removes a user by username+authDB from the project.
// NotFound (404) is NOT an error — same as `rm -f` semantics; the caller's
// intent is "this user should not exist here" and Atlas agrees.
func (c *Client) DeleteDatabaseUser(ctx context.Context, projectID, username, authDB string) error {
	if authDB == "" {
		authDB = "admin"
	}
	path := fmt.Sprintf("/api/atlas/v1.0/groups/%s/databaseUsers/%s/%s",
		projectID, authDB, username)
	resp, err := c.doDigest(ctx, http.MethodDelete, path, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil
	}
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("atlas DeleteDatabaseUser: HTTP %d: %s", resp.StatusCode, truncate(string(body), 512))
	}
	return nil
}

// doDigest issues an HTTP request with Atlas's required Digest auth. Atlas
// expects: first request returns 401 + WWW-Authenticate challenge, then
// the second request carries the Authorization header computed from the
// challenge + the body hash.
func (c *Client) doDigest(ctx context.Context, method, path string, body []byte) (*http.Response, error) {
	url := c.BaseURL + path
	// First probe — always 401 + challenge.
	req, err := http.NewRequestWithContext(ctx, method, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("atlas probe: %w", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		return nil, fmt.Errorf("atlas: expected 401 WWW-Authenticate probe, got %d", resp.StatusCode)
	}
	challenge := resp.Header.Get("WWW-Authenticate")
	if challenge == "" {
		return nil, fmt.Errorf("atlas: 401 with no WWW-Authenticate header")
	}
	authz, err := buildDigestAuthz(c.PublicKey, c.PrivateKey, method, path, challenge, body)
	if err != nil {
		return nil, fmt.Errorf("build digest: %w", err)
	}
	// Second call — with Authorization + body.
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req2, err := http.NewRequestWithContext(ctx, method, url, rdr)
	if err != nil {
		return nil, err
	}
	req2.Header.Set("Authorization", authz)
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set("Accept", "application/json")
	return c.HTTPClient.Do(req2)
}

// buildDigestAuthz parses a WWW-Authenticate challenge and emits the
// Authorization value per RFC 2617 (MD5, qop=auth). Atlas uses MD5 — not
// SHA-256 — so this stays at md5 by design.
func buildDigestAuthz(username, password, method, uri, challenge string, body []byte) (string, error) {
	parts := parseDigestChallenge(challenge)
	realm := parts["realm"]
	nonce := parts["nonce"]
	qop := parts["qop"]
	opaque := parts["opaque"]
	if realm == "" || nonce == "" {
		return "", fmt.Errorf("digest challenge missing realm or nonce")
	}

	ha1 := md5sum(username + ":" + realm + ":" + password)
	ha2 := md5sum(method + ":" + uri)
	cnonce := randomCNonce()
	nc := "00000001"

	response := md5sum(ha1 + ":" + nonce + ":" + nc + ":" + cnonce + ":" + qop + ":" + ha2)

	fields := []string{
		fmt.Sprintf(`username="%s"`, username),
		fmt.Sprintf(`realm="%s"`, realm),
		fmt.Sprintf(`nonce="%s"`, nonce),
		fmt.Sprintf(`uri="%s"`, uri),
		"algorithm=MD5",
		fmt.Sprintf(`qop=%s`, qop),
		fmt.Sprintf(`nc=%s`, nc),
		fmt.Sprintf(`cnonce="%s"`, cnonce),
		fmt.Sprintf(`response="%s"`, response),
	}
	if opaque != "" {
		fields = append(fields, fmt.Sprintf(`opaque="%s"`, opaque))
	}
	return "Digest " + strings.Join(fields, ", "), nil
}

// parseDigestChallenge splits a WWW-Authenticate: Digest ... header into
// key→value pairs. Quoted values are unquoted; unknown fields are kept
// (Atlas sends qop + opaque + stale + algorithm — this is lenient).
func parseDigestChallenge(challenge string) map[string]string {
	challenge = strings.TrimPrefix(challenge, "Digest ")
	out := map[string]string{}
	// Split on commas not inside quotes.
	var cur strings.Builder
	inQuote := false
	fields := []string{}
	for _, r := range challenge {
		switch {
		case r == '"':
			inQuote = !inQuote
			cur.WriteRune(r)
		case r == ',' && !inQuote:
			fields = append(fields, cur.String())
			cur.Reset()
		default:
			cur.WriteRune(r)
		}
	}
	if cur.Len() > 0 {
		fields = append(fields, cur.String())
	}
	for _, f := range fields {
		kv := strings.SplitN(strings.TrimSpace(f), "=", 2)
		if len(kv) != 2 {
			continue
		}
		v := strings.TrimSpace(kv[1])
		if len(v) >= 2 && v[0] == '"' && v[len(v)-1] == '"' {
			v = v[1 : len(v)-1]
		}
		out[strings.TrimSpace(kv[0])] = v
	}
	return out
}

func md5sum(s string) string {
	h := md5.Sum([]byte(s))
	return hex.EncodeToString(h[:])
}

func randomCNonce() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// generatePassword returns a URL-safe random password of `length` chars.
// Uses only chars that Atlas accepts in connection strings without
// escaping (no @, :, /, %, ?, etc.).
func generatePassword(length int) (string, error) {
	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"
	out := make([]byte, length)
	max := big.NewInt(int64(len(alphabet)))
	for i := range out {
		n, err := rand.Int(rand.Reader, max)
		if err != nil {
			return "", err
		}
		out[i] = alphabet[n.Int64()]
	}
	return string(out), nil
}

func truncate(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	return s[:limit] + "…"
}
