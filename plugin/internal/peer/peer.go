// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

// Package peer registers sc-plugin-mongodb's peer-invoke capabilities
// (mint-scoped-user, delete-scoped-user) and provides their handlers.
//
// # Design
//
// A calling plugin (e.g. sc-component-backup) asks SC core's broker to
// mint an Atlas database user scoped to one database. The broker routes
// the call to this plugin's SCCloudPlugin.PluginInvoke; runtime's
// HandlePluginInvoke switches on capability name and runs the handler
// from this package.
//
// # Credential plumbing
//
// Atlas API creds live on the operator-defined mongodb-atlas resource
// config and arrive at this plugin during Provision AND during
// ComputeProcess. The peer-invoke call fires on a CONSUMER deploy, not
// on parent provision — but SC core processes `uses:` siblings in
// declaration order, and the mongodb plugin's ComputeProcess fires
// BEFORE the backup component's (which is the typical layout:
// `uses: [the-mongo, my-backup]`). At the moment backup calls
// `peer.PluginInvoke("mongodb", "mint-scoped-user", ...)`, mongodb's
// ComputeProcess has already run in the same plugin subprocess and
// stashed the creds via StashCreds.
//
// This is the "plugin-process state cache that spans ComputeProcess →
// PluginInvoke within one subprocess lifetime" model. Alternatives
// (payload-carries-creds / parent-outputs-via-StackReference / SC-core
// plumbs-via-header) are listed in the roadmap item
// sc-components-plugin-peer-invoke.md; this one is picked because:
//
//   - It keeps the capability payload minimal (no creds on the wire).
//   - It requires no api changes beyond what PR #616 shipped.
//   - It matches the natural `uses:` ordering of consumer deploys.
//
// Ordering guard: if the backup component is declared BEFORE the mongo
// sibling in `uses:`, mongodb's ComputeProcess hasn't run yet and the
// cache is empty. The handler returns a diagnosable error naming the
// cluster + the fix ("declare the mongodb sibling before the backup
// component in your `uses:` list"). Same ordering caveat as the slice
// 1d.7 auto-discover path.
package peer

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/pkg/errors"
	"github.com/simple-container-com/api/pkg/plugin/runtime"

	"github.com/simple-container-com/sc-plugin-mongodb/plugin/internal/atlas"
)

// MintScopedUser is the capability name — matches the string
// advertised in SchemaResponse.peer_capabilities + the token callers
// pass on PeerClient.PluginInvoke.
const (
	MintScopedUser   = "mint-scoped-user"
	DeleteScopedUser = "delete-scoped-user"
)

// MintPayload is the JSON shape a peer plugin passes on the
// `mint-scoped-user` call. The cluster field selects which mongodb
// resource's cached creds to use — a fleet with multiple mongodb
// clusters routes by this name.
type MintPayload struct {
	// Cluster is the mongodb resource name (e.g. "the-mongo",
	// "forge-infra-mongo"). Must match the mongodb-atlas resource's
	// SC Name in the parent stack; the cred cache is keyed by it.
	Cluster string `json:"cluster"`
	// Database the scoped user needs access to. Written into the
	// user's `readWrite` role scope.
	Database string `json:"database"`
	// RoleName — the Atlas built-in role granted. "readWrite" is the
	// expected value for backup; other callers (monitoring) may pass
	// "read" or "readAnyDatabase".
	RoleName string `json:"roleName"`
	// ConsumerStack identifies the caller's stack for audit +
	// username derivation (e.g. "forge-conductor--staging").
	ConsumerStack string `json:"consumerStack"`
}

// MintReply is the JSON shape returned to the calling plugin. The
// username + password combine with the cluster's connection URI
// (looked up from the cache) to form a ready-to-use mongo URI.
type MintReply struct {
	Username string `json:"username"`
	Password string `json:"password"`
	// URI is the scoped mongodb:// URI the caller can use directly —
	// username + password woven into the host string.
	URI string `json:"uri"`
}

// DeletePayload is the JSON shape for `delete-scoped-user`. Matches
// the Atlas REST endpoint's needs: project + username + the auth DB
// (always "admin" for Atlas API-created users today).
type DeletePayload struct {
	Cluster  string `json:"cluster"`
	Username string `json:"username"`
}

// DeleteReply is empty — the delete is a side-effect-only call.
type DeleteReply struct{}

// ClusterCreds carries one mongodb cluster's Atlas API credentials +
// its user-facing connection URI. Stashed at ComputeProcess time and
// read at PluginInvoke time.
type ClusterCreds struct {
	PublicKey  string
	PrivateKey string
	ProjectID  string
	// ClusterURI is the mongodb+srv:// base URL for this cluster
	// (without auth) — handlers weave in the scoped user's creds.
	ClusterURI string
}

// credCache is the process-wide map from cluster name to Atlas creds.
// Populated by StashCreds from the mongodb plugin's ComputeProcess;
// read by MintHandler + DeleteHandler. Lifetime = the mongodb plugin
// subprocess's lifetime = the duration of one SC deploy.
var credCache = struct {
	mu sync.RWMutex
	m  map[string]ClusterCreds
}{m: map[string]ClusterCreds{}}

// StashCreds records a cluster's Atlas creds so later peer-invoke
// handlers can reach them. Called from the mongodb plugin's
// ComputeProcess after decoding its AtlasConfig.
//
// Zero-fielded values are silently rejected (a fleet-wide invariant:
// Atlas creds are non-optional for every mongodb resource).
func StashCreds(clusterName string, c ClusterCreds) {
	if clusterName == "" || c.PublicKey == "" || c.PrivateKey == "" || c.ProjectID == "" {
		return
	}
	credCache.mu.Lock()
	credCache.m[clusterName] = c
	credCache.mu.Unlock()
}

// getCreds reads a cluster's stashed creds. Returns ok=false when the
// cluster hasn't had ComputeProcess fire yet in this subprocess —
// usually a `uses:` ordering problem on the consumer side.
func getCreds(clusterName string) (ClusterCreds, bool) {
	credCache.mu.RLock()
	c, ok := credCache.m[clusterName]
	credCache.mu.RUnlock()
	return c, ok
}

// RegisterCapabilities wires the peer-invoke handlers at plugin
// startup. Called from `main.go`'s init path. Decoupled from init() so
// a plugin author sees the registration explicitly + tests can
// register-then-reset.
func RegisterCapabilities() error {
	if err := runtime.RegisterPeerCapability(MintScopedUser, MintHandler); err != nil {
		return errors.Wrap(err, "register mint-scoped-user")
	}
	if err := runtime.RegisterPeerCapability(DeleteScopedUser, DeleteHandler); err != nil {
		return errors.Wrap(err, "register delete-scoped-user")
	}
	return nil
}

// MintHandler is the capability handler for `mint-scoped-user`.
// Decodes the payload, looks up the target cluster's creds, calls
// Atlas REST to create a database user, weaves the creds into the
// cluster URI, and returns the scoped user's credentials.
func MintHandler(ctx context.Context, callingPlugin string, payloadJSON []byte) ([]byte, error) {
	var req MintPayload
	if err := json.Unmarshal(payloadJSON, &req); err != nil {
		return nil, errors.Wrap(err, "mint-scoped-user: decode payload")
	}
	if req.Cluster == "" || req.Database == "" || req.RoleName == "" || req.ConsumerStack == "" {
		return nil, errors.New("mint-scoped-user: cluster, database, roleName, consumerStack are all required")
	}

	creds, ok := getCreds(req.Cluster)
	if !ok {
		return nil, errors.Errorf("mint-scoped-user: no cached credentials for cluster %q — the mongodb plugin's ComputeProcess for that cluster must fire BEFORE the calling plugin's PluginInvoke. Check the caller's `uses:` ordering (declare the mongodb sibling before the component that calls this capability).", req.Cluster)
	}

	// Username convention: `<calling-plugin>-<consumer-stack>` — short
	// enough for Atlas's 100-char username limit, unique per consumer,
	// greps cleanly in audit logs. callingPlugin is empty today (SC
	// core's broker doesn't stamp it yet — Phase 1e+ work); fall back
	// to the generic prefix.
	userPrefix := callingPlugin
	if userPrefix == "" {
		userPrefix = "peer"
	}
	username := fmt.Sprintf("%s-%s", userPrefix, req.ConsumerStack)
	// Atlas caps usernames at 100 chars; truncate safely without
	// splitting mid-UTF8 (ASCII only here, so byte truncation is
	// fine).
	if len(username) > 100 {
		username = username[:100]
	}

	client := atlas.NewClient(creds.PublicKey, creds.PrivateKey)
	user, err := client.CreateDatabaseUser(ctx, creds.ProjectID, atlas.DatabaseUser{
		Username:     username,
		DatabaseName: "admin",
		Roles: []atlas.Role{{
			RoleName:     req.RoleName,
			DatabaseName: req.Database,
		}},
	})
	if err != nil {
		return nil, errors.Wrapf(err, "mint-scoped-user: Atlas REST CreateDatabaseUser(%s) on cluster %q", username, req.Cluster)
	}

	// Weave the creds into the cluster URI. The cluster URI arrives as
	// `mongodb+srv://<host>` or `mongodb://<hostlist>`; we insert
	// `<user>:<password>@` after the scheme. If the cluster URI was
	// empty in the cache (older subprocess state, unlikely), fall back
	// to a reasonable srv shape; the caller can re-derive if needed.
	scopedURI := weaveCredsIntoURI(creds.ClusterURI, username, user.Password)

	reply := MintReply{Username: username, Password: user.Password, URI: scopedURI}
	return json.Marshal(reply)
}

// DeleteHandler is the capability handler for `delete-scoped-user`.
func DeleteHandler(ctx context.Context, _ string, payloadJSON []byte) ([]byte, error) {
	var req DeletePayload
	if err := json.Unmarshal(payloadJSON, &req); err != nil {
		return nil, errors.Wrap(err, "delete-scoped-user: decode payload")
	}
	if req.Cluster == "" || req.Username == "" {
		return nil, errors.New("delete-scoped-user: cluster + username are required")
	}

	creds, ok := getCreds(req.Cluster)
	if !ok {
		return nil, errors.Errorf("delete-scoped-user: no cached credentials for cluster %q — the mongodb plugin's ComputeProcess for that cluster must fire BEFORE the calling plugin's PluginInvoke. Check `uses:` ordering.", req.Cluster)
	}

	client := atlas.NewClient(creds.PublicKey, creds.PrivateKey)
	if err := client.DeleteDatabaseUser(ctx, creds.ProjectID, req.Username, "admin"); err != nil {
		return nil, errors.Wrapf(err, "delete-scoped-user: Atlas REST DeleteDatabaseUser(%s) on cluster %q", req.Username, req.Cluster)
	}

	return json.Marshal(DeleteReply{})
}

// weaveCredsIntoURI inserts `<user>:<password>@` after the first `://`
// in a mongodb URI. Returns the input unchanged when it has no `://`
// (defensive — never worked a scoped URI into a non-URL cluster
// identifier). Does NOT URL-encode the password; Atlas-generated
// passwords are URL-safe base64, so this is safe for every password
// CreateDatabaseUser returns.
func weaveCredsIntoURI(clusterURI, username, password string) string {
	i := indexOf(clusterURI, "://")
	if i < 0 {
		return clusterURI
	}
	return clusterURI[:i+3] + username + ":" + password + "@" + clusterURI[i+3:]
}

// indexOf is a tiny no-alloc substring search. Factored out so the
// weaving function doesn't drag `strings.Index` into a cold-call path.
func indexOf(s, sep string) int {
	if sep == "" || len(s) < len(sep) {
		return -1
	}
	for i := 0; i+len(sep) <= len(s); i++ {
		if s[i:i+len(sep)] == sep {
			return i
		}
	}
	return -1
}

// ResetCacheForTesting wipes the cred cache. Only for tests.
func ResetCacheForTesting() {
	credCache.mu.Lock()
	credCache.m = map[string]ClusterCreds{}
	credCache.mu.Unlock()
}
