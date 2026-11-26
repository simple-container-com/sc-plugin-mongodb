// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package peer

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// fakeAtlas stands in for Atlas's Database Users surface: it performs
// the 401-challenge dance the real API requires (atlas.doDigest refuses
// anything else), records the bodies it is sent, and lets a test choose
// whether POST answers 201 or 409 USER_ALREADY_EXISTS.
type fakeAtlas struct {
	mu             sync.Mutex
	postBodies     []map[string]any
	patchBodies    []map[string]any
	requests       int
	conflictOnPost bool
}

func (f *fakeAtlas) start(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			// Atlas always challenges the first, bodyless probe.
			w.Header().Set("WWW-Authenticate",
				`Digest realm="MMS Public API", domain="/api/atlas", nonce="dcd98b71", algorithm=MD5, qop="auth", stale=false`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)

		f.mu.Lock()
		f.requests++
		switch r.Method {
		case http.MethodPost:
			f.postBodies = append(f.postBodies, body)
		case http.MethodPatch:
			f.patchBodies = append(f.patchBodies, body)
		}
		conflict := f.conflictOnPost
		f.mu.Unlock()

		if r.Method == http.MethodPost && conflict {
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"errorCode":"USER_ALREADY_EXISTS","detail":"exists"}`))
			return
		}
		// Atlas echoes the user WITHOUT the password, which is the whole
		// reason a generated password has only one copy. Reproduce that
		// faithfully — a fake that echoed it back would hide the bug.
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"username":"peer-forge-conductor--staging","databaseName":"admin"}`))
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func (f *fakeAtlas) snapshot() (posts, patches []map[string]any, requests int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.postBodies, f.patchBodies, f.requests
}

func mintOnce(t *testing.T, baseURL, salt string) (MintReply, error) {
	t.Helper()
	ResetCacheForTesting()
	StashCreds(tCluster, ClusterCreds{
		PublicKey:    "pub",
		PrivateKey:   "priv",
		ProjectID:    tProject,
		ClusterURI:   "mongodb+srv://cluster0.example.mongodb.net",
		PasswordSalt: salt,
		BaseURL:      baseURL,
	})
	payload, err := json.Marshal(MintPayload{
		Cluster:       tCluster,
		Database:      tDB,
		RoleName:      "readWrite",
		ConsumerStack: "forge-conductor--staging",
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	out, err := MintHandler(context.Background(), "peer", payload)
	if err != nil {
		return MintReply{}, err
	}
	var reply MintReply
	if err := json.Unmarshal(out, &reply); err != nil {
		t.Fatalf("decode reply: %v", err)
	}
	return reply, nil
}

// TestMintHandler_SendsDerivedPassword — the password Atlas is told and
// the password the caller is handed must both be the derived one. If
// the handler let CreateDatabaseUser invent it, the reply would carry a
// value no second deploy could ever reproduce.
func TestMintHandler_SendsDerivedPassword(t *testing.T) {
	t.Cleanup(ResetCacheForTesting)
	fake := &fakeAtlas{}
	base := fake.start(t)

	reply, err := mintOnce(t, base, tSalt)
	if err != nil {
		t.Fatalf("mint: %v", err)
	}

	want, err := DerivePassword(tSalt, tCluster, tProject, tDB, "peer-forge-conductor--staging")
	if err != nil {
		t.Fatalf("derive: %v", err)
	}
	if reply.Password != want {
		t.Errorf("reply password is not the derived one")
	}
	if !strings.Contains(reply.URI, reply.Password) {
		t.Error("scoped URI must carry the minted password")
	}

	posts, _, _ := fake.snapshot()
	if len(posts) != 1 {
		t.Fatalf("expected one POST to Atlas, got %d", len(posts))
	}
	if got, _ := posts[0]["password"].(string); got != want {
		t.Error("Atlas was sent a password other than the derived one")
	}
}

// TestMintHandler_RepeatedMintsDoNotRotate is the regression test for
// the 2026-11-25 incident. Two deploys of one consumer hit 409
// USER_ALREADY_EXISTS and PATCH; before this change each PATCH carried
// a freshly generated password, so the earlier deploy's stored
// credential stopped working the moment the later deploy ran — and if
// the later deploy's push was lost, nothing on earth held a working
// one. Both PATCH bodies must now carry the same value.
func TestMintHandler_RepeatedMintsDoNotRotate(t *testing.T) {
	t.Cleanup(ResetCacheForTesting)
	fake := &fakeAtlas{conflictOnPost: true}
	base := fake.start(t)

	first, err := mintOnce(t, base, tSalt)
	if err != nil {
		t.Fatalf("first mint: %v", err)
	}
	second, err := mintOnce(t, base, tSalt)
	if err != nil {
		t.Fatalf("second mint: %v", err)
	}
	if first.Password != second.Password {
		t.Error("two mints of the same consumer rotated the password")
	}

	_, patches, _ := fake.snapshot()
	if len(patches) != 2 {
		t.Fatalf("expected two PATCHes (409 recovery each time), got %d", len(patches))
	}
	p0, _ := patches[0]["password"].(string)
	p1, _ := patches[1]["password"].(string)
	if p0 == "" || p0 != p1 {
		t.Errorf("the 409-recovery PATCH rotated the live Atlas password between deploys")
	}
	if p0 != first.Password {
		t.Error("the PATCH sent a password the caller was never told")
	}
}

// TestMintHandler_MissingSalt_RefusesBeforeTouchingAtlas — a missing
// salt must not degrade to the old random behaviour, and must not reach
// Atlas at all: a PATCH that rotates the live password and then fails
// is strictly worse than not starting. The message has to name the
// config key, because the operator's only clue is this string.
func TestMintHandler_MissingSalt_RefusesBeforeTouchingAtlas(t *testing.T) {
	t.Cleanup(ResetCacheForTesting)
	fake := &fakeAtlas{}
	base := fake.start(t)

	_, err := mintOnce(t, base, "")
	if err == nil {
		t.Fatal("a cluster with no scopedUserPasswordSalt must be refused")
	}
	if !strings.Contains(err.Error(), "scopedUserPasswordSalt") {
		t.Errorf("error must name the missing config key, got: %v", err)
	}
	if _, _, requests := fake.snapshot(); requests != 0 {
		t.Errorf("refusal must happen before any Atlas call, saw %d", requests)
	}
}
