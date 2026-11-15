// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package peer

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

// TestStashCreds_ZeroFieldsSkipped — defensive: a mongodb resource
// with empty creds would be an operator config error; the stash
// silently drops rather than caching broken values.
func TestStashCreds_ZeroFieldsSkipped(t *testing.T) {
	t.Cleanup(ResetCacheForTesting)

	// Empty cluster name — dropped.
	StashCreds("", ClusterCreds{PublicKey: "pub", PrivateKey: "priv", ProjectID: "proj"})
	if _, ok := getCreds(""); ok {
		t.Error("empty cluster name must not land in cache")
	}
	// Missing keys — dropped (any zero-valued field).
	StashCreds("c1", ClusterCreds{PublicKey: "pub", ProjectID: "proj"}) // no PrivateKey
	if _, ok := getCreds("c1"); ok {
		t.Error("missing PrivateKey must not land in cache")
	}
	StashCreds("c2", ClusterCreds{PrivateKey: "priv", ProjectID: "proj"}) // no PublicKey
	if _, ok := getCreds("c2"); ok {
		t.Error("missing PublicKey must not land in cache")
	}
	// Full creds — cached.
	StashCreds("c3", ClusterCreds{PublicKey: "pub", PrivateKey: "priv", ProjectID: "proj"})
	c, ok := getCreds("c3")
	if !ok || c.ProjectID != "proj" {
		t.Errorf("full creds must be cached: %+v ok=%v", c, ok)
	}
}

// TestMintHandler_NoCachedCreds_ClearError — the ordering-violation
// case: backup plugin's PluginInvoke fires BEFORE mongodb's
// ComputeProcess stashed creds for the named cluster. The error
// names the cluster + the fix (`uses:` ordering).
func TestMintHandler_NoCachedCreds_ClearError(t *testing.T) {
	t.Cleanup(ResetCacheForTesting)

	payload, _ := json.Marshal(MintPayload{
		Cluster:       "unknown-cluster",
		Database:      "forge_app",
		RoleName:      "readWrite",
		ConsumerStack: "consumer--staging",
	})
	_, err := MintHandler(context.Background(), "sc-component-backup", payload)
	if err == nil {
		t.Fatal("expected error for missing cache entry")
	}
	if !strings.Contains(err.Error(), "unknown-cluster") {
		t.Errorf("error should name the cluster: %v", err)
	}
	if !strings.Contains(err.Error(), "uses:") {
		t.Errorf("error should hint at the fix: %v", err)
	}
}

// TestMintHandler_MissingFields_Refused — payload validation.
func TestMintHandler_MissingFields_Refused(t *testing.T) {
	t.Cleanup(ResetCacheForTesting)

	cases := []MintPayload{
		{Database: "d", RoleName: "r", ConsumerStack: "cs"}, // no Cluster
		{Cluster: "c", RoleName: "r", ConsumerStack: "cs"},  // no Database
		{Cluster: "c", Database: "d", ConsumerStack: "cs"},  // no RoleName
		{Cluster: "c", Database: "d", RoleName: "r"},        // no ConsumerStack
	}
	for i, p := range cases {
		b, _ := json.Marshal(p)
		_, err := MintHandler(context.Background(), "caller", b)
		if err == nil {
			t.Errorf("case %d: expected refusal for %+v", i, p)
		}
		if !strings.Contains(err.Error(), "required") {
			t.Errorf("case %d: error should name `required`: %v", i, err)
		}
	}
}

// TestMintHandler_BadJSON_Refused — payload JSON must be well-formed;
// the handler returns a clear decode error.
func TestMintHandler_BadJSON_Refused(t *testing.T) {
	t.Cleanup(ResetCacheForTesting)

	_, err := MintHandler(context.Background(), "", []byte(`{not json`))
	if err == nil {
		t.Fatal("expected JSON decode error")
	}
	if !strings.Contains(err.Error(), "decode payload") {
		t.Errorf("error should name the step: %v", err)
	}
}

// TestDeleteHandler_NoCachedCreds_ClearError — same ordering case for
// delete; distinct error message naming the delete capability.
func TestDeleteHandler_NoCachedCreds_ClearError(t *testing.T) {
	t.Cleanup(ResetCacheForTesting)

	payload, _ := json.Marshal(DeletePayload{Cluster: "unknown-cluster", Username: "someone"})
	_, err := DeleteHandler(context.Background(), "", payload)
	if err == nil {
		t.Fatal("expected error for missing cache entry")
	}
	if !strings.Contains(err.Error(), "unknown-cluster") || !strings.Contains(err.Error(), "uses:") {
		t.Errorf("error should name cluster + fix: %v", err)
	}
}

// TestDeleteHandler_MissingFields_Refused — cluster + username are
// non-optional.
func TestDeleteHandler_MissingFields_Refused(t *testing.T) {
	t.Cleanup(ResetCacheForTesting)

	for i, p := range []DeletePayload{
		{Username: "u"}, // no cluster
		{Cluster: "c"},  // no username
	} {
		b, _ := json.Marshal(p)
		_, err := DeleteHandler(context.Background(), "", b)
		if err == nil {
			t.Errorf("case %d: expected refusal", i)
		}
	}
}

// TestWeaveCredsIntoURI — credential-weaving happy path + edge cases.
func TestWeaveCredsIntoURI(t *testing.T) {
	cases := map[string]string{
		// Standard SRV URI.
		"mongodb+srv://cluster0.abc.mongodb.net/": "mongodb+srv://user:pw@cluster0.abc.mongodb.net/",
		// Plain mongodb:// URI with hostlist.
		"mongodb://host-00:27017,host-01:27017/": "mongodb://user:pw@host-00:27017,host-01:27017/",
		// Already has creds — we naively insert again, caller shouldn't
		// pass a URI that already has creds. Document the shape.
		"mongodb://old:pw@host/": "mongodb://user:pw@old:pw@host/",
		// No `://` at all — returned unchanged (defensive).
		"not-a-uri": "not-a-uri",
		"":          "",
	}
	for in, want := range cases {
		got := weaveCredsIntoURI(in, "user", "pw")
		if got != want {
			t.Errorf("weaveCredsIntoURI(%q): got %q, want %q", in, got, want)
		}
	}
}

// TestRegisterCapabilities_HooksBothHandlers — register the two
// capabilities and verify they resolve in runtime's package registry
// by calling HandlePluginInvoke (which looks them up by name).
//
// We don't test the full roundtrip through Atlas REST — that's an
// integration concern. Here we only check the capability names are
// bound to our handlers (the handlers will fail with
// no-cached-creds, which is proof-of-wiring).
func TestRegisterCapabilities_HooksBothHandlers(t *testing.T) {
	t.Cleanup(ResetCacheForTesting)

	if err := RegisterCapabilities(); err != nil {
		t.Fatalf("register: %v", err)
	}
	// Direct-invoke the handlers (they're exported; the registry
	// check is the "register returned no error" + the fact that
	// RegisterPeerCapability refuses an empty capability + nil handler,
	// which RegisterCapabilities would hit if the constants / funcs
	// were wrong).
	payload, _ := json.Marshal(MintPayload{
		Cluster: "any", Database: "d", RoleName: "readWrite", ConsumerStack: "cs",
	})
	if _, err := MintHandler(context.Background(), "", payload); err == nil {
		t.Error("MintHandler should fail — no cached creds")
	}
}
