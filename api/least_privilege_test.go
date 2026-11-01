// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package mongodb

import "testing"

func TestNetworkAccessRequiresExplicitOptIn(t *testing.T) {
	no, yes := false, true
	for _, cfg := range []*AtlasNetworkConfig{{}, {AllowAllIps: &no}} {
		if cfg.AllowsAllIPs() {
			t.Fatalf("open by default: %+v", cfg)
		}
	}
	if !(&AtlasNetworkConfig{AllowAllIps: &yes}).AllowsAllIPs() {
		t.Fatal("explicit compatibility opt in ignored")
	}
}

func TestOplogReadersAreScopedToStackAndEnvironment(t *testing.T) {
	cfg := &AtlasConfig{OplogReaders: []string{"meeting/staging"}}
	if !cfg.AllowsOplog("meeting", "staging") {
		t.Fatal("explicit oplog reader rejected")
	}
	for _, identity := range [][2]string{{"meeting", "production"}, {"other", "staging"}, {"meeting", ""}, {"", "staging"}} {
		if cfg.AllowsOplog(identity[0], identity[1]) {
			t.Fatalf("unexpected grant to %v", identity)
		}
	}
	cfg.OplogReaders = []string{"*", "meeting"}
	if cfg.AllowsOplog("meeting", "staging") {
		t.Fatal("wildcard/bare name must not grant access")
	}
}

func TestLegacyPublicAccessAndOptIn(t *testing.T) {
	if !(&AtlasConfig{}).AllowsPublicAccess() {
		t.Fatal("missing networkConfig must retain 0.0.0.0/0")
	}
	if (&AtlasConfig{DisableDefaultPublicAccess: true}).AllowsPublicAccess() {
		t.Fatal("opt in ignored")
	}
	if (&AtlasConfig{NetworkConfig: &AtlasNetworkConfig{}}).AllowsPublicAccess() {
		t.Fatal("explicit networkConfig changed")
	}
}
