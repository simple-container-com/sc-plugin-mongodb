// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package main

import (
	"encoding/json"
	scpluginv1 "github.com/simple-container-com/api/pkg/plugin/proto/v1"
	mongoApi "github.com/simple-container-com/sc-plugin-mongodb/api"
	"testing"
)

func TestAtlasNetworkAndOplogSettingsCrossScalarTransport(t *testing.T) {
	yes := true
	cidrs := []string{"203.0.113.10/32"}
	want := &mongoApi.AtlasConfig{OplogReaders: []string{"meeting/staging"}, NetworkConfig: &mongoApi.AtlasNetworkConfig{AllowAllIps: &yes, AllowCidrs: &cidrs}}
	raw, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	// Match the older core serializer: only scalars survive.
	inputs := map[string]*scpluginv1.InputValue{}
	for key, value := range fields {
		if text, ok := value.(string); ok {
			inputs[key] = &scpluginv1.InputValue{Value: &scpluginv1.InputValue_StringValue{StringValue: text}}
		}
	}
	got := &mongoApi.AtlasConfig{}
	if err := decodeInputsAs(inputs, got); err != nil {
		t.Fatal(err)
	}
	if !got.AllowsOplog("meeting", "staging") || !got.NetworkConfig.AllowsAllIPs() || got.NetworkConfig.AllowCidrs == nil || (*got.NetworkConfig.AllowCidrs)[0] != cidrs[0] {
		t.Fatalf("settings lost across plugin bus: %+v", got)
	}
}

func TestAtlasRejectsInvalidCompleteConfig(t *testing.T) {
	inputs := map[string]*scpluginv1.InputValue{"atlasConfigJson": {Value: &scpluginv1.InputValue_StringValue{StringValue: "invalid json"}}}
	if err := decodeInputsAs(inputs, &mongoApi.AtlasConfig{}); err == nil {
		t.Fatal("invalid complete config ignored")
	}
}

func TestLegacyAndHardenedNetworkDefaultsCrossScalarTransport(t *testing.T) {
	for _, disable := range []bool{false, true} {
		inputs := map[string]*scpluginv1.InputValue{}
		if disable {
			inputs["disableDefaultPublicAccess"] = &scpluginv1.InputValue{Value: &scpluginv1.InputValue_BoolValue{BoolValue: true}}
		}
		got := &mongoApi.AtlasConfig{}
		if err := decodeInputsAs(inputs, got); err != nil {
			t.Fatal(err)
		}
		if got.NetworkConfig != nil || got.AllowsPublicAccess() == disable {
			t.Fatalf("legacy/scalar default changed: %+v", got)
		}
	}
}
