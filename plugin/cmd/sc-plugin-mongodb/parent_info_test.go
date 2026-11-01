package main

import (
	"encoding/json"
	"testing"

	pApi "github.com/simple-container-com/api/pkg/clouds/pulumi/api"
	scpluginv1 "github.com/simple-container-com/api/pkg/plugin/proto/v1"
	"github.com/simple-container-com/api/pkg/plugin/runtime"

	mongoApi "github.com/simple-container-com/sc-plugin-mongodb/api"
)

// Core sends ParentInfo as json.Marshal of the untagged struct; decoding must round-trip.
func TestDecodeParentInfoRoundTripsCoreJSON(t *testing.T) {
	want := pApi.ParentInfo{
		StackName:     "infra",
		FullReference: "forge/infra/staging",
		ParentEnv:     "staging",
		StackEnv:      "staging-ru",
	}
	raw, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := runtime.ParentInfoFromRequest(&scpluginv1.ComputeProcessRequest{ParentInfoJson: raw})
	if err != nil {
		t.Fatal(err)
	}
	if got.StackName != want.StackName || got.FullReference != want.FullReference ||
		got.ParentEnv != want.ParentEnv || got.StackEnv != want.StackEnv {
		t.Fatalf("decoded %+v, want %+v", *got, want)
	}
}

func TestDecodeParentInfoEmpty(t *testing.T) {
	got, err := runtime.ParentInfoFromRequest(&scpluginv1.ComputeProcessRequest{})
	if err != nil {
		t.Fatal(err)
	}
	// Empty parent info returns a nil pointer, matching the runtime library's
	// "no parent on this RPC" semantics.
	if got != nil {
		t.Fatalf("empty input: got %+v, want nil", got)
	}
}

// A staging-ru client using the parent's staging cluster resolves names in the parent env.
func TestResourceInputUsesParentEnv(t *testing.T) {
	req := &scpluginv1.ComputeProcessRequest{
		Header:       &scpluginv1.RequestHeader{StackName: "two-peaks", Environment: "staging-ru"},
		ResourceType: "mongodb-atlas",
		ResourceName: "mongodb",
	}
	in := resourceInputFromComputeProcess(req, &mongoApi.AtlasConfig{}, &pApi.ParentInfo{ParentEnv: "staging"})
	if in.StackParams.Environment != "staging" {
		t.Fatalf("env = %q, want staging", in.StackParams.Environment)
	}
	in = resourceInputFromComputeProcess(req, &mongoApi.AtlasConfig{}, &pApi.ParentInfo{})
	if in.StackParams.Environment != "staging-ru" {
		t.Fatalf("fallback env = %q, want staging-ru", in.StackParams.Environment)
	}
}
