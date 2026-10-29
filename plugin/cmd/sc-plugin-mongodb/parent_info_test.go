package main

import (
	"encoding/json"
	"testing"

	pApi "github.com/simple-container-com/api/pkg/clouds/pulumi/api"
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
	got, err := decodeParentInfo(raw)
	if err != nil {
		t.Fatal(err)
	}
	if got.StackName != want.StackName || got.FullReference != want.FullReference ||
		got.ParentEnv != want.ParentEnv || got.StackEnv != want.StackEnv {
		t.Fatalf("decoded %+v, want %+v", *got, want)
	}
}

func TestDecodeParentInfoEmpty(t *testing.T) {
	got, err := decodeParentInfo(nil)
	if err != nil || got == nil || got.StackName != "" {
		t.Fatalf("empty input: got %+v, err %v", got, err)
	}
}
