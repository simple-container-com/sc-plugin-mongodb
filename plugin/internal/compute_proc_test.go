// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package mongodb

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pulumi/pulumi-mongodbatlas/sdk/v3/go/mongodbatlas"
	"github.com/pulumi/pulumi/sdk/v3/go/common/resource"
	sdk "github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/samber/lo"
	"github.com/simple-container-com/api/pkg/api"
	"github.com/simple-container-com/api/pkg/api/logger"
	pApi "github.com/simple-container-com/api/pkg/clouds/pulumi/api"
	mongoApi "github.com/simple-container-com/sc-plugin-mongodb/api"
)

type parentOutputMocks struct {
	mongoCompatibilityMocks
	outputs resource.PropertyMap
}

func (m *parentOutputMocks) NewResource(a sdk.MockResourceArgs) (string, resource.PropertyMap, error) {
	if a.TypeToken == "pulumi:pulumi:StackReference" {
		if a.Inputs["name"].StringValue() != "/alphamind-co/alphamind-co" {
			return "", nil, fmt.Errorf("unexpected parent reference")
		}
		return a.ID, resource.PropertyMap{
			"name": a.Inputs["name"], "outputs": resource.NewObjectProperty(m.outputs),
		}, nil
	}
	return m.mongoCompatibilityMocks.NewResource(a)
}

// These literal export names reproduce 2025.12.6 cluster.go:330-336 and
// util/string.go:9-13, versus the plugin's v2 naming. Expectations must not
// use the naming helpers under test: that would conceal this regression.
func TestClusterComputeProcessorParentOutputs(t *testing.T) {
	const (
		legacyExport = "alphamind---godb--prod-mongo-uri-options"
		modernExport = "alphamind-co--mong-8d9f-mongo-uri-options"
		legacyURI    = "mongodb+srv://legacy.example.net/?retryWrites=true"
		modernURI    = "mongodb+srv://modern.example.net/?retryWrites=true"
	)
	plain := resource.NewStringProperty
	secret := func(s string) resource.PropertyValue { return resource.MakeSecret(plain(s)) }
	for _, tc := range []struct {
		name          string
		version       *int
		customName    string
		adopt         bool
		outputs       resource.PropertyMap
		secretProject bool
		wantHost      string
		wantCluster   string
		wantErr       string
	}{
		{name: "2025.12.6 parent without version", outputs: resource.PropertyMap{legacyExport: plain(legacyURI)}, wantHost: "legacy.example.net", wantCluster: "alphamind---godb--prod"},
		{name: "2025.12.6 parent with v2 config", version: lo.ToPtr(2), outputs: resource.PropertyMap{legacyExport: plain(legacyURI)}, wantHost: "legacy.example.net", wantCluster: "alphamind---godb--prod"},
		{name: "2025.12.6 parent with secret URI", outputs: resource.PropertyMap{legacyExport: secret(legacyURI)}, wantHost: "legacy.example.net", wantCluster: "alphamind---godb--prod"},
		{name: "2025.12.6 ignored custom name", customName: "custom-cluster", outputs: resource.PropertyMap{legacyExport: plain(legacyURI)}, wantHost: "legacy.example.net", wantCluster: "alphamind---godb--prod"},
		{name: "2025.12.6 adopted cluster", customName: "atlas-existing", adopt: true, outputs: resource.PropertyMap{legacyExport: plain(legacyURI)}, wantHost: "legacy.example.net", wantCluster: "atlas-existing"},
		{name: "explicit v1 parent", version: lo.ToPtr(1), outputs: resource.PropertyMap{legacyExport: plain(legacyURI)}, wantHost: "legacy.example.net", wantCluster: "alphamind---godb--prod"},
		{name: "modern parent", outputs: resource.PropertyMap{modernExport: plain(modernURI)}, wantHost: "modern.example.net", wantCluster: "alphamind-co--mong-8d9f"},
		{name: "modern secret URI and project", secretProject: true, outputs: resource.PropertyMap{modernExport: secret(modernURI)}, wantHost: "modern.example.net", wantCluster: "alphamind-co--mong-8d9f"},
		{name: "prefer configured name", version: lo.ToPtr(2), outputs: resource.PropertyMap{modernExport: plain(modernURI), legacyExport: plain(legacyURI)}, wantHost: "modern.example.net", wantCluster: "alphamind-co--mong-8d9f"},
		{name: "prefer configured secret over legacy plain", version: lo.ToPtr(2), outputs: resource.PropertyMap{modernExport: secret(modernURI), legacyExport: plain(legacyURI)}, wantHost: "modern.example.net", wantCluster: "alphamind-co--mong-8d9f"},
		{name: "default prefers legacy when both exist", outputs: resource.PropertyMap{modernExport: plain(modernURI), legacyExport: plain(legacyURI)}, wantHost: "legacy.example.net", wantCluster: "alphamind---godb--prod"},
		{name: "invalid default legacy URI does not select modern", outputs: resource.PropertyMap{modernExport: plain(modernURI), legacyExport: resource.NewNumberProperty(42)}, wantErr: "not string"},
		{name: "custom parent", customName: "custom-cluster", outputs: resource.PropertyMap{"custom-cluster-mongo-uri-options": plain(modernURI)}, wantHost: "modern.example.net", wantCluster: "custom-cluster"},
		{name: "missing URI", outputs: resource.PropertyMap{}, wantErr: legacyExport},
		{name: "empty URI does not select another cluster", version: lo.ToPtr(2), outputs: resource.PropertyMap{modernExport: plain(""), legacyExport: plain(legacyURI)}, wantErr: "mongo uri is empty"},
		{name: "malformed URI does not select another cluster", version: lo.ToPtr(2), outputs: resource.PropertyMap{modernExport: resource.NewNumberProperty(42), legacyExport: plain(legacyURI)}, wantErr: "not string"},
		{name: "undecryptable URI does not select another cluster", version: lo.ToPtr(2), outputs: resource.PropertyMap{modernExport: resource.NewObjectProperty(resource.PropertyMap{"ciphertext": plain("sensitive-placeholder")}), legacyExport: plain(legacyURI)}, wantErr: "not string"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			outputs := tc.outputs.Copy()
			outputs["alphamind-co--mongodb--prod-id"] = plain("project-id")
			if tc.secretProject {
				outputs["alphamind-co--mongodb--prod-id"] = secret("project-id")
			}
			mocks := &parentOutputMocks{mongoCompatibilityMocks: mongoCompatibilityMocks{inputs: map[string]resource.PropertyMap{}}, outputs: outputs}
			var collector *compatibilityCollector
			err := sdk.RunErr(func(ctx *sdk.Context) error {
				provider, err := mongodbatlas.NewProvider(ctx, "atlas", nil)
				if err != nil {
					return err
				}
				log := logger.New()
				collector = &compatibilityCollector{ComputeContextCollector: pApi.NewComputeContextCollector(ctx.Context(), log, "app-ai-innmind--staging", "staging"), templates: map[string]map[string]string{}}
				input := createTestResourceInput("mongodb", &mongoApi.AtlasConfig{NamingStrategyVersion: tc.version, ClusterName: tc.customName, Adopt: tc.adopt})
				input.StackParams.Environment = "staging"
				input.StackParams.ParentEnv = "prod"
				_, err = ClusterComputeProcessor(ctx, api.Stack{Name: "app-ai-innmind--staging"}, input, collector, pApi.ProvisionParams{
					Log: log, Provider: provider, ParentStack: &pApi.ParentInfo{StackName: "alphamind-co", FullReference: "/alphamind-co/alphamind-co", ParentEnv: "prod", StackEnv: "staging", UsesResource: true},
				})
				return err
			}, sdk.WithMocks("project", "stack", mocks))
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error = %v, want containing %q", err, tc.wantErr)
				}
				if strings.Contains(err.Error(), "sensitive-placeholder") || strings.Contains(err.Error(), "mongodb+srv://") {
					t.Fatal("parent output value leaked in error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			wantURI := "mongodb+srv://app-ai-innmind--staging:test-password@" + tc.wantHost + "/app-ai-innmind--staging?retryWrites=true"
			foundURI := false
			for _, env := range collector.SecretEnvVariables() {
				if env.Name == "MONGO_URI" {
					foundURI = true
					if env.Value != wantURI {
						t.Errorf("MONGO_URI = %q, want %q", env.Value, wantURI)
					}
				}
			}
			if !foundURI || collector.templates["mongodb"]["uri"] != wantURI {
				t.Fatal("parent URI did not reach secret environment and resource template")
			}
			userKey := "mongodbatlas:index/databaseUser:DatabaseUser/" + tc.wantCluster + "-app-ai-innmind--staging-user"
			user, ok := mocks.inputs[userKey]
			if !ok || user["projectId"].StringValue() != "project-id" {
				t.Fatalf("database user not registered against the resolved parent cluster/project: %s", userKey)
			}
		})
	}
}
