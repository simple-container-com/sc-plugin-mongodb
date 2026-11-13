// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package mongodb

import (
	"encoding/json"
	"testing"

	"github.com/pulumi/pulumi-mongodbatlas/sdk/v3/go/mongodbatlas"
	"github.com/pulumi/pulumi/sdk/v3/go/common/resource"
	sdk "github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi/internals"
	"github.com/simple-container-com/api/pkg/api"
	"github.com/simple-container-com/api/pkg/api/logger"
	pApi "github.com/simple-container-com/api/pkg/clouds/pulumi/api"

	mongoApi "github.com/simple-container-com/sc-plugin-mongodb/api"
)

// awaitedExport is one resolved entry of the RunFunc's export map.
type awaitedExport struct {
	value  any
	secret bool
	err    error
}

// renderExports runs a provisioning function under the Pulumi mocks and
// resolves ctx.GetCurrentExportMap().
//
// The existing renderMongoGraph harness captures registered RESOURCES only and
// never looks at the export map, which is why nothing has ever pinned what
// this plugin publishes — the gap that let the operator credentials be dropped
// unnoticed on both paths.
func renderExports(t *testing.T, cfg *mongoApi.AtlasConfig) map[string]awaitedExport {
	t.Helper()

	out := map[string]awaitedExport{}
	mocks := &mongoCompatibilityMocks{inputs: map[string]resource.PropertyMap{}}
	err := sdk.RunErr(func(ctx *sdk.Context) error {
		provider, err := mongodbatlas.NewProvider(ctx, "atlas", nil)
		if err != nil {
			return err
		}
		params := pApi.ProvisionParams{
			Provider:    provider,
			Log:         logger.New(),
			ParentStack: &pApi.ParentInfo{StackName: "parent", StackEnv: "staging"},
		}
		if _, err := Cluster(ctx, api.Stack{Name: "infra"}, api.ResourceInput{
			Descriptor: &api.ResourceDescriptor{
				Type:   mongoApi.ResourceTypeMongodbAtlas,
				Name:   "mongodb",
				Config: api.Config{Config: cfg},
			},
			StackParams: &api.StackParams{StackName: "infra", Environment: "staging"},
		}, params); err != nil {
			return err
		}
		// Resolve inside the RunFunc: the export map is only populated while
		// it is running, and awaiting an Output needs the live context.
		for name, input := range ctx.GetCurrentExportMap() {
			res, err := internals.UnsafeAwaitOutput(ctx.Context(), sdk.ToOutput(input))
			out[name] = awaitedExport{value: res.Value, secret: res.Secret, err: err}
		}
		return nil
	}, sdk.WithMocks("project", "stack", mocks))
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	return out
}

// Core's harvest (api/pkg/plugin/runtime/harvest.go:anyToInputValue) carries
// only scalars; a map or slice converts to nil and HarvestExports drops the
// key with no warning. The `<projectName>-users` export is a map, so every
// password this plugin generates was invisible to `sc stack outputs` — on the
// create path since users existed, and on the adopt path since #13 started
// creating them. These per-user scalar exports are what reach the stack.
//
// Both paths are exercised because they nest the users output differently: the
// create path wraps createDatabaseUsers in another sdk.All().ApplyT(), adopt
// calls it directly. If ApplyT's unwrapping did not flatten both to a resolved
// map, the extraction would fail — which is exactly what this pins.
func TestDbUserCredentialsAreExportedAsSecretScalars(t *testing.T) {
	for _, tc := range []struct {
		name  string
		adopt bool
	}{
		{"create path", false},
		{"adopt path", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &mongoApi.AtlasConfig{
				Adopt:       tc.adopt,
				ProjectId:   "697a574e90d5387e4a889f5f",
				ClusterName: "infra--mongodb--staging",
				OrgId:       "5b89110a4e6581562623c59c",
				Region:      "EU_CENTRAL_1",
				Admins:      []string{"vitaly", "smecsia"},
				Developers:  []string{"backup"},
			}

			exports := renderExports(t, cfg)

			for _, user := range []string{"vitaly", "smecsia", "backup"} {
				key := "infra--mongodb--staging-" + user + "-db-user"
				got, ok := exports[key]
				if !ok {
					t.Fatalf("no export %q; the operator credential is unreachable from `sc stack outputs`. got keys: %v",
						key, keysOf(exports))
				}
				if got.err != nil {
					t.Fatalf("export %q failed to resolve: %v", key, got.err)
				}
				// A credential export must never be secret:false — the
				// checkpoint would hold the password in plaintext.
				if !got.secret {
					t.Errorf("export %q is not marked secret", key)
				}
				// Must be a STRING, or core's harvest drops it exactly like
				// the map export this fix works around.
				s, isString := got.value.(string)
				if !isString {
					t.Fatalf("export %q is %T, not a string: core's harvest will drop it", key, got.value)
				}
				var creds mongoDbUser
				if err := json.Unmarshal([]byte(s), &creds); err != nil {
					t.Fatalf("export %q is not DbUserOutput JSON: %v (%q)", key, err, s)
				}
				if creds.UserName != user {
					t.Errorf("export %q carries userName %q, want %q", key, creds.UserName, user)
				}
				if creds.Password == "" {
					t.Errorf("export %q carries an empty password", key)
				}
			}

			// The map export stays, so anything reading it in-process (the
			// ClusterOutput.DbUsers ref) is unaffected by this addition.
			if _, ok := exports["infra--mongodb--staging-users"]; !ok {
				t.Errorf("the -users map export disappeared; in-process consumers read it")
			}
		})
	}
}

// With neither list configured there is nothing to publish, and no stack that
// configures neither should see a new key appear.
func TestNoDbUserExportsWithoutOperators(t *testing.T) {
	exports := renderExports(t, &mongoApi.AtlasConfig{
		Adopt:       true,
		ProjectId:   "697a574e90d5387e4a889f5f",
		ClusterName: "infra--mongodb--staging",
	})

	for name := range exports {
		if len(name) > len("-db-user") && name[len(name)-len("-db-user"):] == "-db-user" {
			t.Errorf("unexpected credential export %q for a cluster with no admins/developers", name)
		}
	}
}

// Slice 1d.3 of sc-components-platform: `<cluster>-database` export when
// the operator sets `config.database:`. sc-component-backup's matcher
// (MatchConsumerToDatabase in sc-component-backup plugin/internal/
// scoped_user.go) resolves the per-consumer target DB via the
// `-database` suffix, so the key must land as a plain string scalar for
// core's harvest to carry it through to the sibling's dependency outputs.
func TestDatabaseExport_PublishedWhenSet(t *testing.T) {
	exports := renderExports(t, &mongoApi.AtlasConfig{
		Adopt:       true,
		ProjectId:   "697a574e90d5387e4a889f5f",
		ClusterName: "infra--mongodb--staging",
		Database:    "forge_app",
	})

	got, ok := exports["infra--mongodb--staging-database"]
	if !ok {
		t.Fatalf("no export %q; keys: %v", "infra--mongodb--staging-database", keysOf(exports))
	}
	if got.err != nil {
		t.Fatalf("export failed to resolve: %v", got.err)
	}
	// DB name is NOT a secret — a bucket name analogue; the credentials
	// embedded in mongo-uri carry the secret-marked ones.
	if got.secret {
		t.Errorf("`-database` export is marked secret; it holds a DB name, not a credential")
	}
	s, isString := got.value.(string)
	if !isString {
		t.Fatalf("`-database` export is %T, not a string: core's harvest would drop it", got.value)
	}
	if s != "forge_app" {
		t.Errorf("`-database` export = %q, want %q", s, "forge_app")
	}
}

// Legacy stacks that pre-date the field see no `-database` export —
// sibling matchers (sc-component-backup) silently return "no match" and
// the scoped-user mint path stays dormant. Guards against a bug where
// an empty-string DB name slips through and takes a sibling down with
// a confusing `Database required` later in Atlas REST.
func TestDatabaseExport_AbsentWhenUnset(t *testing.T) {
	exports := renderExports(t, &mongoApi.AtlasConfig{
		Adopt:       true,
		ProjectId:   "697a574e90d5387e4a889f5f",
		ClusterName: "infra--mongodb--staging",
		// Database intentionally unset.
	})

	if _, ok := exports["infra--mongodb--staging-database"]; ok {
		t.Errorf("`-database` export leaked on an unset config — must be opt-in")
	}
}

type mongoDbUser struct {
	UserName string `json:"userName"`
	Password string `json:"password"`
	DbUri    string `json:"dbUri"`
}

func keysOf[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
