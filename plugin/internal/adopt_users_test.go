// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package mongodb

import (
	"testing"

	"github.com/pulumi/pulumi/sdk/v3/go/common/resource"
	sdk "github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/simple-container-com/api/pkg/api"
	pApi "github.com/simple-container-com/api/pkg/clouds/pulumi/api"

	mongoApi "github.com/simple-container-com/sc-plugin-mongodb/api"
)

const (
	dbUserType   = "mongodbatlas:index/databaseUser:DatabaseUser"
	passwordType = "random:index/randomPassword:RandomPassword"
)

func renderAdoptedCluster(t *testing.T, cfg *mongoApi.AtlasConfig) mongoGraph {
	t.Helper()
	return renderMongoGraph(t, func(ctx *sdk.Context, params pApi.ProvisionParams, _ *compatibilityCollector) error {
		_, err := Cluster(ctx, api.Stack{Name: "infra"}, api.ResourceInput{
			Descriptor: &api.ResourceDescriptor{
				Type:   mongoApi.ResourceTypeMongodbAtlas,
				Name:   "mongodb",
				Config: api.Config{Config: cfg},
			},
			StackParams: &api.StackParams{StackName: "infra", Environment: "staging"},
		}, params)
		return err
	})
}

func adoptedConfig(admins, developers []string) *mongoApi.AtlasConfig {
	return &mongoApi.AtlasConfig{
		Adopt:       true,
		ProjectId:   "697a574e90d5387e4a889f5f",
		ClusterName: "infra--mongodb--staging",
		Admins:      admins,
		Developers:  developers,
	}
}

func rolesOf(t *testing.T, user resource.PropertyMap) map[string]string {
	t.Helper()
	roles := map[string]string{}
	for _, r := range user["roles"].ArrayValue() {
		obj := r.ObjectValue()
		roles[obj["roleName"].StringValue()] = obj["databaseName"].StringValue()
	}
	return roles
}

// Adoption must create the cluster-wide operator users under the SAME Pulumi
// names the provisioning path gave them, so a stack that lost them (forge
// `infra`, 2026-10-31) gets them back as plain creates rather than renames.
func TestAdoptCreatesOperatorUsers(t *testing.T) {
	g := renderAdoptedCluster(t, adoptedConfig([]string{"vitaly"}, []string{"reader"}))

	admin, ok := g.resources[dbUserType+"/infra--mongodb--staging-vitaly-user"]
	if !ok {
		t.Fatalf("admin DatabaseUser not registered; resources: %v", keys(g.resources))
	}
	if got := rolesOf(t, admin); len(got) != 2 || got["readWriteAnyDatabase"] != "admin" || got["read"] != "local" {
		t.Fatalf("admin roles = %v", got)
	}
	if admin["username"].StringValue() != "vitaly" || admin["authDatabaseName"].StringValue() != "admin" ||
		admin["projectId"].StringValue() != "697a574e90d5387e4a889f5f" {
		t.Fatalf("admin user inputs = %v", admin)
	}
	if _, ok := g.resources[passwordType+"/697a574e90d5387e4a889f5f-vitaly-password"]; !ok {
		t.Fatalf("admin RandomPassword not registered under its legacy name; resources: %v", keys(g.resources))
	}

	dev, ok := g.resources[dbUserType+"/infra--mongodb--staging-reader-user"]
	if !ok {
		t.Fatalf("developer DatabaseUser not registered; resources: %v", keys(g.resources))
	}
	if got := rolesOf(t, dev); len(got) != 2 || got["readAnyDatabase"] != "admin" || got["read"] != "local" {
		t.Fatalf("developer roles = %v", got)
	}
}

// With neither list set, adoption must register exactly what it always did:
// no users, no passwords.
func TestAdoptWithoutOperatorUsersIsUnchanged(t *testing.T) {
	g := renderAdoptedCluster(t, adoptedConfig(nil, nil))
	for k := range g.resources {
		if len(k) >= len(dbUserType) && k[:len(dbUserType)] == dbUserType ||
			len(k) >= len(passwordType) && k[:len(passwordType)] == passwordType {
			t.Fatalf("adopt without admins/developers registered %q", k)
		}
	}
}

func keys(m map[string]resource.PropertyMap) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
