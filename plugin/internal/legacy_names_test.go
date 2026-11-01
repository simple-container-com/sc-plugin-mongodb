// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package mongodb

import (
	"testing"

	"github.com/pulumi/pulumi/sdk/v3/go/common/resource"
	sdk "github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/samber/lo"
	"github.com/simple-container-com/api/pkg/api"
	pApi "github.com/simple-container-com/api/pkg/clouds/pulumi/api"
	mongoApi "github.com/simple-container-com/sc-plugin-mongodb/api"
)

// The names are literal expectations from SC 2025.12.6, not derived with the
// naming helpers under test. The config reproduces alphamind-co/server.yaml's
// mongodb resources without loading credentials or requiring Atlas/Pulumi.
func TestClusterPreserves2025126AlphamindNames(t *testing.T) {
	for _, tc := range []struct {
		env, size, project, cluster, user, accessList, backup, password string
	}{
		{
			env: "prod", size: "M20",
			project:    "alphamind-co--mongodb--prod",
			cluster:    "alphamind---godb--prod",
			user:       "alphamind---godb--prod-vitaly-user",
			accessList: "alphamind---godb--prod-ip-access-list",
			backup:     "alphamind---godb--prod-backups-schedule",
			password:   "alphamind-co--mongodb--prod-vitaly-password",
		},
		{
			env: "staging", size: "M0",
			project:    "alphamind-co--mongodb--staging",
			cluster:    "alphamind---b--staging",
			user:       "alphamind---b--staging-vitaly-user",
			accessList: "alphamind---b--staging-ip-access-list",
			password:   "alphamind-co--mongodb--staging-vitaly-password",
		},
	} {
		t.Run(tc.env, func(t *testing.T) {
			graph := renderMongoGraph(t, func(ctx *sdk.Context, params pApi.ProvisionParams, _ *compatibilityCollector) error {
				cfg := &mongoApi.AtlasConfig{
					Admins: []string{"vitaly"}, Developers: []string{},
					InstanceSize: tc.size, Region: "EU_CENTRAL_1", CloudProvider: "AWS",
				}
				if tc.backup != "" {
					cfg.Backup = &mongoApi.AtlasBackup{Every: "1h", Retention: "24h"}
				}
				_, err := Cluster(ctx, api.Stack{Name: "alphamind-co"}, api.ResourceInput{
					Descriptor:  &api.ResourceDescriptor{Name: "mongodb", Type: mongoApi.ResourceTypeMongodbAtlas, Config: api.Config{Config: cfg}},
					StackParams: &api.StackParams{StackName: "alphamind-co", Environment: tc.env},
				}, params)
				return err
			})
			// Type token + logical name determines the resource's Pulumi identity.
			// Check physical names/references as well: aliases alone cannot prevent
			// replacement when the Atlas cluster's Name changes.
			want := map[string]map[string]string{
				"mongodbatlas:index/project:Project/" + tc.project:                            {"name": tc.project},
				"mongodbatlas:index/cluster:Cluster/" + tc.cluster + "-cluster":               {"name": tc.cluster, "projectId": tc.project},
				"mongodbatlas:index/databaseUser:DatabaseUser/" + tc.user:                     {"username": "vitaly", "projectId": tc.project, "authDatabaseName": "admin"},
				"mongodbatlas:index/projectIpAccessList:ProjectIpAccessList/" + tc.accessList: {"cidrBlock": "0.0.0.0/0", "projectId": tc.project},
				"random:index/randomPassword:RandomPassword/" + tc.password:                   {},
			}
			if tc.backup != "" {
				want["mongodbatlas:index/cloudBackupSchedule:CloudBackupSchedule/"+tc.backup] = map[string]string{"clusterName": tc.cluster, "projectId": tc.project}
			}
			if len(graph.resources) != len(want)+1 { // plus the mock Atlas provider
				t.Errorf("registered %d resources, want %d", len(graph.resources), len(want)+1)
			}
			for key, props := range want {
				got, ok := graph.resources[key]
				if !ok {
					t.Errorf("missing legacy resource %s", key)
					continue
				}
				for name, value := range props {
					if prop := got[resource.PropertyKey(name)]; !prop.IsString() || prop.StringValue() != value {
						t.Errorf("%s: %s = %v, want %q", key, name, prop, value)
					}
				}
			}
		})
	}
}

func TestAlphamindNamingDefaultsAndParentEnvironment(t *testing.T) {
	for _, tc := range []struct {
		name   string
		config *mongoApi.AtlasConfig
		want   string
	}{
		{name: "no config", want: "alphamind---godb--prod"},
		{name: "omitted version", config: &mongoApi.AtlasConfig{}, want: "alphamind---godb--prod"},
		{name: "explicit legacy", config: &mongoApi.AtlasConfig{NamingStrategyVersion: lo.ToPtr(1)}, want: "alphamind---godb--prod"},
		{name: "unknown version", config: &mongoApi.AtlasConfig{NamingStrategyVersion: lo.ToPtr(3)}, want: "alphamind---godb--prod"},
		{name: "explicit hash opt in", config: &mongoApi.AtlasConfig{NamingStrategyVersion: lo.ToPtr(2)}, want: "alphamind-co--mong-8d9f"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := createTestResourceInput("mongodb", tc.config)
			input.StackParams.Environment = "staging"
			input.StackParams.ParentEnv = "prod"
			if got := toClusterName("alphamind-co", input); got != tc.want {
				t.Errorf("cluster name = %q, want %q", got, tc.want)
			}
			if got := toProjectName("alphamind-co", input); got != "alphamind-co--mongodb--prod" {
				t.Errorf("project name = %q, want alphamind-co--mongodb--prod", got)
			}
		})
	}
}
