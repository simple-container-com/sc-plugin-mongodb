// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package mongodb

import (
	"reflect"
	"sync"
	"testing"

	"github.com/pulumi/pulumi-mongodbatlas/sdk/v3/go/mongodbatlas"
	"github.com/pulumi/pulumi/sdk/v3/go/common/resource"
	sdk "github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/simple-container-com/api/pkg/api"
	"github.com/simple-container-com/api/pkg/api/logger"
	pApi "github.com/simple-container-com/api/pkg/clouds/pulumi/api"
	mongoApi "github.com/simple-container-com/sc-plugin-mongodb/api"
)

type mongoCompatibilityMocks struct {
	mu     sync.Mutex
	inputs map[string]resource.PropertyMap
}

func (m *mongoCompatibilityMocks) Call(a sdk.MockCallArgs) (resource.PropertyMap, error) {
	return a.Args, nil
}
func (m *mongoCompatibilityMocks) NewResource(a sdk.MockResourceArgs) (string, resource.PropertyMap, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.inputs[a.TypeToken+"/"+a.Name] = a.Inputs.Copy()
	out := a.Inputs.Copy()
	if a.TypeToken == "random:index/randomPassword:RandomPassword" {
		out["result"] = resource.NewStringProperty("test-password")
	}
	if a.TypeToken == "mongodbatlas:index/cluster:Cluster" {
		out["mongoUri"] = resource.NewStringProperty("mongodb+srv://example.net")
		out["mongoUriWithOptions"] = resource.NewStringProperty("mongodb+srv://example.net/?retryWrites=true")
		out["connectionStrings"] = resource.NewArrayProperty(nil)
	}
	return a.Name, out, nil
}

type compatibilityCollector struct {
	pApi.ComputeContextCollector
	templates map[string]map[string]string
}

func (c *compatibilityCollector) AddResourceTplExtension(name string, v map[string]string) {
	c.templates[name] = v
}
func (c *compatibilityCollector) AddDependencyTplExtension(dep, name string, v map[string]string) {
	c.templates[dep+"/"+name] = v
}

type mongoGraph struct {
	resources    map[string]resource.PropertyMap
	templates    map[string]map[string]string
	env, secrets []pApi.ComputeEnvVariable
}

func renderMongoGraph(t *testing.T, run func(*sdk.Context, pApi.ProvisionParams, *compatibilityCollector) error) mongoGraph {
	t.Helper()
	mocks := &mongoCompatibilityMocks{inputs: map[string]resource.PropertyMap{}}
	var collector *compatibilityCollector
	err := sdk.RunErr(func(ctx *sdk.Context) error {
		provider, err := mongodbatlas.NewProvider(ctx, "atlas", nil)
		if err != nil {
			return err
		}
		log := logger.New()
		collector = &compatibilityCollector{ComputeContextCollector: pApi.NewComputeContextCollector(ctx.Context(), log, "app", "prod"), templates: map[string]map[string]string{}}
		return run(ctx, pApi.ProvisionParams{Provider: provider, Log: log, ParentStack: &pApi.ParentInfo{StackName: "parent", StackEnv: "prod"}}, collector)
	}, sdk.WithMocks("project", "stack", mocks))
	if err != nil {
		t.Fatal(err)
	}
	return mongoGraph{resources: mocks.inputs, templates: collector.templates, env: collector.EnvVariables(), secrets: collector.SecretEnvVariables()}
}
func TestDefaultMongoUsersMatchOriginMain(t *testing.T) {
	for _, tc := range []struct {
		name         string
		old, current func(*sdk.Context, appendParams) error
	}{
		{"uses", originMainUsesResourceContext, appendUsesResourceContext},
		{"dependency", originMainDependsOnResourceContext, appendDependsOnResourceContext},
	} {
		t.Run(tc.name, func(t *testing.T) {
			render := func(f func(*sdk.Context, appendParams) error) mongoGraph {
				return renderMongoGraph(t, func(ctx *sdk.Context, params pApi.ProvisionParams, c *compatibilityCollector) error {
					return f(ctx, appendParams{stack: api.Stack{Name: "app"}, collector: c, input: api.ResourceInput{Descriptor: &api.ResourceDescriptor{Type: mongoApi.ResourceTypeMongodbAtlas, Name: "db"}, StackParams: &api.StackParams{Environment: "prod"}}, dependency: api.StackConfigDependencyResource{Name: "database", Owner: "org/parent", Resource: "db"}, clusterName: "cluster", projectId: "project", mongoUri: "mongodb+srv://example.net", suffix: "-prod", provisionParams: params, readOplog: true})
				})
			}
			want, got := render(tc.old), render(tc.current)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("default %s graph/templates/credentials differ from origin/main\nwant %+v\ngot %+v", tc.name, want, got)
			}
			if len(got.resources) != 3 || len(got.templates) != 1 || len(got.secrets) == 0 {
				t.Fatalf("comparison did not exercise database credentials: %+v", got)
			}
		})
	}
}
func TestDefaultMongoNetworkMatchesOriginMain(t *testing.T) {
	render := func(builder func(*sdk.Context, api.Stack, api.ResourceInput, pApi.ProvisionParams) (*api.ResourceOutput, error)) mongoGraph {
		return renderMongoGraph(t, func(ctx *sdk.Context, params pApi.ProvisionParams, _ *compatibilityCollector) error {
			_, err := builder(ctx, api.Stack{Name: "parent"}, api.ResourceInput{Descriptor: &api.ResourceDescriptor{Type: mongoApi.ResourceTypeMongodbAtlas, Config: api.Config{Config: &mongoApi.AtlasConfig{InstanceSize: "M10", Region: "EU_WEST_1", CloudProvider: "AWS"}}}, StackParams: &api.StackParams{Environment: "prod"}}, params)
			return err
		})
	}
	want, got := render(originMainCluster), render(Cluster)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("default cluster/access-list graph differs from origin/main\nwant %+v\ngot %+v", want, got)
	}
	found := false
	for key, props := range got.resources {
		if props["cidrBlock"].IsString() && props["cidrBlock"].StringValue() == "0.0.0.0/0" {
			found = true
			t.Log("preserved access list", key)
		}
	}
	if !found {
		t.Fatal("default public access-list entry missing")
	}
}
