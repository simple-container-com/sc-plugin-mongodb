// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

// sc-plugin-mongodb is the Simple Container plugin for MongoDB Atlas,
// extracted from `pkg/clouds/{mongodb,pulumi/mongodb}` per Phase 3+
// of forge/docs/design/2026-10-24/sc-plugin-architecture/
// 07-plugin-first-for-new-clouds.md §8.
//
// # Shipped scope (v1)
//
//   - Atlas Provider (`mongodbatlas:index/provider:Provider`).
//   - Cluster provisioning (project + cluster + optional backup
//     schedule + ProjectIpAccessList for AllowCidrs/AllowAllIps).
//   - AdoptCluster (import existing cluster with
//     pApi.AdoptionProtectionOptions).
//   - ClusterComputeProcessor — opens a StackReference against the
//     parent and emits MONGO_URI / cluster-id env vars via the
//     ComputeResultBuilder helpers from api/pkg/plugin/compute.go.
//   - DropDatabaseHook — reads `cloudExtras.mongoDbDestroyDatabase` off
//     the per-env client descriptor PreDestroyRequest carries and drops
//     the per-service DBs enumerated from stack outputs.
//
// # Deferred (follow-up)
//
//   - PrivateLink + AWS VPC endpoint + MongoDB private link endpoint
//     service. The in-tree driver pulled pulumi-aws + pulumi-ec2 into
//     a non-AWS plugin — moving that reintroduces 200 MB. The Phase 1e
//     PeerInvoke D8 pathway (SCCore callback + BrokerAware) is the
//     clean way to broker AWS resources from sc-plugin-aws without a
//     Go-module dependency. Cluster() refuses `networkConfig.privateLinkEndpoint`
//     explicitly with a message pointing operators at that gap. Lands
//     when sc-plugin-aws exists.
package main

import (
	"context"
	"fmt"
	"os"
	"sync"

	"gopkg.in/yaml.v3"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/pkg/errors"
	sdk "github.com/pulumi/pulumi/sdk/v3/go/pulumi"

	"github.com/simple-container-com/api/pkg/api"
	"github.com/simple-container-com/api/pkg/api/logger"
	pApi "github.com/simple-container-com/api/pkg/clouds/pulumi/api"
	scPlugin "github.com/simple-container-com/api/pkg/plugin"
	scpluginv1 "github.com/simple-container-com/api/pkg/plugin/proto/v1"

	mongoApi "github.com/simple-container-com/sc-plugin-mongodb/api"
	mongoInternal "github.com/simple-container-com/sc-plugin-mongodb/plugin/internal"
)

type mongoPlugin struct {
	scpluginv1.UnimplementedSCCloudPluginServer

	mu       sync.Mutex
	progress *scPlugin.Publisher
}

func (p *mongoPlugin) Schema(_ context.Context, _ *scpluginv1.SchemaRequest) (*scpluginv1.SchemaResponse, error) {
	return &scpluginv1.SchemaResponse{
		PluginVersion:         "mongodb-0.0.1",
		ProviderTypes:         []string{mongoApi.ProviderType},
		ResourceTypes:         []string{mongoApi.ResourceTypeMongodbAtlas},
		ComputeProcessorTypes: []string{mongoApi.ResourceTypeMongodbAtlas},
		PulumiPlugins: []*scpluginv1.PulumiPluginRequirement{
			// pulumi-mongodbatlas is the big one (~19 MB in the module
			// cache). Pin against api/go.mod — bumping either side
			// without the other replaces the provider in state, which on
			// mongodb means a cluster replace with data loss.
			{Package: "mongodbatlas", Version: "3.38.0"},
			// pulumi-random — generates per-stack DB passwords.
			{Package: "random", Version: "4.19.0"},
		},
	}, nil
}

func (p *mongoPlugin) pulumiContextFromHeader(ctx context.Context, h *scpluginv1.RequestHeader) (*sdk.Context, func(), error) {
	if h.GetMonitorAddress() == "" {
		return nil, nil, errors.New("no monitor address in request header")
	}
	pctx, err := sdk.NewContext(ctx, sdk.RunInfo{
		MonitorAddr: h.GetMonitorAddress(),
		Project:     h.GetProjectName(),
		Stack:       h.GetStackName(),
	})
	if err != nil {
		return nil, nil, errors.Wrap(err, "open pulumi context from monitor address")
	}
	return pctx, func() { pctx.Close() }, nil
}

func (p *mongoPlugin) Provider(ctx context.Context, req *scpluginv1.ProviderRequest) (*scpluginv1.ProviderResponse, error) {
	pctx, done, err := p.pulumiContextFromHeader(ctx, req.GetHeader())
	if err != nil {
		return nil, err
	}
	defer done()
	p.progress.Info("provider", fmt.Sprintf("initialising mongodb-atlas provider for %q", req.GetProviderResourceName()))

	cfg := &mongoApi.AtlasConfig{}
	if err := decodeInputsAs(req.GetInputs(), cfg); err != nil {
		return nil, errors.Wrap(err, "decode AtlasConfig from Provider inputs")
	}

	input := providerInput(req)
	out, err := mongoInternal.Provider(pctx, api.Stack{Name: req.GetHeader().GetStackName()}, input, pApi.ProvisionParams{Log: logger.New()})
	if err != nil {
		p.progress.Error("provider", err.Error())
		return nil, err
	}
	urn := waitForRef(out.Ref)
	p.progress.Info("provider", fmt.Sprintf("provider ready (urn=%s)", urn))
	return &scpluginv1.ProviderResponse{ProviderUrn: urn}, nil
}

func (p *mongoPlugin) Provision(ctx context.Context, req *scpluginv1.ProvisionRequest) (*scpluginv1.ProvisionResponse, error) {
	pctx, done, err := p.pulumiContextFromHeader(ctx, req.GetHeader())
	if err != nil {
		return nil, err
	}
	defer done()
	p.progress.Info("provision", fmt.Sprintf("provisioning mongodb cluster %q", req.GetResourceName()))

	cfg := &mongoApi.AtlasConfig{}
	if err := decodeInputsAs(req.GetInputs(), cfg); err != nil {
		return nil, errors.Wrap(err, "decode AtlasConfig from Provision inputs")
	}

	input := resourceInputFromProvision(req, cfg)
	// mongodb.Cluster branches internally on cfg.Adopt and delegates to
	// AdoptCluster when the operator asked to import an existing cluster.
	out, err := mongoInternal.Cluster(pctx, api.Stack{Name: req.GetHeader().GetStackName()}, input, pApi.ProvisionParams{Log: logger.New()})
	if err != nil {
		p.progress.Error("provision", err.Error())
		return nil, err
	}
	urn := waitForRef(out.Ref)
	p.progress.Info("provision", fmt.Sprintf("cluster %q registered (urn=%s)", req.GetResourceName(), urn))
	return &scpluginv1.ProvisionResponse{Urn: urn}, nil
}

func (p *mongoPlugin) ComputeProcess(ctx context.Context, req *scpluginv1.ComputeProcessRequest) (*scpluginv1.ComputeProcessResponse, error) {
	pctx, done, err := p.pulumiContextFromHeader(ctx, req.GetHeader())
	if err != nil {
		return nil, err
	}
	defer done()
	p.progress.Info("compute-process", fmt.Sprintf("resource=%q type=%q", req.GetResourceName(), req.GetResourceType()))

	// ClusterComputeProcessor consumes params.ParentStack. v2 proto
	// packs it as ParentInfoJson — unmarshal back into the Go struct the
	// in-tree code expects.
	parent := &pApi.ParentInfo{}
	if raw := req.GetParentInfoJson(); len(raw) > 0 {
		if err := yaml.Unmarshal(raw, parent); err != nil {
			return nil, errors.Wrap(err, "decode ParentInfo from compute-process request")
		}
	}

	cfg := &mongoApi.AtlasConfig{}
	if err := decodeInputsAs(req.GetInputs(), cfg); err != nil {
		return nil, errors.Wrap(err, "decode AtlasConfig from ComputeProcess inputs")
	}

	input := resourceInputFromComputeProcess(req, cfg)

	// ClusterComputeProcessor returns an api.ResourceOutput and mutates
	// the collector. Our sidecar wrapper is the collector — the SC-core
	// sidecar then applies our env_vars + tpl_extensions back into the
	// live core-side collector.
	collector := newCapturingCollector()
	_, err = mongoInternal.ClusterComputeProcessor(pctx, api.Stack{Name: req.GetHeader().GetStackName()}, input, collector, pApi.ProvisionParams{
		Log:         logger.New(),
		ParentStack: parent,
	})
	if err != nil {
		p.progress.Error("compute-process", err.Error())
		return nil, err
	}

	resp := collector.ToResponse()
	p.progress.Info("compute-process", fmt.Sprintf("emitted %d env var(s) + %d tpl extension(s)", len(resp.GetEnvVars()), len(resp.GetTplExtensions())))
	return resp, nil
}

func (p *mongoPlugin) PreDestroy(ctx context.Context, req *scpluginv1.PreDestroyRequest) (*scpluginv1.PreDestroyResponse, error) {
	p.progress.Info("pre-destroy", fmt.Sprintf("stack=%q env=%q", req.GetHeader().GetStackName(), req.GetHeader().GetEnvironment()))
	if err := mongoInternal.DropDatabaseHookFromPlugin(ctx, p.progress, req); err != nil {
		p.progress.Error("pre-destroy", err.Error())
	}
	return &scpluginv1.PreDestroyResponse{}, nil
}

func (p *mongoPlugin) LogStream(_ *scpluginv1.LogStreamRequest, stream scpluginv1.SCCloudPlugin_LogStreamServer) error {
	ctx := stream.Context()
	for {
		batch, ok := p.progress.Drain()
		if !ok {
			return nil
		}
		for _, ev := range batch {
			if err := stream.Send(ev); err != nil {
				return err
			}
		}
		if ctx.Err() != nil {
			return nil
		}
	}
}

// Helpers below — all shape-matching glue.

// decodeInputsAs lifts the plugin bus's input map into a typed struct
// via YAML round-trip. Same pattern sc-plugin-cloudru + sc-plugin-yandex
// use. The map covers scalar inputs; nested shapes (AtlasConfig.backup,
// .networkConfig) survive IF the SC-core sidecar serialised them as
// nested maps. Known gap: today's `serializeConfigToInputs` has only
// scalar cases; a deeply nested AtlasConfig may lose fields. Follow-up:
// extend serializeConfigToInputs or ship AtlasConfig as a JSON blob in
// bytes_value.
func decodeInputsAs[T any](inputs map[string]*scpluginv1.InputValue, out *T) error {
	plain := make(map[string]any, len(inputs))
	for k, v := range inputs {
		switch x := v.GetValue().(type) {
		case *scpluginv1.InputValue_StringValue:
			plain[k] = x.StringValue
		case *scpluginv1.InputValue_BoolValue:
			plain[k] = x.BoolValue
		case *scpluginv1.InputValue_DoubleValue:
			plain[k] = x.DoubleValue
		case *scpluginv1.InputValue_BytesValue:
			plain[k] = string(x.BytesValue)
		}
	}
	b, err := yaml.Marshal(plain)
	if err != nil {
		return err
	}
	return yaml.Unmarshal(b, out)
}

// providerInput, resourceInputFromProvision, resourceInputFromComputeProcess
// reconstruct the api.ResourceInput shape the lifted driver code expects
// from the plugin-bus Request message.
func providerInput(req *scpluginv1.ProviderRequest) api.ResourceInput {
	return api.ResourceInput{
		Descriptor: &api.ResourceDescriptor{
			Type: mongoApi.ProviderType,
			Name: req.GetProviderResourceName(),
		},
		StackParams: &api.StackParams{
			StackName:   req.GetHeader().GetStackName(),
			Environment: req.GetHeader().GetEnvironment(),
		},
	}
}

func resourceInputFromProvision(req *scpluginv1.ProvisionRequest, cfg *mongoApi.AtlasConfig) api.ResourceInput {
	return api.ResourceInput{
		Descriptor: &api.ResourceDescriptor{
			Type:   req.GetResourceType(),
			Name:   req.GetResourceName(),
			Config: api.Config{Config: cfg},
		},
		StackParams: &api.StackParams{
			StackName:   req.GetHeader().GetStackName(),
			Environment: req.GetHeader().GetEnvironment(),
		},
	}
}

func resourceInputFromComputeProcess(req *scpluginv1.ComputeProcessRequest, cfg *mongoApi.AtlasConfig) api.ResourceInput {
	return api.ResourceInput{
		Descriptor: &api.ResourceDescriptor{
			Type:   req.GetResourceType(),
			Name:   req.GetResourceName(),
			Config: api.Config{Config: cfg},
		},
		StackParams: &api.StackParams{
			StackName:   req.GetHeader().GetStackName(),
			Environment: req.GetHeader().GetEnvironment(),
		},
	}
}

// waitForRef blocks until a Pulumi Output in a resource ref resolves —
// same reason + same shape as sc-plugin-cloudflare/yandex/cloudru.
func waitForRef(ref any) string {
	if ref == nil {
		return ""
	}
	switch r := ref.(type) {
	case string:
		return r
	case sdk.ProviderResource:
		ch := make(chan string, 1)
		r.URN().ApplyT(func(u sdk.URN) sdk.URN {
			select {
			case ch <- string(u):
			default:
			}
			return u
		})
		return <-ch
	case sdk.IDOutput:
		ch := make(chan string, 1)
		r.ApplyT(func(id sdk.ID) sdk.ID {
			select {
			case ch <- string(id):
			default:
			}
			return id
		})
		return <-ch
	case sdk.URNOutput:
		ch := make(chan string, 1)
		r.ApplyT(func(u sdk.URN) sdk.URN {
			select {
			case ch <- string(u):
			default:
			}
			return u
		})
		return <-ch
	default:
		return fmt.Sprintf("%v", r)
	}
}

func main() {
	impl := &mongoPlugin{progress: scPlugin.NewPublisher(0)}
	scPlugin.Serve(scPlugin.ServerConfig{
		Impl: impl,
		Logger: hclog.New(&hclog.LoggerOptions{
			Name:   "sc-plugin-mongodb",
			Level:  hclog.Warn,
			Output: os.Stderr,
		}),
	})
}
