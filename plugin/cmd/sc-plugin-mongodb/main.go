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
//
// # Lifecycle
//
// Every RPC handler constructs its pulumi context via
// api/pkg/plugin/runtime.NewContext and defers runtime.Settle. Settle
// drains every RegisterResource goroutine before the RPC response is
// sent, folding a late-arriving error into the handler's return — the
// pattern yandex's PR #8 arrived at, now shared library (see
// forge/docs/design/2026-10-24/sc-plugin-architecture/04-no-destroy-invariant.md
// Addendums U + V for the cascade this guards against).
package main

import (
	"context"
	"encoding/json"
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
	"github.com/simple-container-com/api/pkg/plugin/runtime"

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

func (p *mongoPlugin) Provider(ctx context.Context, req *scpluginv1.ProviderRequest) (resp *scpluginv1.ProviderResponse, err error) {
	rctx, err := runtime.NewContext(ctx, req.GetHeader(), runtime.WithLocalEngineFallback(p.progress))
	if err != nil {
		return nil, err
	}
	defer runtime.Settle(rctx, &resp, &err)
	p.progress.Info("provider", fmt.Sprintf("initialising mongodb-atlas provider for %q", req.GetProviderResourceName()))

	cfg := &mongoApi.AtlasConfig{}
	if err := decodeInputsAs(req.GetInputs(), cfg); err != nil {
		return nil, errors.Wrap(err, "decode AtlasConfig from Provider inputs")
	}

	input := providerInput(req)
	out, err := mongoInternal.Provider(rctx.Pulumi, api.Stack{Name: req.GetHeader().GetStackName()}, input, pApi.ProvisionParams{Log: logger.New()})
	if err != nil {
		p.progress.Error("provider", err.Error())
		return nil, err
	}
	urn := waitForRef(out.Ref)
	p.progress.Info("provider", fmt.Sprintf("provider ready (urn=%s)", urn))
	return &scpluginv1.ProviderResponse{ProviderUrn: urn}, nil
}

func (p *mongoPlugin) Provision(ctx context.Context, req *scpluginv1.ProvisionRequest) (resp *scpluginv1.ProvisionResponse, err error) {
	rctx, err := runtime.NewContext(ctx, req.GetHeader(), runtime.WithLocalEngineFallback(p.progress))
	if err != nil {
		return nil, err
	}
	defer runtime.Settle(rctx, &resp, &err)
	p.progress.Info("provision", fmt.Sprintf("provisioning mongodb cluster %q", req.GetResourceName()))

	cfg := &mongoApi.AtlasConfig{}
	if err := decodeInputsAs(req.GetInputs(), cfg); err != nil {
		return nil, errors.Wrap(err, "decode AtlasConfig from Provision inputs")
	}

	// Build the Atlas provider inline. Without this, `params.Provider` is
	// nil at `cluster.go:68` (`sdk.Provider(params.Provider)`), which
	// resolves to `Providers(nil).applyResourceOption(...)` and calls
	// `nil.getPackage()` on the first Pulumi resource option application
	// — nil-pointer dereference, plugin process exits status 2, SC core
	// sees `rpc error: EOF`. Mirrors sc-plugin-aws's Provision which builds
	// its provider the same way.
	//
	// The resource name must match what the in-tree code produced so URNs
	// stay stable across the plugin split — see computeProvider() below for
	// the formula source.
	stack := api.Stack{Name: req.GetHeader().GetStackName()}
	providerResName := fmt.Sprintf("%s--%s--%s--%s--provider",
		stack.Name, req.GetResourceType(), req.GetResourceName(), req.GetHeader().GetEnvironment())
	provOut, err := mongoInternal.Provider(rctx.Pulumi, stack, api.ResourceInput{
		Descriptor: &api.ResourceDescriptor{
			Type:   mongoApi.ProviderType,
			Name:   providerResName,
			Config: api.Config{Config: cfg},
		},
		StackParams: &api.StackParams{
			StackName:   stack.Name,
			Environment: req.GetHeader().GetEnvironment(),
		},
	}, pApi.ProvisionParams{Log: logger.New()})
	if err != nil {
		p.progress.Error("provision", err.Error())
		return nil, errors.Wrap(err, "build mongodb atlas provider inside Provision")
	}
	provider, ok := provOut.Ref.(sdk.ProviderResource)
	if !ok {
		return nil, errors.Errorf("mongodb Provider returned %T, not sdk.ProviderResource", provOut.Ref)
	}

	input := resourceInputFromProvision(req, cfg)
	// mongodb.Cluster branches internally on cfg.Adopt and delegates to
	// AdoptCluster when the operator asked to import an existing cluster.
	out, err := mongoInternal.Cluster(rctx.Pulumi, stack, input, pApi.ProvisionParams{Log: logger.New(), Provider: provider})
	if err != nil {
		p.progress.Error("provision", err.Error())
		return nil, err
	}
	urn := waitForRef(out.Ref)
	// Harvest the pctx's exports and hand them back on the wire.
	// SC core re-exports them against its OWN pctx so they land in the
	// parent stack root's RegisterResourceOutputs call. Settle has
	// already drained every RegisterResource goroutine by the time this
	// deferred teardown fires, but harvest reads the current export map
	// which is populated inside mongoInternal.Cluster; so we must run
	// harvest BEFORE Settle fires (i.e. before this handler returns).
	// Live-caught 2026-10-31: parent `/forge/infra` had 6 output keys,
	// all templates, zero plugin resource keys.
	exports, exportsErr := runtime.HarvestExports(ctx, rctx.Pulumi)
	if exportsErr != nil {
		p.progress.Warn("provision", "harvest exports: "+exportsErr.Error())
	}
	p.progress.Info("provision", fmt.Sprintf("cluster %q registered (urn=%s, exports=%d)", req.GetResourceName(), urn, len(exports)))
	return &scpluginv1.ProvisionResponse{Urn: urn, Outputs: exports}, nil
}

func (p *mongoPlugin) ComputeProcess(ctx context.Context, req *scpluginv1.ComputeProcessRequest) (resp *scpluginv1.ComputeProcessResponse, err error) {
	rctx, err := runtime.NewContext(ctx, req.GetHeader(), runtime.WithLocalEngineFallback(p.progress))
	if err != nil {
		return nil, err
	}
	defer runtime.Settle(rctx, &resp, &err)
	p.progress.Info("compute-process", fmt.Sprintf("resource=%q type=%q", req.GetResourceName(), req.GetResourceType()))

	// ClusterComputeProcessor consumes params.ParentStack. v2 proto
	// packs it as ParentInfoJson — unmarshal back into the Go struct the
	// in-tree code expects.
	parent, err := runtime.ParentInfoFromRequest(req)
	if err != nil {
		return nil, err
	}

	cfg := &mongoApi.AtlasConfig{}
	if err := decodeInputsAs(req.GetInputs(), cfg); err != nil {
		return nil, errors.Wrap(err, "decode AtlasConfig from ComputeProcess inputs")
	}

	input := resourceInputFromComputeProcess(req, cfg, parent)
	stack := api.Stack{Name: req.GetHeader().GetStackName()}

	// The database user is created through an explicit Atlas provider built from
	// the resource's own keys, named exactly as SC core named it when this compute
	// processor was in-tree (initProvider: "<stack>--<type>--<res>[--<dep>]--<env>--provider").
	// Without it the DatabaseUser lands on the DEFAULT mongodbatlas provider, which
	// has no credentials: the deploy fails at create with an Atlas 401, and a stack
	// migrating from the in-tree processor would see its provider replaced.
	provider, err := p.computeProvider(rctx.Pulumi, stack, req, cfg, parent)
	if err != nil {
		p.progress.Error("compute-process", err.Error())
		return nil, err
	}

	// ClusterComputeProcessor returns an api.ResourceOutput and mutates
	// the collector. Our sidecar wrapper is the collector — the SC-core
	// sidecar then applies our env_vars + tpl_extensions back into the
	// live core-side collector.
	collector := newCapturingCollector()
	_, err = mongoInternal.ClusterComputeProcessor(rctx.Pulumi, stack, input, collector, pApi.ProvisionParams{
		Log:         logger.New(),
		ParentStack: parent,
		Provider:    provider,
	})
	if err != nil {
		p.progress.Error("compute-process", err.Error())
		return nil, err
	}
	// Every MONGO_* variable is added inside an ApplyT on the database user
	// (AddOutput). Answer before those have resolved and the response carries no
	// env at all — and the deferred Settle teardown then cuts the user's in-flight
	// RegisterResource. Waiting here is what in-tree SC did by blocking the
	// consumer's provisioning on collector.Outputs().
	if err := collector.AwaitOutputs(ctx); err != nil {
		p.progress.Error("compute-process", err.Error())
		return nil, err
	}

	resp = collector.ToResponse()
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

// decodeInputsAs lifts the plugin bus's input map into a typed struct. For
// AtlasConfig the plugin's api package emits an `atlasConfigJson` scalar
// fallback alongside the structured bus fields, because older SC cores
// serialised only scalars onto the plugin bus (nested maps and lists were
// dropped). This function prefers the complete JSON payload when present,
// then falls back to a yaml round-trip through api/pkg/plugin.InputsToPlain
// via runtime.DecodeInputs — which handles nested maps and lists correctly
// (unlike the scalar-only unwrap this file used to carry inline).
func decodeInputsAs[T any](inputs map[string]*scpluginv1.InputValue, out *T) error {
	// AtlasConfig emits a scalar JSON fallback because older SC cores omit
	// maps/lists from the bus. Prefer the complete payload when available.
	if payload := inputs["atlasConfigJson"]; payload != nil {
		raw := payload.GetStringValue()
		if raw == "" {
			raw = string(payload.GetBytesValue())
		}
		if err := json.Unmarshal([]byte(raw), out); err != nil {
			return errors.Wrap(err, "decode complete Atlas config")
		}
		return nil
	}
	// Fall back to the structured bus fields via a yaml round-trip through
	// scPlugin.InputsToPlain, which preserves nested maps and lists.
	// `runtime.DecodeInputs` wraps that round-trip; we inline the shape
	// here only because the config_transport_test's scalar-only path exercises
	// a yaml.v3 edge case AtlasConfig's own fallback test fixtures target.
	plain := scPlugin.InputsToPlain(inputs)
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

// resourceInputFromComputeProcess delegates to runtime.ResourceInputForComputeProcess
// — a thin wrapper so parent_info_test.go keeps using mongodb's own test API,
// and the AtlasConfig cfg is attached in the local shape before handing off.
func resourceInputFromComputeProcess(req *scpluginv1.ComputeProcessRequest, cfg *mongoApi.AtlasConfig, parent *pApi.ParentInfo) api.ResourceInput {
	return runtime.ResourceInputForComputeProcess(req, parent, cfg)
}

// computeProvider builds the Atlas provider the compute processor's resources use.
func (p *mongoPlugin) computeProvider(pctx *sdk.Context, stack api.Stack, req *scpluginv1.ComputeProcessRequest, cfg *mongoApi.AtlasConfig, parent *pApi.ParentInfo) (sdk.ProviderResource, error) {
	resName := req.GetResourceName()
	if parent != nil && parent.DependsOnResource != nil && parent.DependsOnResource.Name != "" {
		resName = fmt.Sprintf("%s--%s", resName, parent.DependsOnResource.Name)
	}
	env := req.GetHeader().GetEnvironment()
	if parent != nil && parent.ParentEnv != "" {
		env = parent.ParentEnv
	}
	out, err := mongoInternal.Provider(pctx, stack, api.ResourceInput{
		Descriptor: &api.ResourceDescriptor{
			Type:   mongoApi.ProviderType,
			Name:   fmt.Sprintf("%s--%s--%s--%s--provider", stack.Name, req.GetResourceType(), resName, env),
			Config: api.Config{Config: cfg},
		},
		StackParams: &api.StackParams{StackName: stack.Name, Environment: env},
	}, pApi.ProvisionParams{Log: logger.New()})
	if err != nil {
		return nil, errors.Wrap(err, "init mongodb-atlas provider for compute-process")
	}
	prov, ok := out.Ref.(sdk.ProviderResource)
	if !ok {
		return nil, errors.Errorf("mongodb Provider returned %T, not sdk.ProviderResource", out.Ref)
	}
	return prov, nil
}

// waitForRef blocks until a Pulumi Output in a resource ref resolves —
// Settle drains pending RegisterResource goroutines before the RPC
// response, but each handler still needs a concrete string for the Urn
// field of its response, and this is how we pluck one out of a resource
// ref. ALWAYS add a case here for a new ref type — see
// forge/docs/design/2026-10-24/sc-plugin-architecture/04-no-destroy-invariant.md
// Addendum V for the race the default-case fallthrough would reintroduce.
func waitForRef(ref any) string {
	if ref == nil {
		return ""
	}
	switch r := ref.(type) {
	case string:
		return r
	case *mongoInternal.ClusterOutput:
		// mongoInternal.Cluster returns this concrete type as its Ref.
		// Await the underlying Cluster resource's URN rather than falling
		// through to the default Sprintf which produces the pointer
		// representation. r.Cluster is nil during an adopt-only path
		// (AdoptCluster builds a ClusterOutput without a *Cluster).
		if r.Cluster == nil {
			return ""
		}
		ch := make(chan string, 1)
		r.Cluster.URN().ApplyT(func(u sdk.URN) sdk.URN {
			select {
			case ch <- string(u):
			default:
			}
			return u
		})
		return <-ch
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
	// Full goroutine traces on panic. Without this, Go's default
	// GOTRACEBACK=`single` prints only the panicking goroutine, and the
	// stack trace is often what's needed to identify a nil-ptr root
	// cause. sc-plugin-aws sets this for the same reason.
	_ = os.Setenv("GOTRACEBACK", "all")
	impl := &mongoPlugin{progress: scPlugin.NewPublisher(0)}
	scPlugin.Serve(scPlugin.ServerConfig{
		Impl: impl,
		// Debug level forwards subprocess stderr (which carries panic
		// headers + goroutine stacks via hashicorp/go-plugin's
		// logStreamer) through SC core's hclog pipeline. Keeping it at
		// Warn (the previous value) dropped the only diagnostic ever
		// emitted on a plugin crash — observed empirically on forge run
		// 41191832415 (2026-10-31) where the panic header appeared at
		// Debug but goroutine stacks were truncated by the parent's
		// own filter.
		Logger: hclog.New(&hclog.LoggerOptions{
			Name:   "sc-plugin-mongodb",
			Level:  hclog.Debug,
			Output: os.Stderr,
		}),
	})
}
