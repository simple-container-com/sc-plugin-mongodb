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
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"gopkg.in/yaml.v3"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/pkg/errors"
	sdk "github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi/internals"
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
		// DryRun landed on RequestHeader via api PR adding dry_run to the
		// plugin bus. Without this the plugin's pctx thinks DryRun() is
		// always false, which breaks `if ctx.DryRun()` preview-skip
		// guards. mongodb has no such guard today, but setting it keeps
		// the pctx aligned with reality for future consumers and is
		// identical to the fix sc-plugin-cloudflare needs for its Pages
		// uploader hang. Zero-value (false) on older SC cores is exactly
		// what the pctx had before this field existed, so no behaviour
		// change for operators on old SC.
		DryRun: h.GetDryRun(),
	})
	if err != nil {
		return nil, nil, errors.Wrap(err, "open pulumi context from monitor address")
	}
	return pctx, func() { pctx.Close() }, nil
}

func (p *mongoPlugin) Provider(ctx context.Context, req *scpluginv1.ProviderRequest) (*scpluginv1.ProviderResponse, error) {
	pctx, _, err := p.pulumiContextFromHeader(ctx, req.GetHeader())
	if err != nil {
		return nil, err
	}
	// DO NOT close pctx here. See the aws plugin's identical note at
	// sc-plugin-aws/plugin/cmd/sc-plugin-aws/main.go:222 and Addendum
	// P/Q/R of 04-no-destroy-invariant.md: closing the context before
	// Pulumi's background RegisterResource goroutines have talked to
	// the monitor drops every in-flight registration silently, and the
	// engine then plans a destroy of every pre-existing state entry.
	// This is the exact bug that cascade-deleted the fleet's shared
	// Atlas project on 2026-10-31 (parent provision 41195331666).
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
	// Drain pending RegisterResource RPCs before returning — see drain.go.
	// Without this, the provider register goroutine may not have reached
	// the monitor when the handler returns, so SC core sees zero URNs
	// for this plugin and plans a destroy of its state entries. The
	// 2026-10-31 cascade delete of Atlas project 697a574e90d5387e4a889f5f
	// was this bug plus the pctx-close bug compounded. The aws plugin
	// has an identical drain for the same reason — see
	// sc-plugin-aws/plugin/cmd/sc-plugin-aws/main.go.
	if derr := drainContext(pctx); derr != nil {
		p.progress.Error("provider", "drain: "+derr.Error())
		return nil, derr
	}
	urn := waitForRef(out.Ref)
	p.progress.Info("provider", fmt.Sprintf("provider ready (urn=%s)", urn))
	return &scpluginv1.ProviderResponse{ProviderUrn: urn}, nil
}

func (p *mongoPlugin) Provision(ctx context.Context, req *scpluginv1.ProvisionRequest) (*scpluginv1.ProvisionResponse, error) {
	pctx, _, err := p.pulumiContextFromHeader(ctx, req.GetHeader())
	if err != nil {
		return nil, err
	}
	// DO NOT close pctx — see Provider() above. The 2026-10-31 cascade
	// delete of project 697a574e90d5387e4a889f5f on parent provision
	// 41195331666 was this exact bug firing from this exact function.
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
	// its provider the same way at `main.go:281` (`buildAwsProvider`).
	//
	// The resource name must match what the in-tree code produced so URNs
	// stay stable across the plugin split — see computeProvider() below for
	// the formula source.
	stack := api.Stack{Name: req.GetHeader().GetStackName()}
	providerResName := fmt.Sprintf("%s--%s--%s--%s--provider",
		stack.Name, req.GetResourceType(), req.GetResourceName(), req.GetHeader().GetEnvironment())
	provOut, err := mongoInternal.Provider(pctx, stack, api.ResourceInput{
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
	out, err := mongoInternal.Cluster(pctx, stack, input, pApi.ProvisionParams{Log: logger.New(), Provider: provider})
	if err != nil {
		p.progress.Error("provision", err.Error())
		return nil, err
	}
	// Drain pending RegisterResource RPCs — see drain.go and the
	// Provider() comment above. The Cluster/DatabaseUser/RandomPassword/
	// BackupSchedule/ProjectIpAccess registrations happen in
	// `mongoInternal.Cluster`'s goroutines; without this drain the
	// handler returns before any of them reach the monitor, and the
	// engine sees them as missing-from-state → cascade-delete at apply
	// time. 2026-10-31 incident, parent provision 41195331666.
	if derr := drainContext(pctx); derr != nil {
		p.progress.Error("provision", "drain: "+derr.Error())
		return nil, derr
	}
	urn := waitForRef(out.Ref)
	// Harvest the pctx's exports and hand them back on the wire.
	// SC core re-exports them against its OWN pctx so they land in the
	// parent stack root's RegisterResourceOutputs call — WITHOUT this,
	// `ctx.Export(...)` on the plugin's pctx stays in the plugin and
	// never reaches the parent stack root; every downstream
	// `uses: [mongodb]` compute-processor's `GetParentOutput(...)`
	// returns empty. Live-caught 2026-10-31: parent `/forge/infra` had
	// 6 output keys, all templates, zero plugin resource keys.
	exports, exportsErr := harvestPctxExports(ctx, pctx)
	if exportsErr != nil {
		p.progress.Warn("provision", "harvest exports: "+exportsErr.Error())
	}
	p.progress.Info("provision", fmt.Sprintf("cluster %q registered (urn=%s, exports=%d)", req.GetResourceName(), urn, len(exports)))
	return &scpluginv1.ProvisionResponse{Urn: urn, Outputs: exports}, nil
}

func (p *mongoPlugin) ComputeProcess(ctx context.Context, req *scpluginv1.ComputeProcessRequest) (*scpluginv1.ComputeProcessResponse, error) {
	pctx, _, err := p.pulumiContextFromHeader(ctx, req.GetHeader())
	if err != nil {
		return nil, err
	}
	// DO NOT close pctx — same reason as Provider/Provision above.
	// ComputeProcess also registers a DatabaseUser + a RandomPassword
	// via createDatabaseUser inside appendUsesResourceContext's ApplyT,
	// which racecloses the same way the Provision registrations did.
	p.progress.Info("compute-process", fmt.Sprintf("resource=%q type=%q", req.GetResourceName(), req.GetResourceType()))

	// ClusterComputeProcessor consumes params.ParentStack. v2 proto
	// packs it as ParentInfoJson — unmarshal back into the Go struct the
	// in-tree code expects.
	parent, err := decodeParentInfo(req.GetParentInfoJson())
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
	provider, err := p.computeProvider(pctx, stack, req, cfg, parent)
	if err != nil {
		p.progress.Error("compute-process", err.Error())
		return nil, err
	}

	// ClusterComputeProcessor returns an api.ResourceOutput and mutates
	// the collector. Our sidecar wrapper is the collector — the SC-core
	// sidecar then applies our env_vars + tpl_extensions back into the
	// live core-side collector.
	collector := newCapturingCollector()
	_, err = mongoInternal.ClusterComputeProcessor(pctx, stack, input, collector, pApi.ProvisionParams{
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
	// env at all — and the deferred pctx.Close() then cuts the user's in-flight
	// RegisterResource. Waiting here is what in-tree SC did by blocking the
	// consumer's provisioning on collector.Outputs().
	if err := collector.AwaitOutputs(ctx); err != nil {
		p.progress.Error("compute-process", err.Error())
		return nil, err
	}
	// AwaitOutputs blocks until each ApplyT resolves, but the DatabaseUser
	// + RandomPassword RegisterResource RPCs ride a separate goroutine
	// path that AwaitOutputs doesn't touch. Drain those too, same reason
	// as Provider/Provision above (see drain.go). The 2026-10-31 incident
	// hit both call sites; both get the drain.
	if derr := drainContext(pctx); derr != nil {
		p.progress.Error("compute-process", "drain: "+derr.Error())
		return nil, derr
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
// use. AtlasConfig's scalar JSON fallback preserves structured settings even
// with SC cores that transmit only scalar InputValues.
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

// resourceInputFromComputeProcess mirrors the in-tree processResourceDependency: the
// resource lives in the PARENT's env (params.CopyForParentEnv(resEnv)), not the consuming
// stack's env from the request header — e.g. a staging-ru client using the parent's
// staging cluster must look up "<parent>--mongodb--staging-id", not "...--staging-ru-id".
func resourceInputFromComputeProcess(req *scpluginv1.ComputeProcessRequest, cfg *mongoApi.AtlasConfig, parent *pApi.ParentInfo) api.ResourceInput {
	env := req.GetHeader().GetEnvironment()
	if parent != nil && parent.ParentEnv != "" {
		env = parent.ParentEnv
	}
	return api.ResourceInput{
		Descriptor: &api.ResourceDescriptor{
			Type:   req.GetResourceType(),
			Name:   req.GetResourceName(),
			Config: api.Config{Config: cfg},
		},
		StackParams: &api.StackParams{
			StackName:   req.GetHeader().GetStackName(),
			Environment: env,
		},
	}
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
// same reason + same shape as sc-plugin-cloudflare/yandex/cloudru.
//
// IMPORTANT: an unhandled `ref` type falls into `default` which Sprintf's
// the pointer without awaiting ANY URN. That is NOT just a cosmetic
// miss — Pulumi SDK schedules RegisterResource on a goroutine triggered
// by the first consumer of the Resource's URN output; if nothing ever
// consumes it in-handler, the register goroutine may never fire its
// gRPC call to the monitor. drainContext() only drains RPCs that have
// *started*; if the SDK is still waiting for the URN-output consumer
// to kick things off, drain sees `rpcs == 0` and returns immediately.
// We saw this live post-cascade on 2026-10-31: the Cluster URN was
// built, the handler returned "cluster registered", drain returned,
// and the Pulumi monitor never got a RegisterResource call for the
// Cluster. ALWAYS add a case here for a new ref type.
func waitForRef(ref any) string {
	if ref == nil {
		return ""
	}
	switch r := ref.(type) {
	case string:
		return r
	case *mongoInternal.ClusterOutput:
		// The Provision handler returns a *ClusterOutput wrapping the Cluster
		// + Project + optional PrivateLink. Awaiting EACH non-nil resource's
		// URN forces the register goroutine to actually dispatch the gRPC
		// RegisterResource — same pattern as sc-plugin-cloudflare/await.go.
		// Return the Cluster URN as the primary ref (it is what SC core
		// records in the sidecar's state).
		ctx := context.Background()
		if r.Project != nil {
			_, _ = internals.UnsafeAwaitOutput(ctx, r.Project.URN())
		}
		if r.PrivateLinkEndpointService != nil {
			_, _ = internals.UnsafeAwaitOutput(ctx, r.PrivateLinkEndpointService.URN())
		}
		if r.Cluster != nil {
			out, err := internals.UnsafeAwaitOutput(ctx, r.Cluster.URN())
			if err == nil {
				if urn, ok := out.Value.(sdk.URN); ok {
					return string(urn)
				}
			}
		}
		return ""
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
		// own filter. See sc-plugin-aws main.go for the same shape.
		Logger: hclog.New(&hclog.LoggerOptions{
			Name:   "sc-plugin-mongodb",
			Level:  hclog.Debug,
			Output: os.Stderr,
		}),
	})
}

// decodeParentInfo reads ComputeProcessRequest.parent_info_json. Core writes it with
// json.Marshal on an untagged struct, so the keys are the Go field names ("StackName",
// "FullReference", …). It must be decoded as JSON: yaml.v3 matches untagged fields by
// their lowercased names and leaves every field empty, which broke every `uses:` of a
// mongodb-atlas resource (an empty parent StackReference).
func decodeParentInfo(raw []byte) (*pApi.ParentInfo, error) {
	parent := &pApi.ParentInfo{}
	if len(raw) == 0 {
		return parent, nil
	}
	if err := json.Unmarshal(raw, parent); err != nil {
		return nil, errors.Wrap(err, "decode ParentInfo from compute-process request")
	}
	return parent, nil
}

// harvestPctxExports reads every `pctx.Export(k, v)` call made during the
// RPC handler and converts each value into an InputValue the wire can carry.
// Awaits every sdk.Output so a late-resolving ApplyT chain (e.g.
// Cluster.MongoUri, which only resolves after the register goroutine
// completes) still gets its concrete string into the response.
//
// SC core re-exports whatever this returns onto its OWN pctx — that is what
// lands the keys in the parent stack root's `RegisterResourceOutputs`.
// Without this, the plugin's exports stay in the plugin and the parent root
// has no plugin keys (live-caught 2026-10-31 after the mongodb cascade adopt;
// see Addendum U of 04-no-destroy-invariant.md).
func harvestPctxExports(ctx context.Context, pctx *sdk.Context) (map[string]*scpluginv1.InputValue, error) {
	raw := pctx.GetCurrentExportMap()
	out := make(map[string]*scpluginv1.InputValue, len(raw))
	for k, v := range raw {
		plain, secret, err := awaitInputToPlain(ctx, v)
		if err != nil {
			return out, errors.Wrapf(err, "await export %q", k)
		}
		if iv := anyToInputValue(plain); iv != nil {
			iv.Secret = secret
			out[k] = iv
		}
	}
	return out, nil
}

// awaitInputToPlain returns (concrete value, secret flag, error). The
// Secret flag MUST be preserved: SC core wraps the re-export with
// sdk.ToSecret when this bool is true, so a secret-marked output
// doesn't land plaintext in the parent stack root. mongodb's exports
// today are not secret-flagged (Mongo URIs are credential-free by
// design in Cluster()), but the same wire shape ships for every plugin
// and losing the flag would be a plaintext leak for anything that is.
// Live-caught 2026-10-31 against the aws harvest; see Addendum V of
// forge/docs/design/2026-10-24/sc-plugin-architecture/04-no-destroy-invariant.md.
func awaitInputToPlain(ctx context.Context, v sdk.Input) (any, bool, error) {
	if v == nil {
		return nil, false, nil
	}
	// Easy cases first — Pulumi's primitive Input wrappers resolve inline.
	switch tv := v.(type) {
	case sdk.String:
		return string(tv), false, nil
	case sdk.Bool:
		return bool(tv), false, nil
	case sdk.Int:
		return int(tv), false, nil
	case sdk.Float64:
		return float64(tv), false, nil
	}
	// Pulumi's Output types all implement sdk.Output; await their value.
	if out, ok := v.(sdk.Output); ok {
		resolved, aerr := internals.UnsafeAwaitOutput(ctx, out)
		if aerr != nil {
			return nil, false, aerr
		}
		return unwrapPlain(resolved.Value), resolved.Secret, nil
	}
	return nil, false, nil
}

func unwrapPlain(v any) any {
	// Common Pulumi concrete values are plain Go types; nothing to do.
	switch tv := v.(type) {
	case sdk.URN:
		return string(tv)
	case sdk.ID:
		return string(tv)
	}
	return v
}

// anyToInputValue is a thin mirror of api/pkg/clouds/pulumi.toInputValue —
// kept local because that helper is unexported and this plugin only needs
// the scalar branches today. Returns nil for a nil value (SC core treats
// the absent key as a no-op, matching api/pkg/plugin.InputValueToPlain).
func anyToInputValue(v any) *scpluginv1.InputValue {
	if v == nil {
		return nil
	}
	switch sv := v.(type) {
	case string:
		if sv == "" {
			return nil
		}
		return &scpluginv1.InputValue{Value: &scpluginv1.InputValue_StringValue{StringValue: sv}}
	case bool:
		return &scpluginv1.InputValue{Value: &scpluginv1.InputValue_BoolValue{BoolValue: sv}}
	case int:
		return &scpluginv1.InputValue{Value: &scpluginv1.InputValue_IntValue{IntValue: int64(sv)}}
	case int64:
		return &scpluginv1.InputValue{Value: &scpluginv1.InputValue_IntValue{IntValue: sv}}
	case float64:
		return &scpluginv1.InputValue{Value: &scpluginv1.InputValue_DoubleValue{DoubleValue: sv}}
	case []byte:
		return &scpluginv1.InputValue{Value: &scpluginv1.InputValue_BytesValue{BytesValue: sv}}
	}
	return nil
}
