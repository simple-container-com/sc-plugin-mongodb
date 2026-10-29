// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package main

import (
	"context"
	"sync"

	"github.com/pkg/errors"
	sdk "github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi/internals"

	pApi "github.com/simple-container-com/api/pkg/clouds/pulumi/api"
	scpluginv1 "github.com/simple-container-com/api/pkg/plugin/proto/v1"
)

// capturingCollector implements pApi.ComputeContextCollector in-plugin
// and records what the lifted ClusterComputeProcessor mutates, so the
// plugin can translate those mutations into a Phase 1e v2
// ComputeProcessResponse.
//
// What crosses the bus (and thus is captured here): AddEnvVariableIfNotExist,
// AddSecretEnvVariableIfNotExist, AddResourceTplExtension,
// AddDependencyTplExtension. These map 1:1 to the response's EnvVars
// + TplExtensions fields.
//
// AddOutput(sdk.Output) does not cross either, but it is NOT a no-op: the
// outputs are recorded and AwaitOutputs blocks on them, because the env vars
// above are added from inside their ApplyT callbacks.
//
// What does NOT cross and stays as a no-op: AddDependency(sdk.Resource),
// AddPreProcessor, AddPostProcessor,
// ResolvePlaceholders, Run*Processors. These carry Pulumi handles or
// Go closures — see pkg/plugin/compute.go doc for the general rule.
// A plugin that applies them does so AGAINST ITS OWN sdk.Context,
// which ClusterComputeProcessor does: ctx.Export calls for mongo URIs
// land on the plugin's own context before this collector returns.
type capturingCollector struct {
	mu            sync.Mutex
	envVars       []pApi.ComputeEnvVariable
	resourceTpl   []resourceTplEntry
	dependencyTpl []dependencyTplEntry
	outputs       []sdk.Output
}

type resourceTplEntry struct {
	resName string
	values  map[string]string
}

type dependencyTplEntry struct {
	depName string
	resName string
	values  map[string]string
}

func newCapturingCollector() *capturingCollector {
	return &capturingCollector{}
}

func (c *capturingCollector) SecretEnvVariables() []pApi.ComputeEnvVariable { return nil }
func (c *capturingCollector) EnvVariables() []pApi.ComputeEnvVariable       { return nil }

func (c *capturingCollector) AddEnvVariableIfNotExist(name, value, resType, resName, stackName string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.envVars = append(c.envVars, pApi.ComputeEnvVariable{
		Name: name, Value: value, ResourceType: resType, ResourceName: resName, StackName: stackName, Secret: false,
	})
}

func (c *capturingCollector) AddSecretEnvVariableIfNotExist(name, value, resType, resName, stackName string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.envVars = append(c.envVars, pApi.ComputeEnvVariable{
		Name: name, Value: value, ResourceType: resType, ResourceName: resName, StackName: stackName, Secret: true,
	})
}

func (c *capturingCollector) AddResourceTplExtension(resName string, values map[string]string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resourceTpl = append(c.resourceTpl, resourceTplEntry{resName: resName, values: values})
}

func (c *capturingCollector) AddDependencyTplExtension(depName, resName string, values map[string]string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.dependencyTpl = append(c.dependencyTpl, dependencyTplEntry{depName: depName, resName: resName, values: values})
}

// The methods below are deliberately no-ops. See the type comment.
func (c *capturingCollector) AddDependency(sdk.Resource)                       {}
func (c *capturingCollector) Dependencies() []sdk.Resource                     { return nil }
func (c *capturingCollector) ResolvePlaceholders(any) error                    { return nil }
func (c *capturingCollector) AddPreProcessor(any, pApi.PreProcessor)           {}
func (c *capturingCollector) AddPostProcessor(any, pApi.PostProcessor)         {}
func (c *capturingCollector) GetPreProcessors(any) ([]pApi.PreProcessor, bool) { return nil, false }

func (c *capturingCollector) GetPostProcessors(any) ([]pApi.PostProcessor, bool) { return nil, false }
func (c *capturingCollector) RunPreProcessors(any, any) error                    { return nil }
func (c *capturingCollector) RunPostProcessors(any, any) error                   { return nil }
func (c *capturingCollector) ResourceOutputs() pApi.ResourcesOutputs             { return nil }

// AddOutput records an output whose resolution mutates this collector (the
// mongodb processor adds every MONGO_* variable inside an ApplyT on the database
// user). It is not a no-op: AwaitOutputs must wait for it before ToResponse.
func (c *capturingCollector) AddOutput(_ *sdk.Context, o sdk.Output) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.outputs = append(c.outputs, o)
}

func (c *capturingCollector) Outputs() []sdk.Output {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]sdk.Output(nil), c.outputs...)
}

// AwaitOutputs blocks until every output added via AddOutput has resolved, so the
// env vars their ApplyT callbacks add are captured. An output that resolves to an
// error fails the compute process rather than silently dropping its env.
func (c *capturingCollector) AwaitOutputs(ctx context.Context) error {
	for _, o := range c.Outputs() {
		if _, err := internals.UnsafeAwaitOutput(ctx, o); err != nil {
			return errors.Wrap(err, "await compute-context output")
		}
	}
	return nil
}

// ToResponse builds the plugin bus's ComputeProcessResponse from the
// captured mutations. The SC-core sidecar applies each entry back into
// the live core-side collector via AddEnvVariableIfNotExist etc.
func (c *capturingCollector) ToResponse() *scpluginv1.ComputeProcessResponse {
	c.mu.Lock()
	defer c.mu.Unlock()
	resp := &scpluginv1.ComputeProcessResponse{}
	for _, v := range c.envVars {
		resp.EnvVars = append(resp.EnvVars, &scpluginv1.ComputeEnvVariable{
			Name:         v.Name,
			Value:        v.Value,
			Secret:       v.Secret,
			ResourceName: v.ResourceName,
			ResourceType: v.ResourceType,
			StackName:    v.StackName,
		})
	}
	for _, t := range c.resourceTpl {
		resp.TplExtensions = append(resp.TplExtensions, &scpluginv1.TemplateExtension{
			Kind:    scpluginv1.TemplateExtensionKind_TEMPLATE_EXTENSION_KIND_RESOURCE,
			ResName: t.resName,
			Values:  t.values,
		})
	}
	for _, t := range c.dependencyTpl {
		resp.TplExtensions = append(resp.TplExtensions, &scpluginv1.TemplateExtension{
			Kind:    scpluginv1.TemplateExtensionKind_TEMPLATE_EXTENSION_KIND_DEPENDENCY,
			DepName: t.depName,
			ResName: t.resName,
			Values:  t.values,
		})
	}
	return resp
}
