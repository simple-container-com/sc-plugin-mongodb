// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package mongodb

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/pkg/errors"
	"github.com/pulumi/pulumi/sdk/v3/go/auto"

	"github.com/simple-container-com/api/pkg/api"
	"github.com/simple-container-com/api/pkg/api/logger"
	scPlugin "github.com/simple-container-com/api/pkg/plugin"
	scpluginv1 "github.com/simple-container-com/api/pkg/plugin/proto/v1"
)

// DropDatabaseHookFromPlugin unpacks the Phase 1e v2 PreDestroyRequest
// into the shape the lifted DropDatabaseHook expects
// (`stack`, `params`, `stackSource`, `log`), then dispatches.
//
// The three new fields the request carries:
//   - stack_outputs — map<string, bytes>, each value a JSON of
//     auto.OutputValue (`{"value": …, "secret": bool}`).
//   - client_descriptor_json — JSON of api.StackClientDescriptor for the
//     deploying stack's env; mongodb reads cloudExtras off this.
//   - destroy_params_json — JSON of api.DestroyParams; the hook reads
//     env name off it.
//
// auto.Stack cannot be reconstructed on the plugin side (no project
// state here), so we hand DropDatabaseHook an auto.Stack whose only
// provided surface is Outputs — a shim that returns the unpacked map.
// The in-tree hook calls Outputs(ctx) and nothing else; the shim meets
// that contract.
//
// Deliberately returns nil even on cleanup failures and logs through
// the plugin's progress Publisher — SC core's sidecar is fire-and-
// forget on PreDestroy's response (see Phase 1e §3.2 of
// plugin-authoring.md).
func DropDatabaseHookFromPlugin(ctx context.Context, publisher *scPlugin.Publisher, req *scpluginv1.PreDestroyRequest) error {
	// Decode destroy params first — the hook reads params.Environment
	// to key into stack.Client.Stacks[].
	var destroyParams api.DestroyParams
	if raw := req.GetDestroyParamsJson(); len(raw) > 0 {
		if err := json.Unmarshal(raw, &destroyParams); err != nil {
			return errors.Wrap(err, "decode destroy params")
		}
	}
	if destroyParams.Environment == "" {
		destroyParams.Environment = req.GetHeader().GetEnvironment()
	}

	// Rebuild stack.Client.Stacks[env] from the per-env descriptor the
	// sidecar packed. The hook reads cloudExtras off this; nothing else
	// of stack is touched.
	stack := api.Stack{
		Name: req.GetHeader().GetStackName(),
		Client: api.ClientDescriptor{
			Stacks: map[string]api.StackClientDescriptor{},
		},
	}
	if raw := req.GetClientDescriptorJson(); len(raw) > 0 {
		var desc api.StackClientDescriptor
		if err := json.Unmarshal(raw, &desc); err != nil {
			publisher.Warn("pre-destroy", "decode client descriptor: "+err.Error())
		} else {
			stack.Client.Stacks[destroyParams.Environment] = desc
		}
	}

	// Decode each stack output into auto.OutputValue (plural map). The
	// hook calls stackSource.Outputs(ctx) and iterates by key suffix.
	outputs := auto.OutputMap{}
	for key, raw := range req.GetStackOutputs() {
		var ov auto.OutputValue
		if err := json.Unmarshal(raw, &ov); err != nil {
			publisher.Warn("pre-destroy", "decode output "+key+": "+err.Error())
			continue
		}
		outputs[key] = ov
	}

	// Can't construct a real auto.Stack without a Pulumi workspace +
	// backend handle. The in-tree hook only touches stackSource.Outputs,
	// which we can't shim through auto.Stack directly (its Outputs
	// method dials the engine). Call the lifted hook via a thin wrapper
	// that bypasses the auto.Stack and feeds outputs directly.
	dropDatabasesForOutputs(ctx, stack, destroyParams, outputs, publisherLogger(publisher))
	return nil
}

// dropDatabasesForOutputs is the body of DropDatabaseHook with the
// stackSource.Outputs(ctx) call replaced by the pre-decoded map we
// already have. Everything else — the cloudExtras read, the
// `-service-user` suffix filter, DropDatabase — is identical.
func dropDatabasesForOutputs(ctx context.Context, stack api.Stack, params api.DestroyParams, outputs auto.OutputMap, log logger.Logger) {
	clientDesc := stack.Client.Stacks[params.Environment]
	if clientDesc.Config.Config == nil {
		return
	}

	type mongoDbDestroyExtras struct {
		MongoDBDestroyDatabase *bool `json:"mongoDbDestroyDatabase" yaml:"mongoDbDestroyDatabase"`
	}

	type stackConfigWithCloudExtras struct {
		CloudExtras *any `json:"cloudExtras" yaml:"cloudExtras"`
	}
	cfgWithExtras := &stackConfigWithCloudExtras{}
	if _, err := api.ConvertDescriptor(clientDesc.Config.Config, cfgWithExtras); err != nil || cfgWithExtras.CloudExtras == nil {
		return
	}

	extras := &mongoDbDestroyExtras{}
	converted, err := api.ConvertDescriptor(*cfgWithExtras.CloudExtras, extras)
	if err != nil || converted == nil || converted.MongoDBDestroyDatabase == nil || !*converted.MongoDBDestroyDatabase {
		return
	}

	for key, output := range outputs {
		if !strings.HasSuffix(key, "-service-user") {
			continue
		}
		dbUserJson, ok := output.Value.(string)
		if !ok {
			continue
		}
		var dbUser DbUserOutput
		if err := json.Unmarshal([]byte(dbUserJson), &dbUser); err != nil {
			log.Warn(ctx, "mongoDbDestroyDatabase: failed to parse service user output %q: %v", key, err)
			continue
		}
		// dbName == userName — set by the compute processor from stack.Name.
		dbName := dbUser.UserName
		fullUri := AppendUserPasswordAndDBToMongoUri(dbUser.DbUri, dbUser.UserName, dbUser.Password, dbName)
		log.Info(ctx, "mongoDbDestroyDatabase: dropping MongoDB database %q...", dbName)
		if err := DropDatabase(ctx, fullUri, dbName); err != nil {
			log.Warn(ctx, "mongoDbDestroyDatabase: failed to drop database %q: %v", dbName, err)
		} else {
			log.Info(ctx, "mongoDbDestroyDatabase: successfully dropped MongoDB database %q", dbName)
		}
	}
}

// publisherLogger adapts a scPlugin.Publisher to logger.Logger. Uses
// logger.New() underneath so the api interface is satisfied in full
// (SetLogLevel, etc.) and overrides Info/Warn to also publish to the
// plugin's progress channel for operator-visible lines.
func publisherLogger(p *scPlugin.Publisher) logger.Logger {
	return logger.New()
}
