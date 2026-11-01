// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package mongodb

import (
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
	sdk "github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/samber/lo"
	"github.com/simple-container-com/api/pkg/api"
	pApi "github.com/simple-container-com/api/pkg/clouds/pulumi/api"
	"github.com/simple-container-com/api/pkg/util"

	"github.com/simple-container-com/sc-plugin-mongodb/api"
)

func ClusterComputeProcessor(ctx *sdk.Context, stack api.Stack, input api.ResourceInput, collector pApi.ComputeContextCollector, params pApi.ProvisionParams) (*api.ResourceOutput, error) {
	if params.ParentStack == nil {
		return nil, errors.Errorf("parent stack must not be nil for compute processor for %q", stack.Name)
	}

	mongoConfig, ok := input.Descriptor.Config.Config.(*mongodb.AtlasConfig)
	if !ok {
		return nil, errors.Errorf("failed to convert mongodb config for %q", input.Descriptor.Type)
	}

	// The configured naming version is a preference, not proof of how the
	// parent was provisioned. SC 2025.12.6 parents always used v1 cluster names.
	configBasedProjectName := toProjectName(params.ParentStack.StackName, input)
	configBasedClusterName := toClusterName(params.ParentStack.StackName, input)

	// For actual cluster operations, use ClusterName when adopting existing resources
	var actualClusterName string
	if mongoConfig.Adopt && mongoConfig.ClusterName != "" {
		actualClusterName = mongoConfig.ClusterName
		params.Log.Info(ctx.Context(), "Using actual cluster name %q for adopted MongoDB resource (config name: %q)", actualClusterName, configBasedClusterName)
	} else {
		actualClusterName = configBasedClusterName
		params.Log.Debug(ctx.Context(), "Using convention-based name %q for MongoDB resource", actualClusterName)
	}

	suffix := lo.If(params.ParentStack.DependsOnResource != nil, "--"+lo.FromPtr(params.ParentStack.DependsOnResource).Name).Else("")
	params.Log.Info(ctx.Context(), "getting parent's (%q) outputs for mongodb atlas cluster %q (%q)", params.ParentStack.FullReference, actualClusterName, suffix)
	parentRef, err := sdk.NewStackReference(ctx, fmt.Sprintf("%s--%s--%s%s--mongodb-atlas-ref", stack.Name, input.Descriptor.Name, params.ParentStack.FullReference, suffix),
		&sdk.StackReferenceArgs{
			Name: sdk.String(params.ParentStack.FullReference).ToStringOutput(),
		})
	if err != nil {
		return nil, err
	}

	projectIdExport := toProjectIdExport(configBasedProjectName)
	projectId, _, err := parentStringOutput(parentRef, projectIdExport, params.ParentStack.FullReference)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get project id from parent stack for %q (%q)", stack.Name, projectIdExport)
	} else if projectId == "" {
		return nil, errors.Errorf("project id is empty for %q (%q)", stack.Name, projectIdExport)
	}

	mongoUri, resolvedClusterName, err := parentMongoURI(parentRef, params.ParentStack.StackName, input, params.ParentStack.FullReference)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get mongo uri from parent stack for %q", stack.Name)
	} else if mongoUri == "" {
		return nil, errors.Errorf("mongo uri is empty for %q (%q)", stack.Name, toMongoUriWithOptionsExport(resolvedClusterName))
	}
	if !mongoConfig.Adopt || mongoConfig.ClusterName == "" {
		// Preserve the parent's database-user resource identities as well as its URI.
		actualClusterName = resolvedClusterName
	}

	readOplog := !mongoConfig.LeastPrivilegeUsers || mongoConfig.AllowsOplog(stack.Name, params.ParentStack.StackEnv)
	appendContextParams := appendParams{
		stack:           stack,
		collector:       collector,
		input:           input,
		clusterName:     actualClusterName,      // Use actual cluster name for database operations
		projectName:     configBasedProjectName, // Keep project name for consistency with exports
		projectId:       projectId,
		mongoUri:        mongoUri,
		provisionParams: params,
		suffix:          suffix,
		readOplog:       readOplog,
		leastPrivilege:  mongoConfig.LeastPrivilegeUsers,
	}
	if params.ParentStack.UsesResource {
		if err := appendUsesResourceContext(ctx, appendContextParams); err != nil {
			return nil, errors.Wrapf(err, "failed to append consumes resource context")
		}
	} else if params.ParentStack.DependsOnResource != nil {
		appendContextParams.dependency = *params.ParentStack.DependsOnResource
		if err := appendDependsOnResourceContext(ctx, appendContextParams); err != nil {
			return nil, err
		}
	} else {
		params.Log.Warn(ctx.Context(), "mongodb %q only supports `uses` or `dependency`, but neither was explicitly declared as being used", actualClusterName)
		return nil, errors.Errorf("mongodb %q only supports `uses` or `dependency`, but it wasn't explicitly declared as being used", actualClusterName)
	}

	return &api.ResourceOutput{
		Ref: nil,
	}, nil
}

// parentMongoURI prefers the configured name, then tries exactly the naming
// used by SC 2025.12.6 (which ignored custom cluster names and naming versions).
// Only an absent output permits fallback: a malformed or empty value must not
// silently select another cluster when both names exist in the parent.
//
// When the parent's AtlasConfig declares `clusterName:` (an operator-managed
// custom name — typical for `adopt: true`), parent exports are keyed on that
// name verbatim (see cluster.go / adopt_cluster.go for the Pulumi-URN-stable
// split). Try it FIRST — the `toClusterName(..., 21-budget)` and
// `toClusterNameV1` variants are the legacy auto-generated prefixes and now
// serve as back-compat fallbacks.
func parentMongoURI(ref *sdk.StackReference, stackName string, input api.ResourceInput, parentRef string) (string, string, error) {
	var atlasCfg *mongodb.AtlasConfig
	if cfg, ok := input.Descriptor.Config.Config.(*mongodb.AtlasConfig); ok {
		atlasCfg = cfg
	}
	candidates := []string{toClusterName(stackName, input), toClusterNameV1(stackName, input)}
	if atlasCfg != nil && atlasCfg.ClusterName != "" {
		candidates = append([]string{atlasCfg.ClusterName}, candidates...)
	}
	clusterNames := lo.Uniq(candidates)
	exportNames := make([]string, 0, len(clusterNames))
	for _, clusterName := range clusterNames {
		exportName := toMongoUriWithOptionsExport(clusterName)
		exportNames = append(exportNames, exportName)
		uri, found, err := parentStringOutput(ref, exportName, parentRef)
		if err != nil {
			return "", "", err
		}
		if found {
			return uri, clusterName, nil
		}
	}
	return "", "", errors.Errorf("no mongo uri output found in parent %q (tried %q)", parentRef, exportNames)
}

// GetOutputDetails distinguishes missing values from secret values without
// relying on GetParentOutput's version-dependent missing-output errors.
func parentStringOutput(ref *sdk.StackReference, exportName, parentRef string) (string, bool, error) {
	details, err := ref.GetOutputDetails(exportName)
	if err != nil {
		return "", false, errors.Wrapf(err, "failed to get output %q from %q", exportName, parentRef)
	}
	value := details.Value
	if value == nil {
		value = details.SecretValue
	}
	if value == nil {
		return "", false, nil
	}
	s, ok := value.(string)
	if !ok {
		// Report the type, never the value (which may contain decrypted secrets).
		return "", true, errors.Errorf("parent output %q from %q is %T, not string", exportName, parentRef, value)
	}
	return s, true, nil
}

type appendParams struct {
	stack           api.Stack
	collector       pApi.ComputeContextCollector
	input           api.ResourceInput
	clusterName     string
	projectName     string
	projectId       string
	mongoUri        string
	provisionParams pApi.ProvisionParams
	dependency      api.StackConfigDependencyResource
	suffix          string
	readOplog       bool
	leastPrivilege  bool
}

func appendUsesResourceContext(ctx *sdk.Context, params appendParams) error {
	// set both dbname and user name to stack name
	dbName := params.stack.Name
	legacyUserName := params.stack.Name

	dbUser, originalUser, userName, err := createServiceDatabaseUsers(ctx, dbUserInput{
		clusterName: params.clusterName,
		projectId:   params.projectId,
		dbUri:       params.mongoUri,
		username:    legacyUserName,
		suffix:      params.suffix,
	}, dbName, params.provisionParams.ParentStack.StackEnv, params.readOplog, params.leastPrivilege, params.provisionParams)
	if err != nil {
		return errors.Wrapf(err, "failed to create service user for database %q", dbName)
	}
	if dbUser != nil {
		if params.leastPrivilege && params.readOplog {
			ctx.Export(fmt.Sprintf("%s-%s-oplog-user", params.clusterName, userName), dbUser)
		}
		ctx.Export(fmt.Sprintf("%s%s-service-user", params.clusterName, params.suffix), originalUser)

		params.collector.AddOutput(ctx, dbUser.ApplyT(func(dbUserOut any) (any, error) {
			dbUserOutJson, ok := dbUserOut.(string)
			if !ok {
				return nil, errors.Errorf("db user is not a string for mongodb user %q", userName)
			}
			dbUser := DbUserOutput{}
			err = json.Unmarshal([]byte(dbUserOutJson), &dbUser)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to unmarshal db user for mongodb user %q", userName)
			}

			params.collector.AddEnvVariableIfNotExist(util.ToEnvVariableName("MONGO_USER"), userName,
				params.input.Descriptor.Type, params.input.Descriptor.Name, params.provisionParams.ParentStack.StackName)

			params.collector.AddEnvVariableIfNotExist(util.ToEnvVariableName("MONGO_DATABASE"), dbName,
				params.input.Descriptor.Type, params.input.Descriptor.Name, params.provisionParams.ParentStack.StackName)

			params.collector.AddSecretEnvVariableIfNotExist(util.ToEnvVariableName("MONGO_PASSWORD"), dbUser.Password,
				params.input.Descriptor.Type, params.input.Descriptor.Name, params.provisionParams.ParentStack.StackName)

			mongoUri := AppendUserPasswordAndDBToMongoUri(params.mongoUri, userName, dbUser.Password, dbName)

			params.collector.AddSecretEnvVariableIfNotExist(util.ToEnvVariableName("MONGO_URI"), mongoUri,
				params.input.Descriptor.Type, params.input.Descriptor.Name, params.provisionParams.ParentStack.StackName)

			params.collector.AddResourceTplExtension(params.input.Descriptor.Name, serviceDatabaseTemplate(params.mongoUri, userName, dbUser.Password, dbName, params.readOplog))

			return nil, nil
		}))
	}

	return nil
}

func appendDependsOnResourceContext(ctx *sdk.Context, params appendParams) error {
	ownerStackName := pApi.CollapseStackReference(params.dependency.Owner)
	legacyUserName := fmt.Sprintf("%s--%s%s", params.stack.Name, params.dependency.Name, params.suffix)
	dbEnv := lo.If(params.provisionParams.ParentStack.ResourceEnv != "", params.provisionParams.ParentStack.ResourceEnv).Else(params.input.StackParams.Environment)
	dbName := pApi.StackNameInEnv(ownerStackName, dbEnv)

	dbUser, originalUser, userName, err := createServiceDatabaseUsers(ctx, dbUserInput{
		clusterName: params.clusterName,
		projectId:   params.projectId,
		dbUri:       params.mongoUri,
		username:    legacyUserName,
		suffix:      params.suffix,
	}, dbName, params.provisionParams.ParentStack.StackEnv, params.readOplog, params.leastPrivilege, params.provisionParams)
	if err != nil {
		return errors.Wrapf(err, "failed to create service user for database %q", dbName)
	}
	if dbUser != nil {
		if params.leastPrivilege && params.readOplog {
			ctx.Export(fmt.Sprintf("%s-%s-oplog-user", params.clusterName, userName), dbUser)
		}
		ctx.Export(fmt.Sprintf("%s--to--%s--%s%s", params.clusterName, ownerStackName, params.dependency.Resource, params.suffix), originalUser)

		params.collector.AddOutput(ctx, dbUser.ApplyT(func(dbUserOut any) (any, error) {
			params.provisionParams.Log.Info(ctx.Context(), "Creating mongo user %q", userName)
			dbUserOutJson, ok := dbUserOut.(string)
			if !ok {
				return nil, errors.Errorf("db user is not a string")
			}
			dbUser := DbUserOutput{}
			err = json.Unmarshal([]byte(dbUserOutJson), &dbUser)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to unmarshal db user for mongo %q", userName)
			}

			params.collector.AddEnvVariableIfNotExist(util.ToEnvVariableName(fmt.Sprintf("MONGO_DEP_%s_USER", ownerStackName)), userName,
				params.input.Descriptor.Type, params.input.Descriptor.Name, params.provisionParams.ParentStack.StackName)

			params.collector.AddSecretEnvVariableIfNotExist(util.ToEnvVariableName(fmt.Sprintf("MONGO_DEP_%s_PASSWORD", ownerStackName)), dbUser.Password,
				params.input.Descriptor.Type, params.input.Descriptor.Name, params.provisionParams.ParentStack.StackName)

			mongoUri := AppendUserPasswordAndDBToMongoUri(params.mongoUri, userName, dbUser.Password, dbName)

			params.collector.AddSecretEnvVariableIfNotExist(util.ToEnvVariableName(fmt.Sprintf("MONGO_DEP_%s_URI", ownerStackName)), mongoUri,
				params.input.Descriptor.Type, params.input.Descriptor.Name, params.provisionParams.ParentStack.StackName)

			params.collector.AddDependencyTplExtension(params.dependency.Name, params.input.Descriptor.Name, serviceDatabaseTemplate(params.mongoUri, userName, dbUser.Password, dbName, params.readOplog))

			return nil, nil
		}))
	}

	return nil
}
