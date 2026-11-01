// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package mongodb

// Golden builders from origin/main (26b1e5a); only function names changed.
import (
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/pulumi/pulumi-mongodbatlas/sdk/v3/go/mongodbatlas"
	sdk "github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/samber/lo"
	"github.com/simple-container-com/api/pkg/api"
	pApi "github.com/simple-container-com/api/pkg/clouds/pulumi/api"
	"github.com/simple-container-com/api/pkg/util"
	"github.com/simple-container-com/sc-plugin-mongodb/api"
)

func originMainCluster(ctx *sdk.Context, stack api.Stack, input api.ResourceInput, params pApi.ProvisionParams) (*api.ResourceOutput, error) {
	if input.Descriptor.Type != mongodb.ResourceTypeMongodbAtlas {
		return nil, errors.Errorf("unsupported mongodb-atlas type %q", input.Descriptor.Type)
	}

	out := &ClusterOutput{}

	atlasCfg, ok := input.Descriptor.Config.Config.(*mongodb.AtlasConfig)
	if !ok {
		return nil, errors.Errorf("failed to convert mongodb atlas config for %q", input.Descriptor.Type)
	}

	// Handle resource adoption - exit early if adopting
	if atlasCfg.Adopt {
		return AdoptCluster(ctx, stack, input, params)
	}

	projectName := toProjectName(stack.Name, input)
	clusterName := toClusterName(stack.Name, input)

	// Deletion protection check
	if atlasCfg.DeletionProtection {
		params.Log.Info(ctx.Context(), "⚠️ Deletion protection ENABLED for MongoDB cluster %q - resource will be protected from accidental deletion", clusterName)
	}

	// Log cluster naming information for transparency
	if atlasCfg.ClusterName != "" {
		params.Log.Info(ctx.Context(), "📝 Using custom cluster name %q for MongoDB resource %q in stack %q",
			clusterName, input.Descriptor.Name, input.StackParams.StackName)
	} else {
		params.Log.Info(ctx.Context(), "📝 Generated cluster name %q for MongoDB resource %q in stack %q",
			clusterName, input.Descriptor.Name, input.StackParams.StackName)
	}

	var projectId sdk.StringOutput
	opts := []sdk.ResourceOption{
		sdk.Provider(params.Provider),
	}

	// Apply deletion protection if enabled
	if atlasCfg.DeletionProtection {
		opts = append(opts, sdk.Protect(true))
		params.Log.Info(ctx.Context(), "🔒 Deletion protection applied to MongoDB cluster %q - use 'pulumi state unprotect' to disable", clusterName)
	}
	if atlasCfg.ProjectId == "" {
		projName := lo.If(atlasCfg.ProjectName != "", atlasCfg.ProjectName).Else(projectName)

		params.Log.Info(ctx.Context(), "configure MongoDB Atlas project %q for stack %q in %q", projName, input.StackParams.StackName, input.StackParams.Environment)
		project, err := mongodbatlas.NewProject(ctx, projectName, &mongodbatlas.ProjectArgs{
			Name:  sdk.String(projName),
			OrgId: sdk.String(atlasCfg.OrgId),
		}, opts...)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to create mongodb project for stack %q", stack.Name)
		}
		out.Project = project
		projectId = project.ID().ToStringOutput()
		opts = append(opts, sdk.DependsOn([]sdk.Resource{project}))
	} else {
		projectRes, err := mongodbatlas.LookupProject(ctx, &mongodbatlas.LookupProjectArgs{
			ProjectId: &atlasCfg.ProjectId,
		}, sdk.Provider(params.Provider))
		if err != nil {
			return nil, errors.Wrapf(err, "failed to lookup mongodb project for stack %q", stack.Name)
		}
		projectId = sdk.String(*projectRes.ProjectId).ToStringOutput()
	}
	ctx.Export(toProjectIdExport(projectName), projectId)

	sharedInstanceSizes := []string{"M0", "M2", "M5"}
	_, isSharedInstanceSize := lo.Find(sharedInstanceSizes, func(size string) bool {
		return size == atlasCfg.InstanceSize
	})

	params.Log.Info(ctx.Context(), "configure MongoDB Atlas cluster %q for stack %q in %q", clusterName, input.StackParams.StackName, input.StackParams.Environment)
	// terminationProtectionEnabled is the Atlas API-side belt. sdk.Protect(true)
	// above is the Pulumi-side braces. The former refuses the terminate API
	// call regardless of what the IaC tool wants to do; the latter makes the
	// IaC tool refuse to even try. Both wired to the same `deletionProtection`
	// config field for operator ergonomics. After the 2026-10-31 cascade
	// delete that destroyed project 697a574e90d5387e4a889f5f, this is required.
	cluster, err := mongodbatlas.NewCluster(ctx, fmt.Sprintf("%s-cluster", clusterName), &mongodbatlas.ClusterArgs{
		Name:                         sdk.StringPtr(clusterName),
		ProviderRegionName:           sdk.StringPtr(atlasCfg.Region),
		ProviderInstanceSizeName:     sdk.String(atlasCfg.InstanceSize),
		ClusterType:                  sdk.StringPtr("REPLICASET"),
		ProjectId:                    projectId,
		BackingProviderName:          sdk.StringPtrFromPtr(lo.If(isSharedInstanceSize, &atlasCfg.CloudProvider).Else(nil)),
		ProviderName:                 sdk.String(lo.If(isSharedInstanceSize, "TENANT").Else(atlasCfg.CloudProvider)),
		DiskSizeGb:                   sdk.Float64PtrFromPtr(atlasCfg.DiskSizeGB),
		CloudBackup:                  sdk.BoolPtr(lo.If(atlasCfg.Backup != nil, true).Else(false)),
		NumShards:                    sdk.IntPtrFromPtr(atlasCfg.NumShards),
		TerminationProtectionEnabled: sdk.BoolPtr(atlasCfg.DeletionProtection),
	}, opts...)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create mongodb cluster for stack %q", stack.Name)
	}
	out.Cluster = cluster

	if atlasCfg.Backup != nil {
		// Configure backup schedule - support both basic and advanced configurations
		backupArgs := &mongodbatlas.CloudBackupScheduleArgs{
			ProjectId:       projectId,
			ClusterName:     cluster.Name,
			UpdateSnapshots: sdk.Bool(true),
		}

		// Note: Point-in-Time Recovery is handled at the cluster level via PitEnabled field
		// PITR configuration should be set when creating the cluster itself, not in backup schedule

		var err error
		if atlasCfg.Backup.Advanced != nil {
			// Use advanced backup configuration
			err = configureAdvancedBackup(backupArgs, atlasCfg.Backup.Advanced)
		} else if atlasCfg.Backup.Every != "" && atlasCfg.Backup.Retention != "" {
			// Use legacy basic configuration for backwards compatibility
			err = configureBasicBackup(backupArgs, atlasCfg.Backup.Every, atlasCfg.Backup.Retention)
		} else {
			return nil, errors.New("backup configuration must specify either 'advanced' schedules or basic 'every'/'retention' values")
		}

		if err != nil {
			return nil, errors.Wrapf(err, "failed to configure backup schedules")
		}

		_, err = mongodbatlas.NewCloudBackupSchedule(ctx, fmt.Sprintf("%s-backups-schedule", clusterName), backupArgs, opts...)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to create mongodb backups schedule for stack %q", stack.Name)
		}
	}

	networkConfig := atlasCfg.NetworkConfig
	if networkConfig != nil {
		if networkConfig.PrivateLinkEndpoint != nil {
			// sc-plugin-mongodb v1 does not support PrivateLink. The in-tree
			// driver reached into pulumi-aws for SecurityGroup + VpcEndpoint;
			// moving that into the plugin would re-import the 200 MB of AWS
			// SDK the plugin split exists to remove. The Phase 1e PeerInvoke
			// path (SCCore + BrokerAware) is the clean way to broker those
			// AWS resources from sc-plugin-aws without a Go-module
			// dependency — follow-up slice. Until then, this is a refusal.
			return nil, errors.Errorf(
				"PrivateLink for cluster %q in stack %q in %q is not yet supported by sc-plugin-mongodb — "+
					"the follow-up slice adds it via PeerInvoke to sc-plugin-aws. For now, use `networkConfig.allowCidrs` "+
					"or `networkConfig.allowAllIps`.",
				clusterName, input.StackParams.StackName, input.StackParams.Environment,
			)
		} else if networkConfig.AllowCidrs != nil {
			for _, cidrBlock := range lo.FromPtr(networkConfig.AllowCidrs) {
				params.Log.Info(ctx.Context(), "configure MongoDB Atlas to allow cidr block %q for cluster %q in stack %q in %q",
					cidrBlock, clusterName, input.StackParams.StackName, input.StackParams.Environment)
				_, err := mongodbatlas.NewProjectIpAccessList(ctx, fmt.Sprintf("%s-cidr-block-%s", clusterName, cidrBlock), &mongodbatlas.ProjectIpAccessListArgs{
					CidrBlock: sdk.StringPtr(cidrBlock),
					Comment:   sdk.Sprintf("Allow cidr %s explicitly", cidrBlock),
					ProjectId: projectId,
				}, opts...)
				if err != nil {
					return nil, errors.Wrapf(err, "failed to create mongodb cidr block %q stack %q", cidrBlock, stack.Name)
				}
			}
		} else {
			return nil, errors.Errorf("network configuration for MongoDB Atlas cluster %q is provided but not supported", clusterName)
		}
	}

	// if public access was requested or network config wasn't provided
	if networkConfig == nil || lo.FromPtr(networkConfig.AllowAllIps) {
		params.Log.Info(ctx.Context(), "configure MongoDB Atlas ip access list for cluster %q in stack %q in %q",
			clusterName, input.StackParams.StackName, input.StackParams.Environment)
		_, err := mongodbatlas.NewProjectIpAccessList(ctx, fmt.Sprintf("%s-ip-access-list", clusterName), &mongodbatlas.ProjectIpAccessListArgs{
			CidrBlock: sdk.StringPtr("0.0.0.0/0"),
			Comment:   sdk.StringPtr("Allow all access to the cluster (TODO: restrict to our cluster only)"),
			ProjectId: projectId,
		}, opts...)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to create mongodb ip access list for stack %q", stack.Name)
		}
	} else if networkConfig != nil && len(lo.FromPtr(networkConfig.AllowCidrs)) > 0 {
		for _, cidr := range lo.FromPtr(networkConfig.AllowCidrs) {
			params.Log.Info(ctx.Context(), "configure MongoDB Atlas access cidr %q for cluster %q in stack %q in %q",
				cidr, clusterName, input.StackParams.StackName, input.StackParams.Environment)
			_, err := mongodbatlas.NewProjectIpAccessList(ctx, fmt.Sprintf("%s-ip-access-cidr-%q", clusterName, cidr), &mongodbatlas.ProjectIpAccessListArgs{
				CidrBlock: sdk.StringPtr(cidr),
				Comment:   sdk.StringPtr(fmt.Sprintf("Allow access to the cluster from %q", cidr)),
				ProjectId: projectId,
			}, opts...)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to create mongodb cidr access from %q for cluster %q", cidr, clusterName)
			}
		}
	}

	ctx.Export(toClusterIdExport(clusterName), cluster.ClusterId)

	ctx.Export(toMongoUriExport(clusterName), cluster.MongoUri)
	// mongodb v1 has no PrivateLink path (above refusal), so the standard
	// MongoUriWithOptions is always what consumers want. The PrivateLink
	// follow-up re-adds the LookupCluster branch here.
	ctx.Export(toMongoUriWithOptionsExport(clusterName), cluster.MongoUriWithOptions)

	usersOutput := sdk.All(projectId).ApplyT(func(args []any) any {
		return createDatabaseUsers(ctx, cluster, atlasCfg, params)
	})
	ctx.Export(fmt.Sprintf("%s-users", projectName), usersOutput)

	out.DbUsers = usersOutput

	return &api.ResourceOutput{Ref: out}, nil
}

func originMainUsesResourceContext(ctx *sdk.Context, params appendParams) error {
	// set both dbname and user name to stack name
	dbName := params.stack.Name
	userName := params.stack.Name

	dbUser, err := createDatabaseUser(ctx, dbUserInput{
		clusterName: params.clusterName,
		projectId:   params.projectId,
		dbUri:       params.mongoUri,
		username:    params.stack.Name,
		roles: []dbRole{
			{
				dbName: dbName,
				role:   "dbAdmin",
			},
			{
				dbName: dbName,
				role:   "readWrite",
			},
			{
				dbName: "local",
				role:   "read",
			},
		},
		suffix: params.suffix,
	}, params.provisionParams)
	if err != nil {
		return errors.Wrapf(err, "failed to create service user for database %q", dbName)
	}
	if dbUser != nil {
		ctx.Export(fmt.Sprintf("%s%s-service-user", params.clusterName, params.suffix), dbUser)

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

			// oplog uri is necessary for apps that would like to read mongo's oplog
			oplogMongoUri := AppendUserPasswordAndDBToMongoUri(params.mongoUri, userName, dbUser.Password, "local")

			params.collector.AddSecretEnvVariableIfNotExist(util.ToEnvVariableName("MONGO_URI"), mongoUri,
				params.input.Descriptor.Type, params.input.Descriptor.Name, params.provisionParams.ParentStack.StackName)

			params.collector.AddResourceTplExtension(params.input.Descriptor.Name, map[string]string{
				"uri":      mongoUri,
				"dbName":   dbName,
				"password": dbUser.Password,
				"user":     userName,
				"oplogUri": oplogMongoUri,
			})

			return nil, nil
		}))
	}

	return nil
}

func originMainDependsOnResourceContext(ctx *sdk.Context, params appendParams) error {
	ownerStackName := pApi.CollapseStackReference(params.dependency.Owner)
	userName := fmt.Sprintf("%s--%s%s", params.stack.Name, params.dependency.Name, params.suffix)
	dbEnv := lo.If(params.provisionParams.ParentStack.ResourceEnv != "", params.provisionParams.ParentStack.ResourceEnv).Else(params.input.StackParams.Environment)
	dbName := pApi.StackNameInEnv(ownerStackName, dbEnv)

	dbUser, err := createDatabaseUser(ctx, dbUserInput{
		clusterName: params.clusterName,
		projectId:   params.projectId,
		dbUri:       params.mongoUri,
		username:    userName,
		roles: []dbRole{
			{
				dbName: dbName,
				role:   "dbAdmin",
			},
			{
				dbName: dbName,
				role:   "readWrite",
			},
			{
				dbName: "local",
				role:   "read",
			},
		},
		suffix: params.suffix,
	}, params.provisionParams)
	if err != nil {
		return errors.Wrapf(err, "failed to create service user for database %q", dbName)
	}
	if dbUser != nil {
		ctx.Export(fmt.Sprintf("%s--to--%s--%s%s", params.clusterName, ownerStackName, params.dependency.Resource, params.suffix), dbUser)

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

			// oplog uri is necessary for apps that would like to read mongo's oplog
			oplogMongoUri := AppendUserPasswordAndDBToMongoUri(params.mongoUri, userName, dbUser.Password, "local")

			params.collector.AddSecretEnvVariableIfNotExist(util.ToEnvVariableName(fmt.Sprintf("MONGO_DEP_%s_URI", ownerStackName)), mongoUri,
				params.input.Descriptor.Type, params.input.Descriptor.Name, params.provisionParams.ParentStack.StackName)

			params.collector.AddDependencyTplExtension(params.dependency.Name, params.input.Descriptor.Name, map[string]string{
				"uri":      mongoUri,
				"dbName":   dbName,
				"password": dbUser.Password,
				"user":     userName,
				"oplogUri": oplogMongoUri,
			})

			return nil, nil
		}))
	}

	return nil
}
