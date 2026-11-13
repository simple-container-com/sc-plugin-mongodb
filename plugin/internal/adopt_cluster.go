// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

package mongodb

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/pulumi/pulumi-mongodbatlas/sdk/v3/go/mongodbatlas"
	sdk "github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/samber/lo"
	"github.com/simple-container-com/api/pkg/api"
	pApi "github.com/simple-container-com/api/pkg/clouds/pulumi/api"

	"github.com/simple-container-com/sc-plugin-mongodb/api"
)

// AdoptCluster imports an existing MongoDB Atlas cluster into Pulumi state without modifying it
func AdoptCluster(ctx *sdk.Context, stack api.Stack, input api.ResourceInput, params pApi.ProvisionParams) (*api.ResourceOutput, error) {
	if input.Descriptor.Type != mongodb.ResourceTypeMongodbAtlas {
		return nil, errors.Errorf("unsupported mongodb-atlas type %q", input.Descriptor.Type)
	}

	atlasCfg, ok := input.Descriptor.Config.Config.(*mongodb.AtlasConfig)
	if !ok {
		return nil, errors.Errorf("failed to convert mongodb atlas config for %q", input.Descriptor.Type)
	}

	if !atlasCfg.Adopt {
		return nil, errors.Errorf("adopt flag not set for resource %q", input.Descriptor.Name)
	}

	if atlasCfg.ClusterName == "" {
		return nil, errors.Errorf("clusterName is required when adopt=true for resource %q", input.Descriptor.Name)
	}

	// Use identical naming functions as provisioning to ensure export compatibility.
	// `clusterName` is the Pulumi RESOURCE NAME (used in URN) — stable across
	// a config change that adds `clusterName:`. `exportClusterName` is the
	// Atlas-visible cluster name (= `atlasCfg.ClusterName` for adoption) and
	// is what export keys are prefixed with so downstream `uses: [mongodb]`
	// lookups find them under the actual Atlas cluster name.
	// See cluster.go for the live-caught incident this split guards against.
	projectName := toProjectName(stack.Name, input)
	clusterName := toClusterName(stack.Name, input)
	exportClusterName := atlasCfg.ClusterName

	// CRITICAL SAFETY WARNING for production environments
	// Use flexible environment detection instead of hardcoded names
	pApi.LogAdoptionWarnings(ctx, input, params, "MongoDB cluster", atlasCfg.ClusterName)

	params.Log.Info(ctx.Context(), "adopting existing MongoDB Atlas cluster %q for project %q", atlasCfg.ClusterName, atlasCfg.ProjectId)

	// First, lookup the existing cluster to get its current configuration
	params.Log.Info(ctx.Context(), "fetching existing cluster details for %q", atlasCfg.ClusterName)
	existingCluster, err := mongodbatlas.LookupCluster(ctx, &mongodbatlas.LookupClusterArgs{
		ProjectId: atlasCfg.ProjectId,
		Name:      atlasCfg.ClusterName,
	}, sdk.Provider(params.Provider))
	if err != nil {
		return nil, errors.Wrapf(err, "failed to lookup existing MongoDB Atlas cluster %q", atlasCfg.ClusterName)
	}

	// Use the existing cluster's configuration for the import, but allow overrides from config
	instanceSize := existingCluster.ProviderInstanceSizeName
	if atlasCfg.InstanceSize != "" {
		instanceSize = atlasCfg.InstanceSize
		params.Log.Info(ctx.Context(), "overriding instance size with config value: %q", instanceSize)
	}

	region := existingCluster.ProviderRegionName
	if atlasCfg.Region != "" {
		region = atlasCfg.Region
		params.Log.Info(ctx.Context(), "overriding region with config value: %q", region)
	}

	clusterType := existingCluster.ClusterType
	providerName := existingCluster.ProviderName
	backingProviderName := existingCluster.BackingProviderName

	// Override provider settings if cloud provider is explicitly specified
	if atlasCfg.CloudProvider != "" {
		params.Log.Info(ctx.Context(), "overriding cloud provider with config value: %q", atlasCfg.CloudProvider)
		// Apply the same shared instance size logic as regular provisioning
		sharedInstanceSizes := []string{"M0", "M2", "M5"}
		_, isSharedInstanceSize := lo.Find(sharedInstanceSizes, func(size string) bool {
			return size == instanceSize
		})

		if isSharedInstanceSize {
			providerName = "TENANT"
			backingProviderName = atlasCfg.CloudProvider
		} else {
			providerName = atlasCfg.CloudProvider
			backingProviderName = ""
		}
	}

	params.Log.Info(ctx.Context(), "found existing cluster with instance size %q, region %q, provider %q",
		instanceSize, region, providerName)

	// Import existing cluster into Pulumi state
	// The cluster resource ID in MongoDB Atlas is: {project_id}-{cluster_name}
	clusterResourceId := fmt.Sprintf("%s-%s", atlasCfg.ProjectId, atlasCfg.ClusterName)

	// Use standardized adoption protection options
	adoptionOpts := pApi.AdoptionProtectionOptions([]string{
		// Core cluster configuration that might drift
		"diskSizeGb", "numShards", "cloudBackup",
		// Provider-specific settings that might vary
		"providerAutoScalingComputeEnabled", "providerAutoScalingComputeScaleDownEnabled",
		"providerDiskIops", "providerEncryptEbsVolume", "providerVolumeType",
		// Network and security settings
		"mongoDbMajorVersion", "pitEnabled", "rootCertType",
		// Advanced settings that might be managed outside of Pulumi
		"advancedConfiguration", "labels", "tags",
		// Backup and maintenance settings
		"backupEnabled", "mongoDbVersion", "paused",
	})

	opts := append([]sdk.ResourceOption{
		sdk.Provider(params.Provider),
		// Import the existing cluster without creating or modifying it
		sdk.Import(sdk.ID(clusterResourceId)),
	}, adoptionOpts...)

	cluster, err := mongodbatlas.NewCluster(ctx, clusterName, &mongodbatlas.ClusterArgs{
		ProjectId: sdk.String(atlasCfg.ProjectId),
		Name:      sdk.String(atlasCfg.ClusterName),
		// Use the existing cluster's configuration for import
		ProviderInstanceSizeName: sdk.String(instanceSize),
		ProviderRegionName:       sdk.StringPtr(region),
		ClusterType:              sdk.StringPtr(clusterType),
		ProviderName:             sdk.String(providerName),
		BackingProviderName:      sdk.StringPtrFromPtr(lo.If(backingProviderName != "", &backingProviderName).Else(nil)),
		// Note: Using actual cluster configuration from MongoDB Atlas
		// This ensures the import matches the existing cluster exactly
	}, opts...)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to import MongoDB Atlas cluster %q", atlasCfg.ClusterName)
	}

	// Export the same keys as the provisioning function to ensure compute processor compatibility.
	// Prefix with `exportClusterName` (= atlasCfg.ClusterName) so downstream
	// `uses: [mongodb]` consumers find the exports under the actual Atlas
	// cluster name — not the Pulumi-truncated local resource name.
	ctx.Export(toProjectIdExport(projectName), sdk.String(atlasCfg.ProjectId))
	ctx.Export(toClusterIdExport(exportClusterName), cluster.ClusterId)
	ctx.Export(toMongoUriExport(exportClusterName), cluster.MongoUri)
	ctx.Export(toMongoUriWithOptionsExport(exportClusterName), cluster.MongoUriWithOptions)

	// Slice 1d.3 of sc-components-platform — mirror the provisioning
	// path's per-cluster `-database` export. Operator-declared opt-in;
	// sibling matchers (sc-component-backup's MatchConsumerToDatabase)
	// read it via the `-database` suffix.
	if atlasCfg.Database != "" {
		ctx.Export(toDatabaseExport(exportClusterName), sdk.String(atlasCfg.Database))
	}

	// Per-service users are the compute processor's job; the cluster-wide
	// `admins` (readWriteAnyDatabase) and `developers` (readAnyDatabase) are
	// this function's, exactly as on the provisioning path. Adoption used to
	// skip them, which silently dropped every operator user the day a stack
	// switched to `adopt: true` — live-caught on forge `infra` after the
	// 2026-10-31 project cascade was restored through adoption: the
	// `-vitaly-user`/`-smecsia-user` DatabaseUsers left state and `admins`
	// became a no-op. With no admins/developers configured the export stays
	// the empty list it always was, so stacks that set neither see no diff.
	usersOutput := sdk.ToOutput([]interface{}{})
	if len(atlasCfg.Admins)+len(atlasCfg.Developers) > 0 {
		users, ok := createDatabaseUsers(ctx, cluster, atlasCfg, params).(sdk.Output)
		if !ok {
			return nil, errors.Errorf("failed to create database users for adopted cluster %q", atlasCfg.ClusterName)
		}
		usersOutput = users
	}
	ctx.Export(fmt.Sprintf("%s-users", projectName), usersOutput)
	// Same per-user scalar exports as the provisioning path — the map export
	// above is dropped by core's harvest, so without these an adopted
	// cluster's operator credentials are unreachable. No-op when neither
	// `admins` nor `developers` is configured.
	exportDbUserCredentials(ctx, projectName, atlasCfg, usersOutput)

	out := &ClusterOutput{
		DbUsers:                    usersOutput,
		Cluster:                    cluster,
		Project:                    nil, // We don't manage the project for adopted clusters
		PrivateLinkEndpointService: nil, // We don't manage private link for adopted clusters
	}

	params.Log.Info(ctx.Context(), "successfully adopted MongoDB Atlas cluster %q", atlasCfg.ClusterName)

	return &api.ResourceOutput{Ref: out}, nil
}
