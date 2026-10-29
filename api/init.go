// SPDX-License-Identifier: MIT
// Copyright (c) Simple Container

// Package mongodb is the config tier of sc-plugin-mongodb. It owns
// `mongodb-atlas` — one provider type that doubles as the resource
// type (Atlas provisions a cluster, which the compose/k3s stacks
// then consume via the mongo URI it exports).
//
// Extracted from pkg/clouds/mongodb/ in api as part of Phase 3+ per
// forge/docs/design/2026-10-24/sc-plugin-architecture/
// 07-plugin-first-for-new-clouds.md §8.
package mongodb

import (
	"github.com/simple-container-com/api/pkg/api"
)

// ProviderType is the top-level discriminator every mongodb config
// reports from ProviderType(). It doubles as the resource type —
// Atlas has one shape: a cluster.
const ProviderType = "mongodb-atlas"

func init() {
	api.RegisterProviderConfig(api.ConfigRegisterMap{
		ResourceTypeMongodbAtlas: ReadAtlasConfig,
	})

	// Fool-protection: SC core's parse-time validator auto-injects
	// `plugins.mongodb:` with the latest release from GitHub when it
	// finds `type: mongodb-atlas` in a server.yaml without a `plugins:`
	// block. See api/pkg/api/plugin_requirements.go + 04-no-destroy-
	// invariant.md Addendum J.
	api.RegisterPluginRequirement(api.PluginRequirement{
		PluginName: "mongodb",
		OwnedTypes: []string{
			ProviderType,
			ResourceTypeMongodbAtlas,
		},
		DefaultSource: api.PluginSource{
			Type:       "github-release",
			Repository: "simple-container-com/sc-plugin-mongodb",
		},
	})
}
