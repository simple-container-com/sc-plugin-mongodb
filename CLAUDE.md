# sc-plugin-mongodb

MongoDB Atlas plugin for SC. Extracted from `pkg/clouds/{mongodb,pulumi/mongodb}`
in `simple-container-com/api` per Phase 3+. Full README in README.md.

## Load-bearing notes for anyone editing this

- **`/api` is a separate Go module.** SC core imports it to parse
  `type: mongodb-atlas` in a server.yaml. Nothing in `/api` imports
  pulumi-mongodbatlas or anything else heavy — keep it that way.
- **`init.go` has two registrations.** `RegisterProviderConfig` wires
  the YAML reader; `RegisterPluginRequirement` is what SC core's
  parse-time fool-protection reads to auto-inject `plugins.mongodb:`.
  Both must list every type token this plugin owns.
- **`plugin/internal/` is lifted from api verbatim** with two edits:
  package import path changed to `sc-plugin-mongodb/api`, and the
  PrivateLink block in `cluster.go` replaced with a refusal. Keeping
  the lift pattern makes the follow-up PR (drop in-tree mongodb from
  api) a straight deletion.
- **`cluster.go` deliberately refuses `networkConfig.privateLinkEndpoint`.**
  In-tree, that block reached for pulumi-aws (SecurityGroup + VpcEndpoint).
  Keeping that would re-import ~200 MB of AWS SDK and defeat the
  extraction. Phase 1e PeerInvoke D8 is the path forward: this plugin
  will call `sc-plugin-aws.Provision` via `plugin.PeerClient` once
  that peer exists. For now, operators who need PrivateLink stay on
  the pre-plugin SC release.
- **`pre_destroy_plugin.go` adapts the Phase 1e v2 `PreDestroyRequest`**
  (stack_outputs + client_descriptor_json + destroy_params_json) into
  the shape `DropDatabaseHook` expects (`api.Stack` + `api.DestroyParams`
  + outputs map). The hook body is lifted verbatim — only the
  stackSource.Outputs call is shimmed because `auto.Stack` can't be
  reconstructed on the plugin side without a Pulumi workspace handle.
- **`cmd/sc-plugin-mongodb/main.go` is thin on purpose.** Each RPC
  method unpacks the proto, calls the lifted internal function, and
  wraps the response. Keep it that way; new logic goes into `internal/`.
- **The `collector.go` adapter is in `cmd/`, not `internal/`,** so it
  can see the proto types without re-exporting. It implements
  `pApi.ComputeContextCollector` and captures mutations into the
  `scpluginv1.ComputeProcessResponse` the SC-core sidecar then applies
  back to the live core-side collector.

## Running tests

```
cd api && go test ./...
cd plugin && go test ./...
```

Both must stay green on `main`. The URN contract test from the echo
template was NOT ported — mongodb's resource names (project, cluster,
database users) are tested by `cluster_naming_test.go`, which is the
in-tree test ported along with the driver code.

## Release

Push to `main` → auto-release with CalVer `vYYYY.MM.DD-<short-sha>`.
Workflow carries the OOM pins (`GOMAXPROCS=1 GOMEMLIMIT=6GiB`,
`go build -p 1`) — don't drop them, pulumi-mongodbatlas v3 is a
~19 MB SDK and the pulumi-aws SDK adjacent to its toolchain is much
heavier.
