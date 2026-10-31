# sc-plugin-mongodb

The MongoDB Atlas plugin for Simple Container. Extracted from
`pkg/clouds/{mongodb,pulumi/mongodb}` in `simple-container-com/api` per
Phase 3+ of the plugin-architecture roadmap.

Operators who deploy a `type: mongodb-atlas` resource in a parent stack
need this plugin loaded; see the
[Upgrading to Plugins](https://github.com/simple-container-com/api/blob/main/docs/docs/guides/upgrading-to-plugins.md)
guide — SC's parse-time fool-protection auto-resolves the latest release
unless an operator pins a specific version via `plugins.mongodb.source.version`.

## Shipped scope (v1)

- Atlas Provider (`mongodbatlas:index/provider:Provider`).
- Cluster provisioning — project + cluster + optional backup schedule +
  ProjectIpAccessList for `networkConfig.allowCidrs` /
  `networkConfig.allowAllIps`.
- Adopt an existing Atlas cluster via `adopt: true` + `clusterName`.
- ClusterComputeProcessor — a dependent compose/k3s stack picks up
  `MONGO_URI`, cluster id, project id as env vars + template extensions.
- PreDestroy — reads `cloudExtras.mongoDbDestroyDatabase` off the
  deploying stack's client descriptor and drops per-service databases
  enumerated from stack outputs.

## Deferred (follow-up)

- **PrivateLink** + AWS VPC endpoint + MongoDB private link endpoint
  service. The in-tree driver reached into pulumi-aws for SecurityGroup
  + VpcEndpoint; porting that into this plugin would re-import ~200 MB
  of AWS SDK and defeat the extraction. Lands via the Phase 1e PeerInvoke
  pattern (D8 — see 06-decisions.md) once sc-plugin-aws exists:
  sc-plugin-mongodb will `PeerInvoke` into sc-plugin-aws's `Provision`
  for the AWS SecurityGroup + VpcEndpoint resources.

## Repo layout

```
sc-plugin-mongodb/
├── api/                        ← config tier. Own Go module; SC core imports it.
│   ├── init.go                 ← RegisterProviderConfig + RegisterPluginRequirement
│   └── mongodb.go              ← AtlasConfig + nested shapes (backup, networkConfig)
│
├── plugin/                     ← provisioning tier. The go-plugin binary.
│   ├── cmd/sc-plugin-mongodb/
│   │   ├── main.go             ← Schema + per-RPC wiring
│   │   └── collector.go        ← ComputeContextCollector adapter for ComputeProcess v2
│   └── internal/
│       ├── provider.go         ← Atlas Provider
│       ├── cluster.go          ← Cluster + AllowCidrs/AllowAllIps + db users (strips PrivateLink)
│       ├── adopt_cluster.go    ← sdk.Import flow for existing clusters
│       ├── compute_proc.go     ← ClusterComputeProcessor
│       ├── pre_destroy.go      ← DropDatabaseHook (lifted verbatim)
│       ├── pre_destroy_plugin.go ← v2 PreDestroyRequest unwrapper → DropDatabaseHook
│       ├── drop_db.go          ← mongo-driver DB drop
│       └── util.go             ← MongoURI user+password helpers
│
└── .github/workflows/release.yml ← CalVer, three-platform build, OOM-safe pins
```

## Prior art

Lifted from `simple-container-com/sc-plugin-echo` (the template every
plugin clones from). See the authoring guide at
`api/docs/docs/guides/plugin-authoring.md` for the walkthrough.

## License

MIT. See [LICENSE](LICENSE).

## Optional Atlas hardening (S-04)

Upgrading with unchanged config preserves the original behavior. Missing
`networkConfig` still creates the same `<cluster>-ip-access-list` resource with
`0.0.0.0/0`, including its original properties. Generated service users keep their
names, passwords, `dbAdmin`/`readWrite` on their database, `read` on `local`, and
the `oplogUri` template key. No flags are required for existing stacks.

The following explicit opt-ins are **recommended for new stacks**. Set them in
the `mongodb-atlas` resource config in `server.yaml`:

```yaml
disableDefaultPublicAccess: true
leastPrivilegeUsers: true
networkConfig:
  allowCidrs: [203.0.113.10/32] # actual application/NAT egress IPs
oplogReaders:
  - meeting-app/staging
```

| Flag | Opt-in behavior |
| --- | --- |
| `disableDefaultPublicAccess` | Stops creating the implicit open access-list entry when `networkConfig` is absent. A top-level scalar flag works with older scalar-only transports. Explicit `networkConfig.allowAllIps: true` still grants public access. |
| `leastPrivilegeUsers` | Removes `read@local` and `oplogUri` from ordinary users. Only consumers explicitly listed in `oplogReaders` receive oplog credentials and the template key. |

Enabling either flag on an existing stack is an intentional migration. Arrange
application egress access before removing a managed public entry. If that entry
is protected by `deletionProtection`, unprotect only its access-list URN before
removal; keep the project and cluster protected. Access lists are project-wide;
independently managed entries remain unchanged. Explicit CIDR configuration and
PrivateLink's existing unsupported status are unchanged.

With `leastPrivilegeUsers: true`, each `oplogReaders` entry must match exactly
`stack-name/consuming-environment` for both `uses` and `dependencies`. Wildcards
and bare names grant nothing. An allowed reader gets a separate environment
specific `sc-oplog-<hash>` principal, and MONGO_USER/PASSWORD/URI and template
values switch together. The legacy user and its cleanup export remain, with
reduced roles. Oplog access exposes writes from every database in the cluster;
use a dedicated cluster when that trust boundary is required. Remove the reader
and redeploy that consuming stack to revoke its separate principal. With the
flag omitted, `oplogReaders` does not alter legacy usernames or permissions.

Structured config crosses older scalar-only plugin transports through the JSON
fallback emitted by this plugin's `/api` module. Upgrade both the config module
used by SC and the plugin binary to use CIDR and reader lists reliably. The
legacy missing-network default requires no nested-map transport or JSON payload.
