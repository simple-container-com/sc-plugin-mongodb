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

## Atlas least privilege (S-04)

A consuming stack's generated user keeps `dbAdmin` and `readWrite` on its own
database. It no longer receives `read` on `local` or an `oplogUri` template
extension by default. Applications that require the oplog must be explicitly
listed in the Atlas resource config in `server.yaml` (works for both `uses` and `dependencies`):

```yaml
# Inside the mongodb-atlas resource config:
oplogReaders:
  - meeting-app/staging
networkConfig:
  allowCidrs: [203.0.113.10/32] # replace with actual application/NAT egress IPs
```

Each reader is exactly `stack-name/consuming-environment`, not the parent resource
environment; wildcards and bare stack names grant nothing. Other stacks and
other environments do not gain oplog access. This is resource-side authorization
because the compute-process plugin receives the resource config, not the
consuming stack's client descriptor.

**This grants visibility into writes from every database in the cluster,
including other environments.** Use only with an intentional trust boundary;
prefer a dedicated cluster for applications that need the oplog.
Existing oplog-dependent stacks must be listed before upgrading. The existing
user retains its name/credentials and loses `read@local` in place. An explicit
reader additionally gets a separate environment-specific `sc-oplog-<hash>`
principal; its MONGO_USER/PASSWORD/URI and template extensions switch together
to that principal. The original service-user export is retained for database
cleanup. Environments sharing a legacy username never share the privileged
principal. Revoking access requires removing the reader and redeploying that
consuming stack so its privileged user is removed and credentials switch back.

Public access now defaults to deny: omitted `networkConfig`, `{}`, and
`allowAllIps: false` create no open access-list entry. Configure the resource in
`server.yaml` with your applications' outbound CIDRs before upgrading:

```yaml
# Inside the mongodb-atlas resource config:
networkConfig:
  allowCidrs: [203.0.113.10/32] # replace with actual application/NAT egress IPs
```

`networkConfig: {allowAllIps: true}` explicitly preserves the old open behavior
when migration cannot be completed first; it allows the entire IPv4 internet
and is discouraged. Existing managed implicit `0.0.0.0/0` entries will be removed
on the next provision unless this opt-in is set. If `deletionProtection` was
previously enabled, the old public access-list resource is protected too:
unprotect **only that access-list URN in Pulumi state** before applying the
removal. Keep the project and cluster protected. Alternatively, temporarily set
`allowAllIps: true` while arranging this state-only migration; leaving it set
retains public access. Access lists are project-wide; entries managed outside this stack (including adopted project entries) remain
unchanged, so this does not remove independently managed public access.

The plugin already supports CIDRs, so deny by default is feasible and no new
network configuration field is needed. AWS VPC peering/PrivateLink provisioning
is not added here: PrivateLink remains explicitly unsupported pending the
cross-plugin PeerInvoke integration described above. CIDR allowlisting is the
supported network boundary in this release.

Structured config crosses older scalar-only plugin transports through a JSON
fallback emitted by this plugin's `/api` config module and read by the plugin
binary. Upgrade both the config module used by SC and the plugin binary together;
rebuild SC with this `/api` revision when its embedded module predates this change.
This prevents CIDR and reader lists from being silently dropped. No core protocol
change is required.
