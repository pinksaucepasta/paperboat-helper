# Hosted Project Image

The managed image runs `pbh run`. The helper owns workspace boot, durable
sessions, the embedded frp connector, readiness, and bounded shutdown.

Build from the workspace root through:

```sh
paperboat-helper/deploy/hosted-image/build-image.sh registry.example/paperboat/hosted-image:YYYY.MM.DD.X
```

`PAPERBOAT_NODE_BASE_IMAGE` and `PAPERBOAT_GO_BASE_IMAGE` must be immutable
`name@sha256:<digest>` references. Clean `paperboat-helper` and `paperboat-server` revisions
are recorded as OCI labels. The image records hosted contract and protocol metadata.

## Boot Contract

The control plane supplies only non-secret project/repository/branch/preset intent, the
control URL, and the helper profile and state root in Machine configuration. The helper
authenticates with Fly OIDC, persists its key-bound renewable runtime identity below the
mounted volume, then fetches the selected setup revision and any short-lived private-source
read credential through its signed helper channel. Config assignment, policy, age key
versions, and short-lived config-repository access are fetched only after eligibility.
No enrollment credential, provider token, age identity, or setup script is stored in Fly
Machine configuration before the helper:

Config-sync rollout is server-gated with
`PAPERBOAT_CONFIG_SYNC_MODE=disabled|read_only|leased_writes`,
`PAPERBOAT_CONFIG_SYNC_BYOD_ENABLED`, and the optional
`PAPERBOAT_CONFIG_SYNC_ENVIRONMENT_ALLOWLIST`. The default is disabled. `read_only`
permits authorized restore, polling, and conflict observation while writer leases return a
typed refusal, so helpers retain local pending changes without publishing them.

1. Validate the volume, project identity, repository host/URL, branch, preset catalog, and
   all execution bounds.
2. Create or verify the durable workspace identity and clone/fetch the exact HTTPS origin.
3. Start the embedded, lease-fenced config engine when the assignment is eligible.
4. Apply catalog presets and the bounded setup revision.
5. Fetch control-plane JWKS, verify operation credentials, request connector admission,
   and start the embedded frp route.
6. Report ready only after the hosted lifecycle and edge connector are both ready.

Shutdown stops admission, drains the connector and sessions, performs the embedded
engine's bounded final flush, closes durable state, and exits within the Fly stop timeout. The
Docker healthcheck reads the helper `/healthz` response and requires liveness plus ready
`hosted_lifecycle` and `edge` capabilities.

The image contains Git, chezmoi, CA roots, Node/npm, Python/venv, the helper,
version-pinned catalog preset definitions, and shell tooling required by supported presets. It exposes no Fly service;
all terminal/upload/preview traffic traverses the assigned Paperboat edge route.

## Rollout And Rollback

Server catalogs must reference the image by immutable digest. Rollout metadata includes
the helper/server revisions, helper protocol, hosted image contract,
architecture, and both base-image digests. Rollback selects the previous compatible image
digest and preserves the mounted volume; it does not rewrite workspace identity or apply
pending project configuration silently.

Verify the retained rollback image before promotion:

```sh
deploy/hosted-image/tests/image-rollback-check.sh CURRENT_IMAGE ROLLBACK_IMAGE
```
