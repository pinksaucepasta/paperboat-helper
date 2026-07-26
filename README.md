# paperboat-helper

The remote Paperboat runtime for hosted environments and approved user machines. It owns
the outbound tunnel connector, PTYs, durable terminal sessions, Herdr process launch,
image staging, preview targets, activity, health, and assigned configuration sync.

Hosted deployments additionally use the helper for managed environment lifecycle hooks.
The CLI and web applications never replace or directly administer this runtime.

## Development

Go `1.25.7` is required. See [AGENTS.md](AGENTS.md) for repository ownership and
engineering requirements.

User-machine bootstrap, security, recovery, updates, availability, and removal are covered
in [docs/byod-runtime.md](docs/byod-runtime.md).

```sh
make check
```

`pbh bootstrap` is the dashboard-started BYOD path. It consumes single-use
installation material, verifies the signed helper artifact, requests administrator approval
for a boot-level system service running as the enrolled user, and waits for authenticated
readiness. A minimal root-owned `paperboat-host-service` applies availability policy and
activates paired signed updates; it cannot access PTYs, credentials, workspaces, or user
state. Prevent-sleep is enabled by default and can be changed to `allow_sleep` from the
dashboard or `pb`. A missing user-machine name is prompted interactively;
the workspace is always the invoking user's canonical home directory and cannot be
overridden.

Development deployments can produce signed helper-artifact metadata with
`go run ./tools/artifact`. The command requires an owner-only private-key
file and writes only the manifest and public key; production signing and release channels
remain separate operations.

## Release

`YYYY.MM.DD.X` tags build Darwin and Linux worker and `paperboat-host-service` binaries for
amd64 and arm64, include both signed payloads in the server-consumable artifact manifest,
generate checksums and an SPDX SBOM, attest the binaries, and publish a GitHub release.
Configure the repository secret `HELPER_ARTIFACT_SIGNING_KEY` with the
base64-encoded Ed25519 release seed. The hosted runtime image is published separately as
`ghcr.io/pinksaucepasta/paperboat-helper-hosted`.
Use `tools/release-version.sh next` to generate the next tag; tags have no `v` prefix.

## Removal

`pbh service uninstall` requests administrator approval, restores the original power
configuration, removes both system services and root-owned binaries, and retains the
enrolled user's durable state. State is purged only through an explicit separate action.

## License

MIT. See [LICENSE](LICENSE).
