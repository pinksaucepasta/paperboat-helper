# paperboat-helper

The remote Paperboat runtime for hosted environments and approved BYOD machines. It owns
the outbound tunnel connector, PTYs, durable terminal sessions, Herdr process launch,
image staging, preview targets, activity, health, and assigned configuration sync.

Hosted deployments additionally use the helper for managed environment lifecycle hooks.
The CLI and web applications never replace or directly administer this runtime.

## Development

Go `1.25.7` is required. See [AGENTS.md](AGENTS.md) for repository ownership and
engineering requirements.

```sh
make check
```

`paperboat-helper bootstrap` is the dashboard-started BYOD path. It consumes single-use
installation material, verifies the signed helper artifact, installs the user service,
and waits for authenticated readiness. A missing machine name is prompted interactively;
the workspace is always the invoking user's canonical home directory and cannot be
overridden.

Development deployments can produce signed helper-artifact metadata with
`go run ./cmd/paperboat-helper-artifact`. The command requires an owner-only private-key
file and writes only the manifest and public key; production signing and release channels
remain separate operations.

## License

MIT. See [LICENSE](LICENSE).
