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

## License

MIT. See [LICENSE](LICENSE).
