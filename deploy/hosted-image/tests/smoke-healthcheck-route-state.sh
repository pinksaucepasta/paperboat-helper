#!/usr/bin/env bash
set -Eeuo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

cat > "$tmp/curl" <<'EOF'
#!/usr/bin/env bash
cat "$PAPERBOAT_TEST_HEALTH_BODY"
EOF
chmod +x "$tmp/curl"

export PATH="$tmp:$PATH"
export PAPERBOAT_TEST_HEALTH_BODY="$tmp/health.json"

printf '%s' '{"live":true,"capabilities":{"hosted_lifecycle":{"state":"ready"},"edge":{"state":"ready"}}}' > "$PAPERBOAT_TEST_HEALTH_BODY"
"$root/bin/paperboat-healthcheck"

printf '%s' '{"live":true,"capabilities":{"hosted_lifecycle":{"state":"ready"},"edge":{"state":"unavailable"}}}' > "$PAPERBOAT_TEST_HEALTH_BODY"
if "$root/bin/paperboat-healthcheck"; then
  echo "healthcheck accepted unavailable connector" >&2
  exit 1
fi

printf '%s' '{"live":false,"capabilities":{"hosted_lifecycle":{"state":"ready"},"edge":{"state":"ready"}}}' > "$PAPERBOAT_TEST_HEALTH_BODY"
if "$root/bin/paperboat-healthcheck"; then
  echo "healthcheck accepted non-live helper" >&2
  exit 1
fi
