#!/usr/bin/env bash
set -Eeuo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
repository_root="$(cd "$root/../.." && pwd)"

test -x "$root/build-image.sh"
test -f "$root/tests/image-rollback-check.sh"
bash -n "$root/build-image.sh"
bash -n "$root/tests/image-rollback-check.sh"
bash -n "$root/bin/paperboat-healthcheck"
bash -n "$root/bin/paperboat-workspace-cd.sh"
for file in "$root"/presets.d/*.sh; do bash -n "$file"; done

# Preset installers must resolve immutable versions; runtime scripts may not
# silently consume npm latest, release stable, or floating installer scripts.
if grep -R -Eq 'releases/download/stable|aider\.chat/install\.sh|cursor\.com/install|antigravity\.google/cli/install\.sh' "$root/presets.d"; then
  printf 'preset installer is not pinned to an immutable artifact\n' >&2
  exit 1
fi
for package in \
  '@anthropic-ai/claude-code@2.1.215' '@openai/codex@0.144.6' \
  '@mariozechner/pi-coding-agent@0.73.1' 'opencode-ai@1.18.3' \
  '@sourcegraph/amp@0.0.1784551160-g777afc' '@charmland/crush@0.85.0'; do
  grep -R -q "npm install -g $package" "$root/presets.d" || { printf 'missing pinned npm package: %s\n' "$package" >&2; exit 1; }
done
for command in claude codex pi opencode amp crush; do
  grep -R -q "command -v $command" "$root/presets.d" || { printf 'preset does not verify installed command: %s\n' "$command" >&2; exit 1; }
done

grep -q 'pbh.*run' "$root/Dockerfile"
grep -Fq 'ENTRYPOINT ["/usr/bin/tini", "-s", "--"]' "$root/Dockerfile"
grep -q 'COPY go.mod go.sum' "$root/Dockerfile"
grep -q 'io.paperboat.image.contract="hosted-helper-v1"' "$root/Dockerfile"
grep -q 'io.paperboat.source.helper.revision' "$root/Dockerfile"
grep -q 'io.paperboat.herdr.version' "$root/Dockerfile"
grep -q 'CHEZMOI_VERSION=2.71.0' "$root/Dockerfile"
grep -q 'bc0fc02d4ba500f9cac2353a43e67fe036785ecca6eb55378e050fac3c103059' "$root/Dockerfile"
grep -q '544e0002de42806d1ab64ccdef3a7e7414f24717b0b6b022bc9e57d2eefd26a2' "$root/Dockerfile"
grep -q 'PAPERBOAT_HELPER_REVISION' "$root/build-image.sh"
grep -q 'PAPERBOAT_NODE_BASE_IMAGE' "$root/build-image.sh"
grep -q 'PAPERBOAT_GO_BASE_IMAGE' "$root/build-image.sh"
grep -q 'PAPERBOAT_PROJECT_VM_PUSH' "$root/build-image.sh"
grep -q 'docker buildx build --push' "$root/build-image.sh"
grep -q 'multi-platform builds require' "$root/build-image.sh"
grep -q 'PAPERBOAT_HELPER_PROFILE=hosted' "$root/Dockerfile"
grep -q '"hosted_lifecycle"' "$root/bin/paperboat-healthcheck"
grep -q '"edge"' "$root/bin/paperboat-healthcheck"

if grep -R -q 'PAPERBOAT_CLASSIFIER_API_KEY' "$root/Dockerfile" "$root/bin"; then
  printf 'classifier provider credentials must not be present in the VM image\n' >&2
  exit 1
fi

"$root/tests/smoke-healthcheck-route-state.sh"
(cd "$repository_root" && go test ./internal/hosted ./internal/runtime)
grep -q 'deploy/hosted-image/presets.d/' "$root/Dockerfile"
