#!/usr/bin/env bash
set -Eeuo pipefail

current_ref="${1:?current image reference is required}"
rollback_ref="${2:?rollback image reference is required}"

inspect() {
  docker image inspect "$1" --format '{{.Architecture}}|{{index .Config.Labels "io.paperboat.image.contract"}}|{{index .Config.Labels "io.paperboat.protocol.version"}}|{{json .Config.Cmd}}'
}

current_contract="$(inspect "$current_ref")"
rollback_contract="$(inspect "$rollback_ref")"
if [ "$current_contract" != "$rollback_contract" ]; then
  printf 'rollback image contract mismatch\ncurrent:  %s\nrollback: %s\n' "$current_contract" "$rollback_contract" >&2
  exit 1
fi
if [ "$current_ref" = "$rollback_ref" ]; then
  printf 'rollback evidence requires two distinct image references\n' >&2
  exit 1
fi

for image in "$current_ref" "$rollback_ref"; do
  docker run --rm --entrypoint /usr/local/bin/pbh "$image" version >/dev/null
done

printf 'rollback-compatible: %s -> %s (%s)\n' "$current_ref" "$rollback_ref" "$current_contract"
