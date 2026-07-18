#!/bin/sh
set -eu
contracts=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo=$(basename "$(CDPATH= cd -- "$contracts/../.." && pwd)")
manifest="$contracts/manifest.json"
consumer="$contracts/consumer.json"
command -v jq >/dev/null 2>&1 || { echo "$repo contracts: jq is required" >&2; exit 2; }
jq -e --arg repo "$repo" '.manifestVersion == 1 and ([.artifacts[] | select(.consumers | index($repo))] | length > 0)' "$manifest" >/dev/null
jq -e --arg repo "$repo" --arg digest "$(shasum -a 256 "$manifest" | awk '{print $1}')" --arg version "$(jq -r '.contractVersion' "$manifest")" '.repository == $repo and .contract_version == $version and .canonical_source == "contracts/manifest.json" and .copied_manifest_sha256 == $digest and (.supported.fixture_manifest == "1") and (.last_review | length > 0)' "$consumer" >/dev/null || { echo "$repo contracts: invalid provenance" >&2; exit 1; }
jq -e --arg repo "$repo" '.version == "1.0.0" and (.consumers[$repo] | type == "object") and (.unsupported.mutation_allowed == false) and (.unsupported.error_code == "protocol_incompatible")' "$contracts/compatibility/matrix.json" >/dev/null || { echo "$repo contracts: invalid compatibility claim" >&2; exit 1; }
jq -s -e 'any(.[]; .valid == true) and any(.[]; .valid == false) and all(.[]; if .valid then (.selected as $selected | .offered | index($selected)) != null else (.error | length > 0) and .mutated == false end)' "$contracts/fixtures/compatibility/pairs.ndjson" >/dev/null || { echo "$repo contracts: invalid compatibility vectors" >&2; exit 1; }
while IFS='|' read -r path digest; do
  file="$contracts/$path"
  [ -f "$file" ] || { echo "$repo contracts: missing $path" >&2; exit 1; }
  actual=$(shasum -a 256 "$file" | awk '{print $1}')
  [ "$actual" = "$digest" ] || { echo "$repo contracts: stale $path" >&2; exit 1; }
done <<EOF
$(jq -r --arg repo "$repo" '.artifacts[] | select(.consumers | index($repo)) | "\(.path)|\(.sha256)"' "$manifest")
EOF
for file in $(find "$contracts" -type f | sort); do
  relative=${file#"$contracts/"}
  case "$relative" in manifest.json|consumer.json|validate.sh) continue ;; esac
  jq -e --arg repo "$repo" --arg path "$relative" '.artifacts[] | select(.path == $path and (.consumers | index($repo)))' "$manifest" >/dev/null || { echo "$repo contracts: unknown or inapplicable artifact $relative" >&2; exit 1; }
done
echo "$repo contracts: valid"
