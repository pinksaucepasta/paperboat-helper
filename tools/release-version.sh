#!/bin/sh
set -eu

repository_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)

usage() {
  echo "usage: $0 current | next | validate <YYYY.MM.DD.X>" >&2
  exit 64
}

valid_version() {
  version=$1
  printf '%s\n' "$version" | awk -F. '
    NF == 4 && $1 ~ /^[0-9][0-9][0-9][0-9]$/ &&
    $2 ~ /^[0-9][0-9]$/ && $2 >= 1 && $2 <= 12 &&
    $3 ~ /^[0-9][0-9]$/ && $3 >= 1 && $3 <= 31 &&
    $4 ~ /^(0|[1-9][0-9]*)$/ { found = 1 }
    END { exit found ? 0 : 1 }
  ' || return 1
  release_date=${version%.*}
  calendar_date=$(printf '%s' "$release_date" | tr . -)
  normalized=$(date -u -d "$calendar_date" +%Y-%m-%d 2>/dev/null || date -j -u -f %Y-%m-%d "$calendar_date" +%Y-%m-%d 2>/dev/null) || return 1
  [ "$normalized" = "$calendar_date" ]
}

case "${1:-}" in
  current)
    [ "$#" -eq 1 ] || usage
    for tag in $(git -C "$repository_root" tag --points-at HEAD); do
      if valid_version "$tag"; then
        printf '%s\n' "$tag"
        exit 0
      fi
    done
    exec "$0" next
    ;;
  next)
    [ "$#" -eq 1 ] || usage
    release_date=$(date -u +%Y.%m.%d)
    sequence=-1
    for tag in $(git -C "$repository_root" tag -l "$release_date.*"); do
      valid_version "$tag" || continue
      candidate=${tag##*.}
      [ "$candidate" -le "$sequence" ] || sequence=$candidate
    done
    printf '%s.%s\n' "$release_date" "$((sequence + 1))"
    ;;
  validate)
    [ "$#" -eq 2 ] || usage
    version=$2
    valid_version "$version" || {
      echo "invalid release version: $version (expected YYYY.MM.DD.X)" >&2
      exit 1
    }
    release_date=${version%.*}
    sequence=${version##*.}
    current=0
    while [ "$current" -le "$sequence" ]; do
      git -C "$repository_root" rev-parse -q --verify "refs/tags/$release_date.$current" >/dev/null || {
        echo "missing release tag $release_date.$current; daily sequences must start at 0 and be contiguous" >&2
        exit 1
      }
      current=$((current + 1))
    done
    ;;
  *) usage ;;
esac
