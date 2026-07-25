#!/usr/bin/env bash
set -eu
export PATH="$HOME/.local/bin:$PATH"
command -v cursor-agent >/dev/null 2>&1 && exit 0
version=2026.07.16-899851b
case "$(uname -m)" in
  x86_64|amd64) arch=x64; sha256=106acf6b3a3781cd279038726abc4f79f987449b9f5219b0f6e62d96c88fee6d ;;
  arm64|aarch64) arch=arm64; sha256=8ee8caf3f54aca6c73b68c13c0d64bdb989b1a65dfb6574414d9b030d0d10918 ;;
  *) echo "unsupported Cursor architecture" >&2; exit 1 ;;
esac
root="$HOME/.local/share/cursor-agent/versions/$version"
mkdir -p "$root" "$HOME/.local/bin"
archive="$(mktemp)"
trap 'rm -f "$archive"' EXIT
curl -fsSLo "$archive" "https://downloads.cursor.com/lab/$version/linux/$arch/agent-cli-package.tar.gz"
echo "$sha256  $archive" | sha256sum -c -
tar --strip-components=1 -xzf "$archive" -C "$root"
ln -sfn "$root/cursor-agent" "$HOME/.local/bin/cursor-agent"
ln -sfn "$root/cursor-agent" "$HOME/.local/bin/agent"
