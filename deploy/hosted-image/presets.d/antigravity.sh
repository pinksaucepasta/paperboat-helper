#!/usr/bin/env bash
set -eu
export PATH="$HOME/.local/bin:$PATH"
command -v agy >/dev/null 2>&1 && exit 0
case "$(uname -m)" in
  x86_64|amd64)
    asset=linux-x64/cli_linux_x64.tar.gz
    sha512=a088a1f231d8565b6673cecd8656fc3504e49c89e9c6b8c4116937b5fe7069c8dcfba78bbb2bc5c0ff8e87ba64fe21b63db7001e3a5794504927dad9e89da973
    ;;
  arm64|aarch64)
    asset=linux-arm/cli_linux_arm64.tar.gz
    sha512=8d3c464303b235b6f2c2d441eca07b0c1cc35efa68f7ae16b167a5a2d49373903efdf686b3e41063424f0cf0c5b5d5eb056f7944dade7abf1b8eb225cb8c438c
    ;;
  *) echo "unsupported Antigravity architecture" >&2; exit 1 ;;
esac
archive="$(mktemp)"
trap 'rm -f "$archive"' EXIT
curl -fsSLo "$archive" "https://storage.googleapis.com/antigravity-public/antigravity-cli/1.1.4-6277569641840640/$asset"
echo "$sha512  $archive" | sha512sum -c -
mkdir -p "$HOME/.local/bin"
staging="$(mktemp -d)"
tar -xzf "$archive" -C "$staging" antigravity
install -m 0755 "$staging/antigravity" "$HOME/.local/bin/agy"
