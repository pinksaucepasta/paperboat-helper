#!/usr/bin/env bash
set -eu
export PATH="$HOME/.local/bin:$PATH"
command -v goose >/dev/null 2>&1 && exit 0
curl -fsSL https://github.com/block/goose/releases/download/v1.43.0/download_cli.sh | GOOSE_VERSION=v1.43.0 CONFIGURE=false bash
command -v goose >/dev/null 2>&1
