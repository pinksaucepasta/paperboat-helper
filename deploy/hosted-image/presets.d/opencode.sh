#!/usr/bin/env bash
set -eu
command -v opencode >/dev/null 2>&1 && exit 0
npm install -g opencode-ai@1.18.3
command -v opencode >/dev/null 2>&1
