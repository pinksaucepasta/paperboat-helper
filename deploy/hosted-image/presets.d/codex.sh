#!/usr/bin/env bash
set -eu
command -v codex >/dev/null 2>&1 && exit 0
npm install -g @openai/codex@0.144.6
command -v codex >/dev/null 2>&1
