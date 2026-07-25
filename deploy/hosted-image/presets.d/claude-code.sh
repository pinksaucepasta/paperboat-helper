#!/usr/bin/env bash
set -eu
command -v claude >/dev/null 2>&1 && exit 0
npm install -g @anthropic-ai/claude-code@2.1.215
command -v claude >/dev/null 2>&1
