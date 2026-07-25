#!/usr/bin/env bash
set -eu
command -v crush >/dev/null 2>&1 && exit 0
npm install -g @charmland/crush@0.85.0
command -v crush >/dev/null 2>&1
