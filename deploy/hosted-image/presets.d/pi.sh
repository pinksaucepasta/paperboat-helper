#!/usr/bin/env bash
set -eu
command -v pi >/dev/null 2>&1 && exit 0
npm install -g @mariozechner/pi-coding-agent@0.73.1
command -v pi >/dev/null 2>&1
