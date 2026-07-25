#!/usr/bin/env bash
set -eu
command -v amp >/dev/null 2>&1 && exit 0
npm install -g @sourcegraph/amp@0.0.1784551160-g777afc
command -v amp >/dev/null 2>&1
