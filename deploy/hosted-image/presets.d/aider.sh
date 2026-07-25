#!/usr/bin/env bash
set -eu
export PATH="$HOME/.local/bin:$PATH"
command -v aider >/dev/null 2>&1 && exit 0
venv="$HOME/.local/share/paperboat/agents/aider-0.86.2"
python3 -m venv "$venv"
"$venv/bin/pip" install --no-cache-dir --disable-pip-version-check aider-chat==0.86.2
mkdir -p "$HOME/.local/bin"
ln -sfn "$venv/bin/aider" "$HOME/.local/bin/aider"
command -v aider >/dev/null 2>&1
