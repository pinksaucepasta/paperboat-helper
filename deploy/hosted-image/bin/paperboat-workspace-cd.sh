# shellcheck shell=sh
# Open interactive Paperboat shells inside the cloned project repository.
#
# Sourced from /etc/profile.d (login shells) and /etc/bash.bashrc
# (interactive non-login shells), so agent terminals land in the project
# directory regardless of how the shell was started. The resolved
# path is written by paperboat-prepare-workspace at clone time.
case "$-" in
  *i*) ;;
  *) return 2>/dev/null || exit 0 ;;
esac

if [ -n "${PAPERBOAT_WORKSPACE_CD_DONE:-}" ]; then
  return 2>/dev/null || true
fi

__pb_marker="${PAPERBOAT_WORKSPACE:-/workspace}/.paperboat/project-dir"
if [ -r "$__pb_marker" ]; then
  __pb_dir=$(cat "$__pb_marker" 2>/dev/null)
  if [ -n "$__pb_dir" ] && [ -d "$__pb_dir" ]; then
    PAPERBOAT_WORKSPACE_CD_DONE=1
    export PAPERBOAT_WORKSPACE_CD_DONE
    cd "$__pb_dir" 2>/dev/null || true
  fi
fi
unset __pb_marker __pb_dir
