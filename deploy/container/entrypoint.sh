#!/bin/bash
# Container entrypoint: the first argument picks the process. The commands match the systemd units.
#   dashboard   autocraft-server dash serve
#   runner      autocraft-server runner serve (also serves the worker API)
#   sync        autocraft-server sync [args]             one-shot
#   backup      deploy/backup.sh [PREFIX [KEEP]]  one-shot, default "daily"
#   periodic    autocraft-server periodic          sync every minute and the daily backup, while this copy leads them
#   anything else is passed to the autocraft-server CLI.
set -euo pipefail
cd /app
mkdir -p "${HOME:?}"
cli=(node --no-warnings src/cli.ts)
cmd="${1:-dashboard}"
[ "$#" -gt 0 ] && shift
case "$cmd" in
dashboard) exec "${cli[@]}" dash serve "$@" ;;
runner) exec "${cli[@]}" runner serve "$@" ;;
sync) exec "${cli[@]}" sync "$@" ;;
backup) exec bash deploy/backup.sh "${@:-daily}" ;;
periodic) exec "${cli[@]}" periodic "$@" ;;
*) exec "${cli[@]}" "$cmd" "$@" ;;
esac
