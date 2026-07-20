#!/usr/bin/env bash
set -Eeuo pipefail

project_root="${FELIS_ROOT:-/srv/felis}"
kind="${1:-manual}"

python3 "$project_root/app/deploy/timeweb/sqlite_backup.py" \
  --source "$project_root/data/contracts.db" \
  --target-dir "$project_root/backups" \
  --kind "$kind"

find "$project_root/backups" -type f -name 'contracts-*.db' -mtime +30 -delete
find "$project_root/backups" -type f -name 'contracts-*.manifest.json' -mtime +30 -delete
