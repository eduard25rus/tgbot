#!/usr/bin/env bash
set -Eeuo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

if [[ "$(git branch --show-current)" != "main" ]]; then
  echo "Deploys must run from main" >&2
  exit 2
fi
if ! git diff --quiet || ! git diff --cached --quiet; then
  echo "Commit tracked changes before deploying" >&2
  exit 2
fi

export PYTHONPYCACHEPREFIX="${TMPDIR:-/tmp}/felis-pycache"
python3 -m py_compile webapp.py storage.py bot.py serve.py runtime_safety.py scripts/smoke_webapp.py
python3 -m unittest discover -s tests -v
python3 scripts/smoke_webapp.py

git push origin main
release_sha="$(git rev-parse HEAD)"
server="${FELIS_SERVER:-root@72.56.8.42}"
admin_key="${FELIS_ADMIN_KEY:-$HOME/.ssh/pantera_timeweb_admin_ed25519}"
ssh -i "$admin_key" -o IdentitiesOnly=yes "$server" "/srv/felis/app/deploy/timeweb/release.sh '$release_sha'"
