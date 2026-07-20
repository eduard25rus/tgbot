#!/usr/bin/env bash
set -Eeuo pipefail

release_sha="${1:-}"
project_root="${FELIS_ROOT:-/srv/felis}"
app_dir="$project_root/app"
state_dir="$project_root/state"
compose_file="$app_dir/compose.timeweb.yml"
deploy_key="$project_root/config/deploy_ed25519"

if [[ ! "$release_sha" =~ ^[0-9a-f]{40}$ ]]; then
  echo "A full 40-character Git SHA is required" >&2
  exit 2
fi

mkdir -p "$state_dir"
previous_sha=""
if [[ -f "$state_dir/current-release" ]]; then
  previous_sha="$(tr -d '[:space:]' < "$state_dir/current-release")"
fi

cd "$app_dir"
export GIT_SSH_COMMAND="ssh -i $deploy_key -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new"
git fetch --quiet origin "$release_sha"
resolved_sha="$(git rev-parse "${release_sha}^{commit}")"
if [[ "$resolved_sha" != "$release_sha" ]]; then
  echo "Fetched commit does not match requested SHA" >&2
  exit 3
fi
git checkout --quiet --detach "$release_sha"

if [[ -f "$project_root/data/contracts.db" ]]; then
  "$app_dir/deploy/timeweb/backup.sh" predeploy
fi

docker build --pull --tag "felis-crm:$release_sha" "$app_dir"
export RELEASE_SHA="$release_sha"
export WEB_BIND_PORT="${WEB_BIND_PORT:-8002}"
docker compose --project-name felis --file "$compose_file" up --detach --no-build

healthy=0
for _ in $(seq 1 24); do
  status="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' felis-crm-web 2>/dev/null || true)"
  if [[ "$status" == "healthy" ]]; then
    healthy=1
    break
  fi
  sleep 5
done

if [[ "$healthy" != "1" ]]; then
  docker logs --tail 80 felis-crm-web >&2 || true
  if [[ "$previous_sha" =~ ^[0-9a-f]{40}$ ]] && docker image inspect "felis-crm:$previous_sha" >/dev/null 2>&1; then
    export RELEASE_SHA="$previous_sha"
    docker compose --project-name felis --file "$compose_file" up --detach --no-build
    echo "Release failed; rolled back to $previous_sha" >&2
  else
    docker compose --project-name felis --file "$compose_file" stop web
    echo "Release failed; no previous image was available" >&2
  fi
  exit 1
fi

printf '%s\n' "$release_sha" > "$state_dir/current-release"
printf '%s\n' "$previous_sha" > "$state_dir/previous-release"
echo "Release $release_sha is healthy on 127.0.0.1:${WEB_BIND_PORT}"
