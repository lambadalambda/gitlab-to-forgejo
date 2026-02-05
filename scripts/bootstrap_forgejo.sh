#!/usr/bin/env bash
set -euo pipefail

forgejo_http="${FORGEJO_HTTP:-http://localhost:${FORGEJO_HTTP_PORT:-3000}}"
admin_user="${FORGEJO_ADMIN_USERNAME:-root}"
admin_pass="${FORGEJO_ADMIN_PASSWORD:-admin1234}"
admin_email="${FORGEJO_ADMIN_EMAIL:-admin@example.com}"
token_name="${FORGEJO_ADMIN_TOKEN_NAME:-migrator}"
token_scopes="${FORGEJO_ADMIN_TOKEN_SCOPES:-all}"

state_dir="${STATE_DIR:-state/forgejo}"
token_file="${state_dir}/admin_token"

mkdir -p "$state_dir"

bash scripts/wait_for_url.sh "${forgejo_http}/api/v1/version"

set +e
docker compose exec -T --user git forgejo forgejo admin user create \
  --admin \
  --username "$admin_user" \
  --password "$admin_pass" \
  --email "$admin_email"
create_rc=$?
set -e

if [[ $create_rc -ne 0 ]]; then
  # Most common case: user already exists (re-run). Keep going.
  echo "forgejo admin user create returned $create_rc; continuing" >&2
fi

if [[ -f "$token_file" ]]; then
  echo "Token already exists at $token_file" >&2
  exit 0
fi

docker compose exec -T --user git forgejo forgejo admin user generate-access-token \
  --username "$admin_user" \
  --token-name "$token_name" \
  --scopes "$token_scopes" \
  --raw >"$token_file"

echo "Wrote admin token to $token_file" >&2
