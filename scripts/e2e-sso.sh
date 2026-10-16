#!/usr/bin/env bash
# End-to-end test of single sign-on with a real OpenID Connect provider:
# Keycloak in Docker, with users, groups, a disabled account and an
# outsider. It starts an isolated copy of deploy/single-vm with TLS and
# IGLOO_OIDC_* settings next to Keycloak, signs in through Keycloak's login
# page with a scripted browser (scripts/clients/sso_browser.py), and checks
# group mapping, refusals, access tokens for SQL clients and session ends.
# Slower and heavier than scripts/e2e.sh, which covers access tokens
# without an identity provider. Removes everything afterwards unless
# KEEP_E2E=1; E2E_PROJECT and E2E_*_PORT let several runs share a host,
# and E2E_IMAGE tests an image built already instead of building one.
#
#   scripts/e2e-sso.sh
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
project="${E2E_PROJECT:-igloo-e2e-sso}"
sql_port="${E2E_SQL_PORT:-15533}"
http_port="${E2E_HTTP_PORT:-18532}"
console_port="${E2E_CONSOLE_PORT:-18543}"
image="${E2E_IMAGE:-$project:dev}"
build=--build
[ -z "${E2E_IMAGE:-}" ] || build=--no-build
keycloak_image="${E2E_KEYCLOAK_IMAGE:-quay.io/keycloak/keycloak:26.7.3}"
go_image="${GO_IMAGE:-golang:1.27-bookworm}"
work="$(mktemp -d)"
env_file="$work/e2e.env"
# head closing the pipe ends tr with SIGPIPE, which pipefail would report.
keycloak_admin_password="$(LC_ALL=C tr -dc 'A-Za-z0-9' </dev/urandom | head -c 24 || true)"

compose() {
  docker compose -p "$project" --project-directory "$root/deploy/single-vm" \
    -f "$root/deploy/single-vm/docker-compose.yml" -f "$work/sso.yml" --env-file "$env_file" "$@"
}

cleanup() {
  status=$?
  if [ "$status" -ne 0 ]; then
    echo "--- igloo logs"
    compose logs --no-color --tail 80 igloo || true
    echo "--- keycloak logs"
    compose logs --no-color --tail 40 keycloak || true
  fi
  if [ "${KEEP_E2E:-}" != 1 ]; then
    compose --profile local-storage --profile tools down -v --remove-orphans >/dev/null 2>&1 || true
    [ -n "${E2E_IMAGE:-}" ] || docker image rm "$image" >/dev/null 2>&1 || true
    rm -rf "$work"
  else
    echo "kept the stack: docker compose -p $project -f ... --env-file $env_file ..."
  fi
  exit "$status"
}
trap cleanup EXIT

failures=0
pass() { printf '  ok    %s\n' "$1"; }
fail() { printf '  FAIL  %s\n' "$1"; failures=$((failures + 1)); }

# One certificate for both Igloo and Keycloak, which Igloo and the browser
# trust; test only.
mkdir -p "$work/tls" "$work/jars"
docker run --rm -v "$work/tls:/out" -v igloo-go-build:/root/.cache/go-build -w /out "$go_image" sh -c \
  'go run /usr/local/go/src/crypto/tls/generate_cert.go --host igloo,keycloak,localhost,127.0.0.1 --ca --duration 4h >/dev/null 2>&1 && chmod 644 key.pem'
printf 'e2e-only-client-secret\n' >"$work/tls/client-secret"
chmod 644 "$work/tls/client-secret"

"$root/deploy/single-vm/init-env.sh" "$env_file" >/dev/null
sed -i.bak -e "s/^IGLOO_PORT=.*/IGLOO_PORT=$sql_port/" -e "s/^IGLOO_HTTP_PORT=.*/IGLOO_HTTP_PORT=$http_port/" \
  -e "s/^IGLOO_CONSOLE_PORT=.*/IGLOO_CONSOLE_PORT=$console_port/" -e "s|^IGLOO_IMAGE=.*|IGLOO_IMAGE=$image|" "$env_file"
admin_password="$(grep '^IGLOO_ADMIN_PASSWORD=' "$env_file" | cut -d= -f2)"

cat >"$work/sso.yml" <<EOF
services:
  igloo:
    environment:
      IGLOO_TLS_CERT_FILE: /e2e-tls/cert.pem
      IGLOO_TLS_KEY_FILE: /e2e-tls/key.pem
      # Trust the test certificate when calling Keycloak.
      SSL_CERT_FILE: /e2e-tls/cert.pem
      IGLOO_OIDC_ISSUER: https://keycloak:8443/realms/igloo
      IGLOO_OIDC_CLIENT_ID: igloo
      IGLOO_OIDC_CLIENT_SECRET_FILE: /e2e-tls/client-secret
      IGLOO_OIDC_ALLOWED_DOMAINS: agency.example
      IGLOO_OIDC_DISPLAY_NAME: Keycloak
      IGLOO_OIDC_ACCESS_TOKEN_AUDIENCE: igloo-sql
    volumes:
      - $work/tls:/e2e-tls:ro
  keycloak:
    image: $keycloak_image
    command: ["start-dev", "--import-realm"]
    environment:
      KC_BOOTSTRAP_ADMIN_USERNAME: admin
      KC_BOOTSTRAP_ADMIN_PASSWORD: $keycloak_admin_password
      KC_HOSTNAME: https://keycloak:8443
      KC_HTTPS_CERTIFICATE_FILE: /e2e-tls/cert.pem
      KC_HTTPS_CERTIFICATE_KEY_FILE: /e2e-tls/key.pem
    volumes:
      - $work/tls:/e2e-tls:ro
      - $root/scripts/sso/keycloak-realm.json:/opt/keycloak/data/import/igloo-realm.json:ro
EOF

echo "starting the stack and Keycloak"
compose up -d "$build" --wait --wait-timeout 300 igloo >/dev/null
compose up -d keycloak >/dev/null
network="${project}_default"

# browser COMMAND ARGS... runs the scripted browser on the stack's network.
browser() {
  docker run --rm --network "$network" -e E2E_CA=/e2e-tls/cert.pem -e E2E_JARS=/jars \
    -e E2E_KEYCLOAK_ADMIN_PASSWORD="$keycloak_admin_password" \
    -v "$work/tls:/e2e-tls:ro" -v "$work/jars:/jars" -v "$root/scripts/clients:/clients:ro" \
    python:3.12-slim python /clients/sso_browser.py "$@"
}
# field JSON KEY prints a string or number field of a one-line JSON object.
field() { sed -n "s/.*\"$2\": *\"\{0,1\}\([^\",}]*\)\"\{0,1\}.*/\1/p" <<<"$1"; }

psql_as() {
  local user=$1 password=$2
  shift 2
  docker run --rm -i --network "$network" -e PGPASSWORD="$password" -e PGSSLMODE=require postgres:17 \
    psql -h igloo -U "$user" -d igloo -X -At -v ON_ERROR_STOP=1 "$@" 2>&1
}
psql_admin() { psql_as igloo "$admin_password" "$@"; }
expect() {
  local name=$1 want=$2 got
  got="$(psql_as "$3" "$4" -c "$5")" || true
  if [ "$got" = "$want" ]; then pass "$name"; else fail "$name: got [$got], want [$want]"; fi
}

echo "waiting for Keycloak"
deadline=$((SECONDS + 300))
until docker run --rm --network "$network" -v "$work/tls:/e2e-tls:ro" curlimages/curl:8.22.0 \
  -fs --cacert /e2e-tls/cert.pem -o /dev/null https://keycloak:8443/realms/igloo/.well-known/openid-configuration; do
  if [ "$SECONDS" -ge "$deadline" ]; then
    echo "Keycloak did not start"
    exit 1
  fi
  sleep 3
done

echo "group mappings"
expect "map Keycloak groups to roles" "CREATE ROLE
GRANT
GRANT" igloo "$admin_password" "
  CREATE ROLE analysts;
  GRANT analysts TO SSO GROUP 'data-team';
  GRANT writer TO SSO GROUP 'igloo-writers';"
psql_admin -c "CREATE USER carol PASSWORD NULL EMAIL 'carol@agency.example' NOLOGIN" >/dev/null

echo "single sign-on"
got="$(browser signin alice alice-password)"
if [ "$(field "$got" result)|$(field "$got" user)|$(field "$got" method)" = "signed_in|alice|sso" ]; then
  pass "alice signs in through Keycloak and gets an Igloo user"
else
  fail "alice's sign-in: $got"
fi
if [ "$(field "$got" role)" = writer ]; then pass "a group gives a built-in role"; else fail "alice's role: $got"; fi
got="$(psql_admin -c "SHOW ROLES" | cut -d'|' -f1,2)"
if [ "$got" = "analysts|alice" ]; then pass "and another a custom role"; else fail "SHOW ROLES: $got"; fi
got="$(psql_admin -c "SHOW USERS" | grep '^alice|')"
if [ "$(cut -d'|' -f4,6,7,8,9 <<<"$got")" = "person|t|f|alice@agency.example|t" ]; then
  pass "the new user has no password and is linked to Keycloak"
else
  fail "SHOW USERS: $got"
fi

got="$(browser token alice dbt)"
alice_token="$(field "$got" token)"
if [[ "$alice_token" == igloo_* ]]; then pass "an SSO user creates an access token in the console"; else fail "token: $got"; fi
expect "and SQL clients sign in with it" "alice|t" alice "$alice_token" "SELECT current_user, count(*) > 0 FROM information_schema.tables"
got="$(browser access-token alice alice-password)"
expect "Keycloak's own access token signs in to SQL over TLS" "alice" alice "$(field "$got" access_token)" "SELECT current_user"
if psql_as alice "not-a-token" -c "SELECT 1" >/dev/null; then fail "a bad access token signed in"; else pass "a bad access token does not"; fi

echo "refusals"
got="$(browser signin bob bob-password)"
if [ "$(field "$got" result)" = idp ]; then pass "a user Keycloak disabled cannot sign in ($(field "$got" message))"; else fail "bob: $got"; fi
got="$(browser signin carol carol-password)"
if [ "$(field "$got" result)|$(field "$got" code)" = "refused|disabled" ]; then pass "a user Igloo disabled is refused"; else fail "carol: $got"; fi
got="$(browser signin eve eve-password)"
if [ "$(field "$got" result)|$(field "$got" code)" = "refused|domain" ]; then pass "an outsider's domain is refused"; else fail "eve: $got"; fi

echo "group changes and session ends"
got="$(browser remove-group alice igloo-writers)"
if [ "$(field "$got" result)" != removed ]; then fail "remove alice from igloo-writers: $got"; fi
got="$(browser signin alice alice-password)"
if [ "$(field "$got" result)|$(field "$got" role)" = "signed_in|none" ]; then
  pass "leaving a group takes its role away at the next sign-in"
else
  fail "alice after leaving igloo-writers: $got"
fi
psql_admin -c "ALTER USER alice NOLOGIN" >/dev/null
got="$(browser session alice)"
if [ "$(field "$got" status)" = 401 ]; then pass "disabling a user ends their console session"; else fail "alice's session: $got"; fi
if got="$(psql_as alice "$alice_token" -c "SELECT 1")"; then
  fail "a disabled user's token signed in"
elif grep -q "password authentication failed\|not permitted" <<<"$got"; then
  pass "and their access tokens stop working"
else
  fail "disabled alice's token: $got"
fi

echo "SCIM provisioning"
psql_admin -c "CREATE USER okta_scim TYPE SERVICE ROLE admin" >/dev/null
scim_token="$(psql_admin -c "CREATE TOKEN scim FOR USER okta_scim EXPIRES '1d'" | cut -d'|' -f3)"
# scim METHOD PATH [JSON] prints the response body, then its status alone
# on the last line.
scim() {
  docker run --rm --network "$network" -v "$work/tls:/e2e-tls:ro" curlimages/curl:8.22.0 -sS --cacert /e2e-tls/cert.pem \
    -H "Authorization: Bearer $scim_token" -H 'Content-Type: application/scim+json' -X "$1" "https://igloo:8443/scim/v2$2" \
    ${3:+-d "$3"} -w '\n%{http_code}'
}
status_of() { tail -1 <<<"$1"; }
got="$(scim POST /Users '{"schemas":["urn:ietf:params:scim:schemas:core:2.0:User"],"userName":"dana@agency.example","active":true,"emails":[{"value":"dana@agency.example","primary":true}]}')"
if [ "$(status_of "$got")" = 201 ] && grep -q '"id":"dana"' <<<"$got"; then pass "SCIM provisions a user"; else fail "SCIM POST /Users: $got"; fi
got="$(scim POST /Groups '{"schemas":["urn:ietf:params:scim:schemas:core:2.0:Group"],"displayName":"data-team","members":[{"value":"dana"}]}')"
group_id="$(sed -n 's/.*"id":"\([0-9a-f]*\)".*/\1/p' <<<"$got" | head -1)"
if [ "$(status_of "$got")" = 201 ] && [ -n "$group_id" ]; then pass "SCIM pushes a group"; else fail "SCIM POST /Groups: $got"; fi
if psql_admin -c "SHOW ROLES" | grep -q '^analysts|.*dana'; then pass "and its mapping gives the role at once"; else fail "dana's roles: $(psql_admin -c "SHOW ROLES")"; fi
got="$(scim PATCH /Users/dana '{"schemas":["urn:ietf:params:scim:api:messages:2.0:PatchOp"],"Operations":[{"op":"Replace","path":"active","value":"False"}]}')"
if [ "$(status_of "$got")" = 200 ] && [ "$(psql_admin -c "SHOW USERS" | grep '^dana|' | cut -d'|' -f6)" = f ]; then
  pass "SCIM deactivation disables the user"
else
  fail "SCIM deactivation: $got"
fi
got="$(scim PATCH "/Groups/$group_id" '{"schemas":["urn:ietf:params:scim:api:messages:2.0:PatchOp"],"Operations":[{"op":"remove","path":"members[value eq \"dana\"]"}]}')"
if [ "$(status_of "$got")" = 204 ] && ! psql_admin -c "SHOW ROLES" | grep -q 'dana'; then pass "leaving a SCIM group takes its role away"; else fail "SCIM group removal: $got"; fi
psql_admin -c "CREATE USER scim_reader TYPE SERVICE ROLE reader" >/dev/null
reader_token="$(psql_admin -c "CREATE TOKEN t FOR USER scim_reader EXPIRES '1d'" | cut -d'|' -f3)"
got="$(docker run --rm --network "$network" -v "$work/tls:/e2e-tls:ro" curlimages/curl:8.22.0 -sS --cacert /e2e-tls/cert.pem \
  -H "Authorization: Bearer $reader_token" -o /dev/null -w '%{http_code}' https://igloo:8443/scim/v2/Users)"
if [ "$got" = 401 ] || [ "$got" = 403 ]; then pass "SCIM refuses a non-admin's token"; else fail "SCIM with a reader's token: $got"; fi

echo "login history"
deadline=$((SECONDS + 60))
want="t|t|t|t"
got=""
while [ "$SECONDS" -lt "$deadline" ]; do
  got="$(psql_admin -c "SELECT
      bool_or(user_name = 'alice' AND auth_method = 'sso' AND result = 'success'),
      bool_or(user_name = 'alice' AND auth_method = 'token' AND token_name = 'dbt'),
      bool_or(user_name = 'alice' AND auth_method = 'access_token'),
      bool_or(user_name = 'carol' AND auth_method = 'sso' AND result = 'failure' AND reason = 'user is disabled')
    FROM igloo.login_history")" || true
  [ "$got" = "$want" ] && break
  sleep 2
done
if [ "$got" = "$want" ]; then pass "sign-ins are recorded with their method and token"; else fail "login history: $got"; fi

if [ "$failures" -ne 0 ]; then
  echo "$failures check(s) failed"
  exit 1
fi
echo "all checks passed"
