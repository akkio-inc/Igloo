#!/bin/sh
# Creates an env file (default: .env next to this script) from .env.example,
# replacing each "change-me" with a random secret. Refuses to overwrite.
#
#   ./init-env.sh            writes ./.env
#   ./init-env.sh <path>     writes <path>
set -eu

here="$(cd "$(dirname "$0")" && pwd)"
target="${1:-$here/.env}"

if [ -e "$target" ]; then
  echo "$target already exists; leaving it alone" >&2
  exit 0
fi

secret() {
  LC_ALL=C tr -dc 'A-Za-z0-9' </dev/urandom | head -c 32
}

umask 077
while IFS= read -r line || [ -n "$line" ]; do
  case "$line" in
    *=change-me) printf '%s=%s\n' "${line%=change-me}" "$(secret)" ;;
    *) printf '%s\n' "$line" ;;
  esac
done <"$here/.env.example" >"$target"

echo "wrote $target (admin password: IGLOO_ADMIN_PASSWORD in that file)"
