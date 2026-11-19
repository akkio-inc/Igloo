#!/bin/sh
# Sets IGLOO_IMAGE in .env (next to this script): the image the stack runs.
# Every `docker compose` command here reads it, so the stack keeps running
# the same image whichever command comes next.
#
#   ./use-image.sh release   the release this checkout is at, or the last one
#                            before it
#   ./use-image.sh <image>   that image, such as igloo:dev (what `make dev`
#                            builds) or ghcr.io/akkio-inc/igloo:1.0.0
set -eu

here="$(cd "$(dirname "$0")" && pwd)"
env_file="$here/.env"
image="${1:?usage: use-image.sh release|<image>}"

# unpublished IMAGE: the registry says it has no such image, as while a
# release is building, or for a tag whose release never passed. An image
# that is here already, or a registry that cannot be asked, does not count.
unpublished() {
  docker image inspect "$1" >/dev/null 2>&1 && return 1
  docker manifest inspect "$1" 2>&1 >/dev/null | grep -qi 'manifest unknown'
}

if [ "$image" = release ]; then
  # The newest release tag this checkout holds whose image is published. A
  # shallow clone holds at most the tag of the commit it is at, and takes
  # those of the repository it came from. A release sorts above its own
  # candidates: 1.0.0 above 1.0.0-rc.12.
  tags=
  if [ "$(git -C "$here" rev-parse --is-shallow-repository 2>/dev/null)" != true ]; then
    tags="$(git -C "$here" -c versionsort.suffix=- tag --merged HEAD --sort=-v:refname --list 'v[0-9]*' 2>/dev/null |
      head -n 5)"
  fi
  if [ -z "$tags" ]; then
    tags="$(git -C "$here" -c versionsort.suffix=- ls-remote --tags --refs --sort=-v:refname origin 'v[0-9]*' 2>/dev/null |
      head -n 5 | sed 's|.*refs/tags/||')"
  fi
  tag=
  for candidate in $tags; do
    if unpublished "ghcr.io/akkio-inc/igloo:${candidate#v}"; then
      echo "$candidate is tagged and its image is not published: taking the release before it." >&2
      continue
    fi
    tag="$candidate"
    break
  done
  if [ -z "$tag" ]; then
    echo "No published release found for this checkout. Fetch its tags with \`git fetch --tags\`," >&2
    echo "name an image with \`make up IMAGE=ghcr.io/akkio-inc/igloo:<version>\`," >&2
    echo "or build Igloo from source with \`make dev\`." >&2
    exit 1
  fi
  image="ghcr.io/akkio-inc/igloo:${tag#v}"

  # Say so when this checkout's Igloo differs from that release: the release
  # does not have the changes, and `make dev` builds them.
  status=0
  git -C "$here" diff --quiet "$tag" -- ':/cmd' ':/internal' ':/extensions' ':/third_party' \
    ':/Dockerfile' ':/go.mod' ':/go.sum' 2>/dev/null || status=$?
  if [ "$status" -eq 1 ]; then
    echo "This checkout has changes to Igloo since $tag, which the release does not include: \`make dev\` builds them." >&2
  fi
fi

[ -e "$env_file" ] || "$here/init-env.sh" "$env_file"

# Rewrite in place, so the file keeps its mode: it holds the stack's secrets.
tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT
awk -v image="$image" '
  /^IGLOO_IMAGE=/ { print "IGLOO_IMAGE=" image; done = 1; next }
  { print }
  END { if (!done) print "IGLOO_IMAGE=" image }
' "$env_file" >"$tmp"
cat "$tmp" >"$env_file"

echo "Igloo image: $image"
