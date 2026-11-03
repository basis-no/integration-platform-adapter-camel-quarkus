#!/usr/bin/env bash

set -euo pipefail

tag="${1:-}"
if [[ ! "$tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "Tag '$tag' is not a final release tag (vX.Y.Z); refusing to publish." >&2
  exit 1
fi

printf '%s\n' "${tag#v}"
