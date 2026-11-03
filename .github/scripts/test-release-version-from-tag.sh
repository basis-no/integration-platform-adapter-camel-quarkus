#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
parser="$script_dir/release-version-from-tag.sh"

assert_version() {
  expected="$1"
  tag="$2"
  actual="$($parser "$tag")"
  if [[ "$actual" != "$expected" ]]; then
    echo "Expected '$tag' to produce '$expected', got '$actual'" >&2
    exit 1
  fi
}

assert_rejected() {
  tag="$1"
  if "$parser" "$tag" >/dev/null 2>&1; then
    echo "Expected '$tag' to be rejected" >&2
    exit 1
  fi
}

assert_version "0.4.0" "v0.4.0"
assert_version "12.34.56" "v12.34.56"

assert_rejected ""
assert_rejected "0.4.0"
assert_rejected "v0.4"
assert_rejected "v0.4.0-rc1"
assert_rejected "v2.0.0-rc27"
assert_rejected "v0.4.0-rc"
assert_rejected "v0.4.0-rc0"
assert_rejected "v0.4.0-rc.1"
assert_rejected "v0.4.0-SNAPSHOT"
assert_rejected "v0.4.0-beta1"

echo "Release tag parser tests passed."
