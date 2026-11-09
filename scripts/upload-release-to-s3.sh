#!/bin/bash
# Downloads the (public) release files of a GitHub release with curl, verifies their checksums and uploads them to S3.
#
# Usage: upload-release-to-s3.sh <version>
#
# Environment overrides (useful for testing):
#   S3_BUCKET       destination, default s3://dist.neo4j.org
#   S3_ACL          default public-read
#   AWS_EXTRA_ARGS  extra flags for `aws s3 cp`, e.g. --dryrun
set -euo pipefail

VERSION="${1:?usage: $0 <version>}"
BUCKET="${S3_BUCKET:-s3://dist.neo4j.org}"
ACL="${S3_ACL:-public-read}"
AWS_EXTRA_ARGS="${AWS_EXTRA_ARGS:-}"
NAME="neo4j-kafka-connect-neo4j-${VERSION}"

WORKDIR=$(mktemp -d)
trap 'rm -rf "$WORKDIR"' EXIT

sha256() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | cut -d' ' -f1; else shasum -a 256 "$1" | cut -d' ' -f1; fi
}

BASE_URL="https://github.com/neo4j/neo4j-kafka-connector/releases/download/${VERSION}"

echo "Downloading ${NAME}.{jar,zip} and checksums from GitHub release ${VERSION}"
for ext in jar jar.sha256 zip zip.sha256; do
  curl --fail --silent --show-error --location --retry 3 \
    --output "$WORKDIR/${NAME}.${ext}" "${BASE_URL}/${NAME}.${ext}"
done

for f in "$WORKDIR/${NAME}.jar" "$WORKDIR/${NAME}.zip"; do
  expected=$(cut -d' ' -f1 "$f.sha256")
  actual=$(sha256 "$f")
  if [ "$expected" != "$actual" ]; then
    echo "CHECKSUM MISMATCH: $f (expected $expected, got $actual)" >&2
    exit 1
  fi
  echo "checksum ok: $(basename "$f")"
done

for f in "$WORKDIR/${NAME}".*; do
  echo "Uploading $(basename "$f") to ${BUCKET} (acl: ${ACL})"
  aws s3 cp "$f" "$BUCKET" --acl "$ACL" $AWS_EXTRA_ARGS
done
