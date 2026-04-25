#!/usr/bin/env bash
set -euo pipefail

OUT_DIR="data/raw/wiki-pageviews"
mkdir -p "${OUT_DIR}"

# Small real-world sample: four hourly Wikimedia pageview files.
# These are enough for local correctness tests without downloading many GB.
BASE_URL="https://dumps.wikimedia.org/other/pageviews/2024/2024-01"
FILES=(
  "pageviews-20240101-000000.gz"
  "pageviews-20240101-010000.gz"
  "pageviews-20240101-020000.gz"
  "pageviews-20240101-030000.gz"
)

for file in "${FILES[@]}"; do
  if [[ ! -f "${OUT_DIR}/${file}" ]]; then
    echo "Downloading ${file}"
    curl -L "${BASE_URL}/${file}" -o "${OUT_DIR}/${file}"
  else
    echo "Already exists: ${file}"
  fi
done

echo "Wrote pageviews sample to ${OUT_DIR}"
