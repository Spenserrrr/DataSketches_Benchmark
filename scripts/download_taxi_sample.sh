#!/usr/bin/env bash
set -euo pipefail

OUT_DIR="data/raw/taxi"
FILE="yellow_tripdata_2024-01.parquet"
URL="https://d37ci6vzurychx.cloudfront.net/trip-data/${FILE}"

mkdir -p "${OUT_DIR}"

if [[ -f "${OUT_DIR}/${FILE}" ]]; then
  echo "Already exists: ${OUT_DIR}/${FILE}"
else
  echo "Downloading ${FILE}"
  curl -L "${URL}" -o "${OUT_DIR}/${FILE}"
fi

echo "Wrote taxi sample to ${OUT_DIR}/${FILE}"
