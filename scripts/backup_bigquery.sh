#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-learn-mcp-490919}"
SOURCE_DATASET="${SOURCE_DATASET:-lease_lens}"
BACKUP_DATASET="${BACKUP_DATASET:-lease_lens_backup}"
BACKUP_SUFFIX="${BACKUP_SUFFIX:-$(date +%Y%m%d_%H%M%S)}"

TABLES=(
  "area_live_scores"
  "area_live_scores_serving"
  "bangalore_pincodes"
  "business_profiles"
  "expansion_sessions"
  "peer_competition_counts"
)

echo "Project: ${PROJECT_ID}"
echo "Source dataset: ${SOURCE_DATASET}"
echo "Backup dataset: ${BACKUP_DATASET}"
echo "Backup suffix: ${BACKUP_SUFFIX}"

if ! bq --project_id="${PROJECT_ID}" ls "${BACKUP_DATASET}" >/dev/null 2>&1; then
  echo "Creating backup dataset ${PROJECT_ID}:${BACKUP_DATASET}"
  bq --project_id="${PROJECT_ID}" mk --dataset "${PROJECT_ID}:${BACKUP_DATASET}"
fi

for table in "${TABLES[@]}"; do
  src="${PROJECT_ID}:${SOURCE_DATASET}.${table}"
  dst="${PROJECT_ID}:${BACKUP_DATASET}.${table}_${BACKUP_SUFFIX}"
  echo "Copying ${src} -> ${dst}"
  bq --project_id="${PROJECT_ID}" cp "${src}" "${dst}"
done

echo "Backup complete."
