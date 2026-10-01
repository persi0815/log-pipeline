#!/usr/bin/env bash
set -euo pipefail
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
docker compose --env-file "$PROJECT_DIR/.env" -f "$PROJECT_DIR/compose.yaml" up -d --wait --wait-timeout 600 kibana
printf '%s\n' 'Elasticsearch ready: https://localhost:9210; Kibana: http://localhost:5610'
