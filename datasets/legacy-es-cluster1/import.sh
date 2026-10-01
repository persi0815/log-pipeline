#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
project_dir="$(cd "$script_dir/.." && pwd)"
source "$project_dir/.env"

es_url="https://localhost:${ES_PORT}"
auth="elastic:${ELASTIC_PASSWORD}"
batch_size=10000

es_request() {
  curl --silent --show-error --fail --insecure --user "$auth" "$@"
}

for index_name in blogs web_traffic; do
  if es_request --head "$es_url/$index_name" >/dev/null 2>&1; then
    echo "Index already exists: $index_name" >&2
    exit 1
  fi
done

blogs_body="$(mktemp)"
tmp_dir="$(mktemp -d)"
response_file="$(mktemp)"
trap 'rm -f "$blogs_body" "$response_file"; rm -rf "$tmp_dir"' EXIT

jq '{mappings: .mappings, settings: {index: {refresh_interval: "-1"}}}' \
  "$script_dir/blogs-template.json" > "$blogs_body"

es_request -X PUT -H 'Content-Type: application/json' \
  --data-binary "@$blogs_body" "$es_url/blogs" >/dev/null
es_request -X PUT -H 'Content-Type: application/json' \
  --data-binary '{"settings":{"index":{"refresh_interval":"-1"}}}' \
  "$es_url/web_traffic" >/dev/null

load_index() {
  local index_name="$1"
  local input_file="$2"
  local chunk_count=0
  local chunk_file
  local bulk_file="$tmp_dir/bulk.ndjson"

  rm -f "$tmp_dir"/chunk-*
  split -a 4 -l "$batch_size" "$input_file" "$tmp_dir/chunk-"

  for chunk_file in "$tmp_dir"/chunk-*; do
    awk '{ print "{\"index\":{}}"; print }' "$chunk_file" > "$bulk_file"
    es_request -X POST -H 'Content-Type: application/x-ndjson' \
      --data-binary "@$bulk_file" "$es_url/$index_name/_bulk" > "$response_file"
    if [[ "$(jq -r '.errors' "$response_file")" != "false" ]]; then
      jq -c '.items[] | select(.index.status >= 300) | .index.error' "$response_file" | head -5 >&2
      return 1
    fi
    chunk_count=$((chunk_count + 1))
    printf '\r%s: %d documents indexed' "$index_name" "$((chunk_count * batch_size))"
    rm -f "$chunk_file"
  done
  printf '\n'
}

load_index blogs "$script_dir/blogs.json"
load_index web_traffic "$script_dir/web_traffic.json"

for index_name in blogs web_traffic; do
  es_request -X PUT -H 'Content-Type: application/json' \
    --data-binary '{"index":{"refresh_interval":"1s"}}' \
    "$es_url/$index_name/_settings" >/dev/null
  es_request -X POST "$es_url/$index_name/_refresh" >/dev/null
done

es_request "$es_url/_cat/indices/blogs,web_traffic?v&h=index,health,status,docs.count,store.size"
