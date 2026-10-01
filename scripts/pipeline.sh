#!/usr/bin/env bash
set -euo pipefail
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
lab_compose() { docker compose --env-file "$PROJECT_DIR/.env" -f "$PROJECT_DIR/compose.yaml" "$@"; }
kafka_cli() { docker exec -e 'KAFKA_HEAP_OPTS=-Xms32m -Xmx128m' kafka-1 "/opt/kafka/bin/$1" "${@:2}"; }
case "${1:-help}" in
  up)
    lab_compose up -d --wait --wait-timeout 600
    ;;
  produce)
    shift
    mkdir -p "$PROJECT_DIR/results"
    lab_compose run --build --rm -T --no-deps producer "$@" 2> "$PROJECT_DIR/results/producer-stderr.log"
    ;;
  verify) python3 "$PROJECT_DIR/scripts/verify.py" ;;
  status)
    lab_compose ps -a
    kafka_cli kafka-consumer-groups.sh --bootstrap-server kafka-1:19092 --describe --group prac-lab-logstash
    ;;
  logs) lab_compose logs -f --tail=40 ;;
  stop) lab_compose stop ;;
  *) echo 'Usage: bash scripts/pipeline.sh {up|produce [--count 300 --rate 5]|verify|status|logs|stop}' ;;
esac
