#!/usr/bin/env python3
"""Check the last producer run actually reached Elasticsearch and Kafka lag is zero."""
import json
import subprocess
import time
import urllib.request
from common import ROOT, es


def main():
    run = json.loads((ROOT / "results/last-run.json").read_text())
    query = {"term": {"lab.run_id": run["run_id"]}}
    deadline = time.monotonic() + 120
    while True:
        result = es("/prac-lab-logs-*/_search", "POST", {
            "size": 3, "track_total_hits": True, "query": query,
            "sort": [{"lab.sequence": "asc"}],
            "aggs": {
                "levels": {"terms": {"field": "log.level"}},
                "ids": {"cardinality": {"field": "event.id", "precision_threshold": 40000}},
                "latency_ms": {"stats": {"field": "lab.latency_ms"}},
                "parse_errors": {"filter": {"terms": {"tags": ["_jsonparsefailure", "_dateparsefailure", "_mutate_error"]}}},
            },
        })
        count = result["hits"]["total"]["value"]
        if count == run["sent"]:
            break
        if time.monotonic() >= deadline:
            raise SystemExit(f"FAIL: sent={run['sent']} indexed={count}")
        print(f"Waiting for Elasticsearch: {count}/{run['sent']}", flush=True)
        time.sleep(5)
    assert result["aggregations"]["ids"]["value"] == run["sent"], "Duplicate or missing event IDs"
    assert result["aggregations"]["parse_errors"]["doc_count"] == 0, "Logstash parsing failures"
    for hit in result["hits"]["hits"]:
        doc = hit["_source"]
        assert doc["log"]["level"] in ("info", "warn", "error")
        assert doc["message"] == doc["message"].strip()
        assert isinstance(doc["http"]["response"]["status_code"], int)
        assert isinstance(doc["duration_ms"], (int, float))
        assert doc["kafka"]["topic"] == "prac-lab-logs"
        assert "ingested" in doc["event"]
    command = ["docker", "exec", "-e", "KAFKA_HEAP_OPTS=-Xms32m -Xmx128m", "kafka-1",
               "/opt/kafka/bin/kafka-consumer-groups.sh", "--bootstrap-server", "kafka-1:19092",
               "--describe", "--group", "prac-lab-logstash"]
    lag_output = subprocess.check_output(command, text=True, timeout=60)
    rows = [line.split() for line in lag_output.splitlines() if line.startswith("prac-lab-logstash")]
    assert len(rows) == 3, lag_output
    assert all(row[5] == "0" for row in rows), lag_output
    with urllib.request.urlopen("http://localhost:9610/_node/stats/pipelines/main", timeout=15) as response:
        pipeline = json.load(response)["pipelines"]["main"]
    assert pipeline["queue"]["events_count"] == 0, "Logstash persistent queue has pending events"
    # These counters reset when Logstash restarts; persisted ES counts are the proof of delivery.
    summary = {"run_id": run["run_id"], "sent": run["sent"], "indexed": count,
               "levels": result["aggregations"]["levels"]["buckets"],
               "latency_ms": result["aggregations"]["latency_ms"],
               "kafka_lag": 0, "logstash_events": pipeline["events"],
               "logstash_queue": pipeline["queue"]["events_count"], "sample": result["hits"]["hits"][0]["_source"]}
    (ROOT / "results/verification.json").write_text(json.dumps(summary, indent=2) + "\n")
    (ROOT / "results/kafka-lag.txt").write_text(lag_output)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
