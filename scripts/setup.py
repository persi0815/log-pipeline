#!/usr/bin/env python3
"""Install only lab-prefixed templates, ingest pipeline and Kibana saved objects."""
import json
import time
from common import ROOT, ES, KIBANA, es, request


def main():
    for attempt in range(60):
        try:
            status = request(KIBANA, "/api/status")
            if status["status"]["overall"]["level"] == "available":
                break
        except (OSError, RuntimeError):
            pass
        if attempt == 59:
            raise RuntimeError("Kibana is not ready; check docker compose logs kibana")
        print("Waiting for Kibana...", flush=True)
        time.sleep(3)
    es("/_ingest/pipeline/prac-lab-ingested", "PUT", {
        "description": "Stamp actual Elasticsearch ingest time and end-to-end latency",
        "processors": [
            {"set": {"field": "event.ingested", "value": "{{{_ingest.timestamp}}}"}},
            {"script": {"if": "ctx.containsKey('lab') && ctx.containsKey('@timestamp')",
                        "source": "ctx.lab.latency_ms = ChronoUnit.MILLIS.between(ZonedDateTime.parse(ctx['@timestamp']), ZonedDateTime.parse(ctx.event.ingested));"}},
        ],
    })
    es("/_index_template/prac-lab-logs", "PUT", {
        "index_patterns": ["prac-lab-logs-*"], "priority": 250,
        "template": {
            "settings": {"number_of_shards": 1, "number_of_replicas": 1},
            "mappings": {"properties": {
                "@timestamp": {"type": "date"}, "message": {"type": "text"},
                "duration_ms": {"type": "float"},
                "event": {"properties": {"id": {"type": "keyword"}, "dataset": {"type": "keyword"},
                                          "outcome": {"type": "keyword"}, "ingested": {"type": "date"}}},
                "lab": {"properties": {"run_id": {"type": "keyword"}, "sequence": {"type": "long"},
                                        "latency_ms": {"type": "long"}}},
                "log": {"properties": {"level": {"type": "keyword"}}},
                "service": {"properties": {"name": {"type": "keyword"}}},
                "http": {"properties": {"response": {"properties": {"status_code": {"type": "integer"}}}}},
                "kafka": {"properties": {"topic": {"type": "keyword"}, "partition": {"type": "integer"},
                                          "offset": {"type": "long"}}},
            }},
        },
    })
    objects = []
    for view_id, title, name in [("prac-lab-logs", "prac-lab-logs-*", "PRAC · Application logs"),
                                 ("prac-lab-metrics", "metricbeat-*", "PRAC · Kafka metrics"),
                                 ("prac-lab-monitoring", "metricbeat-*,.monitoring-*", "PRAC · All infrastructure metrics")]:
        objects.append({"type": "index-pattern", "id": view_id, "attributes": {
            "title": title, "name": name, "timeFieldName": "@timestamp", "fields": "[]",
            "fieldFormatMap": json.dumps({"lab.latency_ms": {"id": "number", "params": {"pattern": "0,0.[0]"}}, "@timestamp": {"id": "date", "params": {"pattern": "YYYY-MM-DD HH:mm:ss"}}})}})

    def visual(ident, title, kind, aggs, params=None, query="", view="prac-lab-logs"):
        attrs = {"title": title, "visState": json.dumps({"title": title, "type": kind,
                 "params": params or {}, "aggs": aggs}), "uiStateJSON": json.dumps({"vis.colors": {"info": "#54B399", "warn": "#D6BF57", "error": "#E7664C", "catalog-api": "#54B399", "checkout-api": "#6092C0", "payment-api": "#9170B8"}}),
                 "description": "PRAC Kafka → Logstash → Elasticsearch lab",
                 "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps({
                     "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index",
                     "query": {"language": "kuery", "query": query}, "filter": []})}}
        objects.append({"type": "visualization", "id": ident, "attributes": attrs,
                        "references": [{"type": "index-pattern", "id": view,
                                        "name": "kibanaSavedObjectMeta.searchSourceJSON.index"}]})

    def metric(ident, title, agg="count", field=None, query="", view="prac-lab-logs"):
        visual(ident, title, "metric", [{"id": "1", "enabled": True, "type": agg, "schema": "metric",
               "params": {"field": field} if field else {}}],
               {"addTooltip": True, "addLegend": False, "type": "metric", "metric": {
                    "percentageMode": False, "useRanges": False, "colorSchema": "Green to Red",
                    "metricColorMode": "None", "colorsRange": [{"from": 0, "to": 1000000}],
                    "style": {"fontSize": 32, "bgFill": "#000", "bgColor": False, "labelColor": False, "subText": ""},
                    "labels": {"show": True}, "invertColors": False}}, query, view)

    count = {"id": "1", "enabled": True, "type": "count", "schema": "metric", "params": {}}
    metric("prac-lab-total", "01 · Indexed events")
    metric("prac-lab-errors", "02 · ERROR events", query='log.level: "error"')
    metric("prac-lab-latency", "03 · Mean latency (ms)", "avg", "lab.latency_ms")
    metric("prac-lab-lag", "04 · Peak Kafka lag", "max", "kafka.consumergroup.consumer_lag",
           'kafka.consumergroup.id: "prac-lab-logstash"', "prac-lab-metrics")
    chart_params = {"addTooltip": True, "addLegend": True, "legendPosition": "right",
                    "categoryAxes": [{"id": "CategoryAxis-1", "type": "category", "position": "bottom",
                                      "show": True, "scale": {"type": "linear"}, "labels": {"show": True}, "title": {"text": "Time"}}],
                    "valueAxes": [{"id": "ValueAxis-1", "type": "value", "position": "left", "show": True,
                                   "scale": {"type": "linear"}, "labels": {"show": True}, "title": {"text": "Events"}}],
                    "seriesParams": [{"show": True, "type": "histogram", "mode": "stacked", "data": {"id": "1", "label": "Events"},
                                      "valueAxis": "ValueAxis-1"}], "grid": {"categoryLines": False}, "labels": {"show": False}}
    visual("prac-lab-throughput", "05 · Events over time by log level", "histogram", [count,
           {"id": "2", "enabled": True, "type": "date_histogram", "schema": "segment",
            "params": {"field": "@timestamp", "interval": "auto", "min_doc_count": 0}},
           {"id": "3", "enabled": True, "type": "terms", "schema": "group",
            "params": {"field": "log.level", "size": 5, "order": "desc", "orderBy": "1"}}], chart_params)
    visual("prac-lab-services", "06 · Events by service", "pie", [count,
           {"id": "2", "enabled": True, "type": "terms", "schema": "segment",
            "params": {"field": "service.name", "size": 5, "order": "desc", "orderBy": "1"}}],
           {"addTooltip": True, "addLegend": True, "legendPosition": "right", "isDonut": True})
    visual("prac-lab-partitions", "07 · Kafka topic / partition / consumer lag", "table", [
        {"id": "1", "enabled": True, "type": "max", "schema": "metric", "params": {"field": "kafka.consumergroup.consumer_lag"}},
        {"id": "2", "enabled": True, "type": "terms", "schema": "bucket", "params": {"field": "kafka.topic.name", "size": 5, "orderBy": "_key", "order": "asc"}},
        {"id": "3", "enabled": True, "type": "terms", "schema": "bucket", "params": {"field": "kafka.partition.id", "size": 10, "orderBy": "_key", "order": "asc"}},
    ], {"perPage": 10, "showTotal": False}, 'kafka.consumergroup.id: "prac-lab-logstash"', "prac-lab-metrics")
    visual("prac-lab-guide", "Pipeline guide", "markdown", [], {"fontSize": 14, "openLinksInNewTab": False,
        "markdown": "### PRAC · 로그 파이프라인 실습\n**Java Producer → Kafka (`prac-lab-logs`) → Logstash → Elasticsearch → Kibana**\n\n"
                    "정제: 공백·레벨·시간·숫자. 전체 지연: 로그 생성부터 Elasticsearch 적재까지.\n\n"
                    "[로그 원문 / Discover](/app/discover#/?_a=(index:prac-lab-logs)) · "
                    "[Stack Monitoring / Logstash 파이프라인](/app/monitoring) · 자동 새로고침 10초\n\n"
                    "지표 수집 확인: 아래 최근 2분 카드가 0보다 크고 최신 수집 시각이 계속 갱신되면 수집 중입니다.\n\n최근 2분 패널은 전체 시간 선택과 독립적입니다. 다른 패널은 선택 시간 범위를 사용합니다. Lag·큐·heap은 선택 시간 내 최댓값이며 현재값이 아닙니다. 로그가 없을 때도 지표는 계속 수집됩니다. 로그 필드로 전역 필터를 걸면 지표 패널이 비어 보일 수 있습니다."})
    objects.append({"type": "search", "id": "prac-lab-events", "attributes": {
        "title": "08 · Latest normalized events", "description": "Follow run ID and Kafka offsets through the pipeline",
        "columns": ["lab.run_id", "log.level", "service.name", "message", "http.response.status_code",
                    "kafka.partition", "kafka.offset", "lab.latency_ms"], "sort": [["@timestamp", "desc"]],
        "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps({"indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index",
            "query": {"query": "", "language": "kuery"}, "filter": []})}},
        "references": [{"name": "kibanaSavedObjectMeta.searchSourceJSON.index", "type": "index-pattern", "id": "prac-lab-logs"}]})
    # 지표 데이터는 로그와 별도의 data view로 조회합니다.
    monitoring_view = "prac-lab-monitoring"
    for module in ("kafka", "logstash", "elasticsearch", "kibana"):
        metric("prac-lab-recent-" + module, module.upper() + " · 최근 2분 수집 건수",
               query='event.module: "' + module + '"', view=monitoring_view)

    visual("prac-lab-freshness", "수집원별 최신 시각 · 선택 기간 지표 건수", "table", [
        count,
        {"id": "2", "enabled": True, "type": "max", "schema": "metric",
         "params": {"field": "@timestamp", "customLabel": "최신 수집 시각"}},
        {"id": "3", "enabled": True, "type": "terms", "schema": "bucket",
         "params": {"field": "event.module", "size": 10, "orderBy": "_key", "order": "asc"}},
    ], {"perPage": 10, "showTotal": False}, view=monitoring_view)

    visual("prac-lab-metric-flow", "지표 수집 추이 · 수집원별", "histogram", [count,
        {"id": "2", "enabled": True, "type": "date_histogram", "schema": "segment",
         "params": {"field": "@timestamp", "interval": "auto", "min_doc_count": 0}},
        {"id": "3", "enabled": True, "type": "terms", "schema": "group",
         "params": {"field": "event.module", "size": 10, "order": "desc", "orderBy": "1"}},
    ], chart_params, view=monitoring_view)

    metric("prac-lab-queue", "Logstash · Peak queued events", "max",
           "logstash.node.stats.queue.events_count", view=monitoring_view)
    metric("prac-lab-heap", "Logstash · Peak JVM heap (%)", "max",
           "logstash.node.stats.jvm.mem.heap_used_percent", view=monitoring_view)
    metric("prac-lab-es-heap", "ES · Peak JVM heap (%)", "max",
           "elasticsearch.node.stats.jvm.mem.heap.used.pct", view=monitoring_view)
    metric("prac-lab-parse-errors", "로그 파싱 오류 건수",
           query='tags: ("_jsonparsefailure" or "_dateparsefailure" or "_mutate_error")')
    visual("prac-lab-key-routing", "서비스 key → 파티션 · 건수 / 최신 offset", "table", [count,
        {"id": "2", "enabled": True, "type": "max", "schema": "metric", "params": {"field": "kafka.offset"}},
        {"id": "3", "enabled": True, "type": "terms", "schema": "bucket", "params": {"field": "service.name", "size": 10, "orderBy": "_key", "order": "asc"}},
        {"id": "4", "enabled": True, "type": "terms", "schema": "bucket", "params": {"field": "kafka.partition", "size": 10, "orderBy": "_key", "order": "asc"}},
    ], {"perPage": 10, "showTotal": False})
    objects.append({"type": "search", "id": "prac-lab-metric-events", "attributes": {
        "title": "지표 원문 · 최신 수집 문서", "description": "Verify fresh metric documents are indexed",
        "columns": ["event.module", "event.dataset", "metricset.period", "agent.name", "service.address"],
        "sort": [["@timestamp", "desc"]],
        "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps({
            "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index",
            "query": {"query": "", "language": "kuery"}, "filter": []})}},
        "references": [{"name": "kibanaSavedObjectMeta.searchSourceJSON.index", "type": "index-pattern", "id": monitoring_view}]})

    panels, refs = [], []
    layout = [
        ("prac-lab-guide", "visualization", 0, 0, 48, 12),
        ("prac-lab-recent-kafka", "visualization", 0, 12, 12, 7),
        ("prac-lab-recent-logstash", "visualization", 12, 12, 12, 7),
        ("prac-lab-recent-elasticsearch", "visualization", 24, 12, 12, 7),
        ("prac-lab-recent-kibana", "visualization", 36, 12, 12, 7),
        ("prac-lab-freshness", "visualization", 0, 19, 20, 12),
        ("prac-lab-metric-flow", "visualization", 20, 19, 28, 12),
        ("prac-lab-total", "visualization", 0, 31, 12, 7),
        ("prac-lab-errors", "visualization", 12, 31, 12, 7),
        ("prac-lab-latency", "visualization", 24, 31, 12, 7),
        ("prac-lab-lag", "visualization", 36, 31, 12, 7),
        ("prac-lab-queue", "visualization", 0, 38, 12, 7),
        ("prac-lab-heap", "visualization", 12, 38, 12, 7),
        ("prac-lab-es-heap", "visualization", 24, 38, 12, 7),
        ("prac-lab-parse-errors", "visualization", 36, 38, 12, 7),
        ("prac-lab-throughput", "visualization", 0, 45, 32, 13),
        ("prac-lab-services", "visualization", 32, 45, 16, 13),
        ("prac-lab-partitions", "visualization", 0, 58, 24, 10),
        ("prac-lab-key-routing", "visualization", 24, 58, 24, 10),
        ("prac-lab-metric-events", "search", 0, 68, 48, 15),
        ("prac-lab-events", "search", 0, 83, 48, 15),
    ]
    for n, (ident, kind, x, y, w, h) in enumerate(layout):
        name = "panel_" + str(n)
        panels.append({"panelIndex": str(n), "panelRefName": name, "type": kind,
                       "gridData": {"x": x, "y": y, "w": w, "h": h, "i": str(n)},
                       "embeddableConfig": ({"timeRange": {"from": "now-2m", "to": "now"}}
                                            if ident.startswith("prac-lab-recent-") else {})})
        refs.append({"type": kind, "id": ident, "name": name})
    objects.append({"type": "dashboard", "id": "prac-log-pipeline", "references": refs, "attributes": {
        "title": "PRAC · Kafka → Logstash → Elasticsearch", "description": "End-to-end local log pipeline and Kafka consumer lag",
        "panelsJSON": json.dumps(panels), "optionsJSON": json.dumps({"useMargins": True, "hidePanelTitles": False}),
        "timeRestore": True, "timeFrom": "now-1h", "timeTo": "now",
        "refreshInterval": {"pause": False, "value": 10000},
        "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps({"query": {"query": "", "language": "kuery"}, "filter": []})},
    }})
    raw = "\n".join(json.dumps(item) for item in objects) + "\n"
    (ROOT / "kibana/lab.ndjson").write_text(raw)
    boundary = "prac_lab_saved_objects_boundary"
    payload = (f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="lab.ndjson"\r\n'
               'Content-Type: application/ndjson\r\n\r\n' + raw + f'\r\n--{boundary}--\r\n').encode()
    imported = request(KIBANA, "/api/saved_objects/_import?overwrite=true", "POST", payload,
                       "multipart/form-data; boundary=" + boundary)
    if not imported.get("success"):
        raise RuntimeError(json.dumps(imported, indent=2))
    print(f"Installed template, ingest pipeline and {imported['successCount']} Kibana objects")
    print(KIBANA + "/app/dashboards#/view/prac-log-pipeline")


if __name__ == "__main__":
    main()
