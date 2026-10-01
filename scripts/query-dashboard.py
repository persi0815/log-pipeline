#!/usr/bin/env python3
"""Create live Vega panels backed by Elasticsearch Query DSL."""
import copy
import json
from datetime import datetime, timedelta, timezone
from common import ROOT, KIBANA, es, request


def main():
    run = json.loads((ROOT / 'results/last-run.json').read_text())
    scope = {'term': {'lab.run_id': run['run_id']}}
    objects, panels, references, verified = [], [], [], {}

    def panel(slug, title, body, path, mark, encoding, x, y, w, h, transform=None):
        # Verify actual DSL before adding Kibana dashboard context tokens.
        test_body = copy.deepcopy(body)
        test_body['query'] = scope
        result = es('/prac-lab-logs-*/_search', 'POST', test_body)
        verified[slug] = result
        body['query'] = {'bool': {
            'must': ['%dashboard_context-must_clause%'],
            'must_not': ['%dashboard_context-must_not_clause%'],
            'filter': [scope, '%dashboard_context-filter_clause%',
                       {'range': {'@timestamp': {'%timefilter%': True}}}]}}
        spec = {'$schema': 'https://vega.github.io/schema/vega-lite/v5.json',
                'width': 'container', 'height': 'container',
                'data': {'url': {'index': 'prac-lab-logs-*', 'body': body}, 'format': {'property': path}},
                'mark': mark, 'encoding': encoding,
                'config': {'view': {'stroke': None}, 'axis': {'labelFontSize': 11, 'titleFontSize': 12}}}
        if transform:
            spec['transform'] = transform
        ident = 'prac-query-chart-' + slug
        objects.append({'type': 'visualization', 'id': ident, 'attributes': {
            'title': title, 'description': 'Live Elasticsearch Query DSL aggregation',
            'visState': json.dumps({'title': title, 'type': 'vega', 'params': {'spec': json.dumps(spec)}, 'aggs': []}),
            'uiStateJSON': '{}', 'kibanaSavedObjectMeta': {'searchSourceJSON': json.dumps({'query': {'query': '', 'language': 'kuery'}, 'filter': []})}},
            'references': []})
        name = 'panel_' + str(len(panels))
        panels.append({'panelIndex': str(len(panels)), 'panelRefName': name, 'type': 'visualization',
                       'gridData': {'x': x, 'y': y, 'w': w, 'h': h, 'i': str(len(panels))}, 'embeddableConfig': {}})
        references.append({'type': 'visualization', 'id': ident, 'name': name})

    def metric(slug, title, aggregation, path, field, x, fmt=',.0f'):
        panel(slug, title, {'size': 0, 'aggs': aggregation}, path,
              {'type': 'text', 'fontSize': 32, 'color': '#343741'},
              {'text': {'field': field, 'type': 'quantitative', 'format': fmt}}, x, 0, 12, 8)

    metric('count', '적재된 로그', {'value': {'filter': {'match_all': {}}}}, 'aggregations.value', 'doc_count', 0)
    metric('errors', 'ERROR 로그', {'value': {'filter': {'term': {'log.level': 'error'}}}}, 'aggregations.value', 'doc_count', 12)
    metric('avg', '평균 적재 지연 (ms)', {'value': {'avg': {'field': 'lab.latency_ms'}}}, 'aggregations.value', 'value', 24, ',.1f')
    metric('p95', 'p95 적재 지연 (ms)', {'value': {'percentiles': {'field': 'lab.latency_ms', 'percents': [95], 'keyed': False}}},
           'aggregations.value.values', 'value', 36, ',.1f')

    panel('timeline', '실제 Elasticsearch 적재량 · 1초 단위', {'size': 0, 'aggs': {
        'buckets': {'date_histogram': {'field': 'event.ingested', 'fixed_interval': '1s', 'min_doc_count': 0}}}},
        'aggregations.buckets.buckets', {'type': 'bar', 'color': '#54B399'}, {
            'x': {'field': 'key', 'type': 'temporal', 'title': '적재 시각'},
            'y': {'field': 'doc_count', 'type': 'quantitative', 'title': '건 / 초'},
            'tooltip': [{'field': 'key_as_string', 'title': '적재 시각'}, {'field': 'doc_count', 'title': '건수'}]}, 0, 8, 32, 15)
    panel('levels', '로그 레벨별 건수', {'size': 0, 'aggs': {'buckets': {'terms': {'field': 'log.level', 'size': 5}}}},
        'aggregations.buckets.buckets', {'type': 'arc', 'innerRadius': 55}, {
            'theta': {'field': 'doc_count', 'type': 'quantitative'},
            'color': {'field': 'key', 'type': 'nominal', 'title': '레벨',
                      'scale': {'domain': ['info', 'warn', 'error'], 'range': ['#54B399', '#D6BF57', '#E7664C']}},
            'tooltip': [{'field': 'key', 'title': '레벨'}, {'field': 'doc_count', 'title': '건수'}]}, 32, 8, 16, 15)
    panel('services', '서비스별 로그 건수', {'size': 0, 'aggs': {'buckets': {'terms': {'field': 'service.name', 'size': 10}}}},
        'aggregations.buckets.buckets', 'bar', {
            'y': {'field': 'key', 'type': 'nominal', 'title': None},
            'x': {'field': 'doc_count', 'type': 'quantitative', 'title': '건수'},
            'color': {'field': 'key', 'type': 'nominal', 'legend': None, 'scale': {'domain': ['catalog-api', 'checkout-api', 'payment-api'], 'range': ['#54B399', '#6092C0', '#9170B8']}},
            'tooltip': [{'field': 'key', 'title': '서비스'}, {'field': 'doc_count', 'title': '건수'}]}, 0, 23, 24, 14)
    panel('routing', '서비스 key → 파티션 분포', {'size': 0, 'aggs': {'buckets': {'terms': {'field': 'service.name', 'size': 10},
          'aggs': {'partitions': {'terms': {'field': 'kafka.partition', 'size': 3}}}}}},
        'aggregations.buckets.buckets', 'rect', {
            'y': {'field': 'key', 'type': 'nominal', 'title': '서비스 key'},
            'x': {'field': 'partitions.key', 'type': 'ordinal', 'title': '파티션'},
            'color': {'field': 'partitions.doc_count', 'type': 'quantitative', 'title': '건수'},
            'tooltip': [{'field': 'key', 'title': '서비스'}, {'field': 'partitions.key', 'title': '파티션'}, {'field': 'partitions.doc_count', 'title': '건수'}]},
        24, 23, 24, 14, [{'flatten': ['partitions.buckets'], 'as': ['partitions']}])
    panel('status', 'HTTP 상태 코드별 건수', {'size': 0, 'aggs': {'buckets': {'terms': {'field': 'http.response.status_code', 'size': 10}}}},
        'aggregations.buckets.buckets', {'type': 'bar', 'color': '#9170B8'}, {
            'x': {'field': 'key', 'type': 'ordinal', 'title': 'HTTP 상태'},
            'y': {'field': 'doc_count', 'type': 'quantitative', 'title': '건수'},
            'tooltip': [{'field': 'key', 'title': 'HTTP 상태'}, {'field': 'doc_count', 'title': '건수'}]}, 0, 37, 24, 14)
    panel('latency', '적재 지연 분포 · 100ms 구간', {'size': 0, 'aggs': {'buckets': {'histogram': {'field': 'lab.latency_ms', 'interval': 100, 'min_doc_count': 0}}}},
        'aggregations.buckets.buckets', {'type': 'bar', 'color': '#54B399'}, {
            'x': {'field': 'key', 'type': 'quantitative', 'title': '지연 구간 시작 (ms)'},
            'y': {'field': 'doc_count', 'type': 'quantitative', 'title': '건수'},
            'tooltip': [{'field': 'key', 'title': '구간 시작 (ms)'}, {'field': 'doc_count', 'title': '건수'}]}, 24, 37, 24, 14)
    assert verified['count']['aggregations']['value']['doc_count'] == run['sent']
    assert sum(b['doc_count'] for b in verified['timeline']['aggregations']['buckets']['buckets']) == run['sent']
    limits = es('/prac-lab-logs-*/_search', 'POST', {'size': 0, 'query': scope,
          'aggs': {'first': {'min': {'field': '@timestamp'}}, 'last': {'max': {'field': 'event.ingested'}}}})['aggregations']
    def date(v, delta):
        return (datetime.fromtimestamp(v / 1000, timezone.utc) + timedelta(seconds=delta)).isoformat()
    objects.append({'type': 'dashboard', 'id': 'prac-elastic-dsl-analysis', 'references': references, 'attributes': {
        'title': 'PRAC · 로그 분석 · Query DSL', 'description': 'Live Query DSL charts for ' + run['run_id'],
        'panelsJSON': json.dumps(panels), 'optionsJSON': json.dumps({'useMargins': True, 'hidePanelTitles': False}),
        'timeRestore': True, 'timeFrom': date(limits['first']['value'], -1), 'timeTo': date(limits['last']['value'], 2),
        'refreshInterval': {'pause': True, 'value': 10000},
        'kibanaSavedObjectMeta': {'searchSourceJSON': json.dumps({'query': {'language': 'kuery', 'query': ''}, 'filter': []})}}})
    raw = '\n'.join(json.dumps(o, ensure_ascii=False) for o in objects) + '\n'
    (ROOT / 'kibana/query-lab.ndjson').write_text(raw)
    (ROOT / 'results/query-lab-results.json').write_text(json.dumps(verified, ensure_ascii=False, indent=2))
    boundary = 'prac_query_lab_boundary'
    payload = (f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="query-lab.ndjson"\r\nContent-Type: application/ndjson\r\n\r\n' + raw + f'\r\n--{boundary}--\r\n').encode()
    result = request(KIBANA, '/api/saved_objects/_import?overwrite=true', 'POST', payload,
                     'multipart/form-data; boundary=' + boundary)
    if not result.get('success'):
        raise RuntimeError(json.dumps(result))
    print('Verified and installed 10 live Query DSL panels')
    print(KIBANA + '/app/dashboards#/view/prac-elastic-dsl-analysis')


if __name__ == '__main__':
    main()
