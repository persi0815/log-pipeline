#!/usr/bin/env python3
"""Preserve the original query examples dashboard alongside live DSL analysis."""
import json
from common import ROOT, KIBANA, request, kibana


def main():
    slugs = ['count', 'errors', 'services', 'routing', 'timeline', 'latency', 'search']
    layout = [('prac-query-guide', 0, 0, 48, 10)]
    for i, slug in enumerate(slugs):
        y = 10 + i * 30
        layout += [('prac-query-code-' + slug, 0, y, 28, 28),
                   ('prac-query-result-' + slug, 28, y, 20, 16)]
        if slug != 'search':
            layout.append(('prac-query-live-' + slug, 28, y + 16, 20, 12))
    objects, panels, refs = [], [], []
    for i, (ident, x, y, w, h) in enumerate(layout):
        obj = kibana('/api/saved_objects/visualization/' + ident)
        objects.append({k: obj[k] for k in ['type', 'id', 'attributes', 'references']})
        name = 'panel_' + str(i)
        panels.append({'panelIndex': str(i), 'panelRefName': name, 'type': 'visualization',
                       'gridData': {'x': x, 'y': y, 'w': w, 'h': h, 'i': str(i)}, 'embeddableConfig': {}})
        refs.append({'type': 'visualization', 'id': ident, 'name': name})
    dashboard = kibana('/api/saved_objects/dashboard/prac-elastic-query-lab')
    attrs = dashboard['attributes']
    attrs['title'] = 'PRAC · 쿼리 예시와 검증 결과'
    attrs['panelsJSON'] = json.dumps(panels)
    run = json.loads((ROOT / 'results/last-run.json').read_text())
    attrs['kibanaSavedObjectMeta']['searchSourceJSON'] = json.dumps({'query': {'language': 'kuery', 'query': 'lab.run_id: "' + run['run_id'] + '"'}, 'filter': []})
    objects.append({'type': 'dashboard', 'id': 'prac-elastic-query-lab', 'attributes': attrs, 'references': refs})
    raw = '\n'.join(json.dumps(o, ensure_ascii=False) for o in objects) + '\n'
    (ROOT / 'kibana/query-examples.ndjson').write_text(raw)
    boundary = 'prac_restore_query_boundary'
    payload = (f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="examples.ndjson"\r\nContent-Type: application/ndjson\r\n\r\n' + raw + f'\r\n--{boundary}--\r\n').encode()
    result = request(KIBANA, '/api/saved_objects/_import?overwrite=true', 'POST', payload, 'multipart/form-data; boundary=' + boundary)
    if not result.get('success'):
        raise RuntimeError(json.dumps(result))
    print('Restored original query examples dashboard:', len(panels), 'panels')


if __name__ == '__main__':
    main()
