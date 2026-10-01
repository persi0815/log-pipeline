#!/usr/bin/env python3
"""Apply a consistent chart palette without changing existing dashboard layouts."""
import json
from common import ROOT, KIBANA, kibana, request

COLORS = {'info': '#54B399', 'warn': '#D6BF57', 'error': '#E7664C',
          'catalog-api': '#54B399', 'checkout-api': '#6092C0', 'payment-api': '#9170B8'}


def main():
    ids = set()
    for ident in ['prac-log-pipeline', 'prac-elastic-query-lab', 'prac-elastic-dsl-analysis']:
        dashboard = kibana('/api/saved_objects/dashboard/' + ident)
        print(ident, len(json.loads(dashboard['attributes']['panelsJSON'])), 'panels')
        ids.update(r['id'] for r in dashboard['references'] if r['type'] == 'visualization')
    objects = []
    for ident in sorted(ids):
        obj = kibana('/api/saved_objects/visualization/' + ident)
        state = json.loads(obj['attributes']['visState'])
        if state['type'] in ['histogram', 'pie', 'metric']:
            ui = json.loads(obj['attributes'].get('uiStateJSON', '{}'))
            ui['vis.colors'] = COLORS
            obj['attributes']['uiStateJSON'] = json.dumps(ui)
            objects.append({k: obj[k] for k in ['type', 'id', 'attributes', 'references']})
    raw = '\n'.join(json.dumps(o) for o in objects) + '\n'
    boundary = 'prac_styles_boundary'
    payload = (f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="styles.ndjson"\r\nContent-Type: application/ndjson\r\n\r\n' + raw + f'\r\n--{boundary}--\r\n').encode()
    result = request(KIBANA, '/api/saved_objects/_import?overwrite=true', 'POST', payload,
                     'multipart/form-data; boundary=' + boundary)
    if not result.get('success'):
        raise RuntimeError(json.dumps(result))
    replacements = {o['id']: o for o in objects}
    for name in ['lab.ndjson', 'query-examples.ndjson']:
        path = ROOT / 'kibana' / name
        rows = [json.loads(line) for line in path.read_text().splitlines()]
        path.write_text('\n'.join(json.dumps(replacements.get(o['id'], o), ensure_ascii=False) for o in rows) + '\n')
    print('Unified palette:', result['successCount'], 'visualizations')


if __name__ == '__main__':
    main()
