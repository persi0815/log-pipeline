"""Local lab API helpers; credentials are read from the existing cluster .env."""
import base64
import json
import os
from pathlib import Path
import ssl
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parent.parent
CLUSTER = ROOT
env = {}
for line in (CLUSTER / ".env").read_text().splitlines():
    if line.strip() and not line.lstrip().startswith("#") and "=" in line:
        key, value = line.split("=", 1)
        env[key.strip()] = value.strip().strip("\"'")
PASSWORD = os.getenv("ELASTIC_PASSWORD", env["ELASTIC_PASSWORD"])
AUTH = "Basic " + base64.b64encode(("elastic:" + PASSWORD).encode()).decode()
CA = Path(os.getenv("CA_PATH", str(ROOT / "certificates/ca.crt")))
ES = os.getenv("ES_URL", "https://localhost:" + env.get("ES_PORT", "9210"))
KIBANA = os.getenv("KIBANA_URL", "http://localhost:" + env.get("KIBANA_PORT", "5610"))


def request(base, path, method="GET", body=None, content_type="application/json"):
    data = body if isinstance(body, bytes) else (json.dumps(body).encode() if body is not None else None)
    req = urllib.request.Request(base + path, data=data, method=method, headers={
        "Authorization": AUTH, "Content-Type": content_type, "kbn-xsrf": "prac-ingest-lab",
    })
    context = ssl.create_default_context(cafile=str(CA)) if base.startswith("https") else None
    try:
        with urllib.request.urlopen(req, context=context, timeout=60) as response:
            raw = response.read()
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"{method} {path}: HTTP {exc.code}: {exc.read().decode()[:1500]}") from exc


def es(path, method="GET", body=None):
    return request(ES, path, method, body)


def kibana(path, method="GET", body=None):
    return request(KIBANA, path, method, body)
