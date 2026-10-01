#!/bin/bash
set -euo pipefail

cd /usr/share/elasticsearch
cert_dir=config/certs
mkdir -p "$cert_dir"

# Never replace an existing CA or silently recover half-written credentials.
if [ ! -e "$cert_dir/ca" ] && [ ! -e "$cert_dir/ca.zip" ]; then
  bin/elasticsearch-certutil ca --silent --pem --out "$cert_dir/ca.zip"
  unzip -q "$cert_dir/ca.zip" -d "$cert_dir"
fi
test -s "$cert_dir/ca/ca.crt"
test -s "$cert_dir/ca/ca.key"

if [ ! -e "$cert_dir/certs.zip" ]; then
  for node in es01 es02 es03; do
    if [ -e "$cert_dir/$node" ]; then
      echo "Incomplete certificate state for $node; inspect the certs volume." >&2
      exit 1
    fi
  done
  {
    echo 'instances:'
    for node in es01 es02 es03; do
      printf '  - name: %s\n    dns: [%s, localhost]\n    ip: [127.0.0.1]\n' "$node" "$node"
    done
  } > "$cert_dir/instances.yml"
  bin/elasticsearch-certutil cert --silent --pem \
    --out "$cert_dir/certs.zip" --in "$cert_dir/instances.yml" \
    --ca-cert "$cert_dir/ca/ca.crt" --ca-key "$cert_dir/ca/ca.key"
  unzip -q "$cert_dir/certs.zip" -d "$cert_dir"
fi
for node in es01 es02 es03; do
  test -s "$cert_dir/$node/$node.crt"
  test -s "$cert_dir/$node/$node.key"
done

chown -R root:root "$cert_dir"
find "$cert_dir" -type d -exec chmod 750 {} +
find "$cert_dir" -type f -exec chmod 640 {} +

# Export only the public CA, atomically, for the other cluster to trust.
cp "$cert_dir/ca/ca.crt" /export-ca/ca.crt.tmp
chmod 644 /export-ca/ca.crt.tmp
mv /export-ca/ca.crt.tmp /export-ca/ca.crt
# Share only the public CA with ingestion containers, without private keys.
mkdir -p /export-public-ca
chmod 755 /export-public-ca
cp "$cert_dir/ca/ca.crt" /export-public-ca/ca.crt.tmp
chmod 644 /export-public-ca/ca.crt.tmp
mv /export-public-ca/ca.crt.tmp /export-public-ca/ca.crt
echo 'Certificates ready.' 
