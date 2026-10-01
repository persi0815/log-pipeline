#!/bin/bash

input="web_traffic.json"
output="bulk.log"
counter=0
max_rows=10000
create='{"create": {}}'
bulk_data=$'\n'

echo "Reading web traffic events from $input..."

while read -r log_event
do
  let "counter=counter+1"
  bulk_data+="$create"$'\n'"$log_event"$'\n'
  if [ $counter -eq $max_rows ]
  then
       echo "Indexing $counter documents..."
       bulk_data+=$'\n'
       echo "$bulk_data" | tee temp.json > /dev/null
       curl -XPOST 'https://elastic:elasticsearch2@localhost:8200/web_traffic/_bulk' -H 'Content-Type: application/x-ndjson' -# --progress-bar   --insecure --data-binary @temp.json > "$output"
       rm -rf temp.json
       counter=0
       bulk_data=$'\n'
  fi
done < "$input"

if [ $counter -lt $max_rows ] && [ $counter -gt 0 ]
then
       echo "Indexing $counter documents..."
       bulk_data+=$'\n'
       echo "$bulk_data" | tee temp.json > /dev/null
       curl -XPOST 'https://elastic:elasticsearch2@localhost:8200/web_traffic/_bulk' -H 'Content-Type: application/x-ndjson'  -# --progress-bar --insecure --data-binary @temp.json > "$output"
       rm -rf temp.json
fi
