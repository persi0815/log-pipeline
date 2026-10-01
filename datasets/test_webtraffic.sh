#!/bin/bash

input="web_traffic.json"
counter=0
max_rows=40
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
       curl -XPOST 'https://elastic:elastic@localhost:9210/web_traffic/_bulk' -H 'Content-Type: application/json' --insecure --data-binary @temp.json
       rm -rf temp.json
       bulk_data=$'\n'
       break
  fi
done < "$input"