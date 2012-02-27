#!/bin/sh
HOST=http://127.0.0.1:8780/api

if [ ! -z "$1" ]; then
  HOST=$1
fi

cd $(dirname $0)

for i in $(seq 6 10); do
    cp payload.json /tmp/tmp.json
    sed "s/hspec_suitable10/hspec_suitable$i/" -i /tmp/tmp.json

    curl $HOST/submit -H 'Content-Type: application/json' -d @/tmp/tmp.json
    echo " submitted"
done

