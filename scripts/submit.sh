#!/bin/sh
HOST=http://127.0.0.1:8780/api

if [ ! -z "$1" ]; then
  HOST=$1
fi

cd $(dirname $0)
curl $HOST/submit -H 'Content-Type: application/json' -d @payload.json

