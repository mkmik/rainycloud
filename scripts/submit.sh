#!/bin/sh

cd $(dirname $0)
curl http://127.0.0.1:8780/api/submit -H 'Content-Type: application/json' -d @payload.json

