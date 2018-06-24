#!/usr/bin/env bash

SIZE=10737418
END=9999

time curl -d '{"bucket": "dsort-testing", "prefix": "shard-", "extension": ".tar", "end": '${END}', "shard_size_bytes": '${SIZE}', "output_name_prefix": "new-shard-", "local": true, "digits_to_prepend_to": 5, "create_concurrency_limit": 1}' -H "Content-Type: application/json" -X POST http://localhost:8080/v1/sort

