#!/bin/bash

# Cleanup old containers (in case the script is reused) 
docker stop graphite grafana 2>/dev/null
docker rm graphite grafana 2>/dev/null
docker network ls --filter "name=dashboard-bridge" -q | xargs -r docker network rm
docker network create dashboard-bridge

# Graphite
docker run -d --rm\
 --name graphite\
 --network=dashboard-bridge\
 -p 80:80\
 -p 2003-2004:2003-2004\
 -p 2023-2024:2023-2024\
 -p 8125:8125/udp\
 -v ~/carbon_config/carbon.conf:/opt/graphite/conf/carbon.conf\
 -v ~/carbon_config/storage-schemas.conf:/opt/graphite/conf/storage-schemas.conf\
 graphiteapp/graphite-statsd

# Grafana
docker run -d --rm\
  --name grafana\
  --network=dashboard-bridge\
  -p 3000:3000\
  grafana/grafana

sleep 10
curl -d '{"name":"Graphite","type":"graphite","url":"http://graphite:80","access":"proxy","basicAuth":false,"isDefault":true}' -H "Content-Type: application/json" -X POST http://admin:admin@localhost:3000/api/datasources > /dev/null 2>&1