#!/bin/bash

while [ ! -f /var/lib/cloud/instance/boot-finished ]; do
  echo -e "Waiting for cloud-init..."
  sleep 1
done
