#!/bin/bash
set -e
for pid in `ps -C aisnode -o pid= `; do
	sudo kill $pid
done
