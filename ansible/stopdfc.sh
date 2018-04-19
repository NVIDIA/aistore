#!/bin/bash
set -e
for pid in `ps -C dfc -o pid= `; do
	sudo kill $pid
done
