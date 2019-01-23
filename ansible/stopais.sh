#!/bin/bash
set -e
for pid in `ps -C ais -o pid= `; do
	sudo kill $pid
done
