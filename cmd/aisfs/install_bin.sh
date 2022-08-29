#!/bin/bash
set -e

echo "Downloading binary installer..."
curl -Lo bin_installer.sh https://raw.githubusercontent.com/NVIDIA/aistore/master/cmd/bin_installer.sh
chmod +x bin_installer.sh
./bin_installer.sh aisfs
