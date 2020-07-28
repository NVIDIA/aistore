#!/bin/bash
set -e

arch=$(uname -s | tr '[:upper:]' '[:lower:]')
echo "Creating a temporary directory..."
tmpdir=$(mktemp -d)
pushd $tmpdir

echo "Downloading ais..."
curl -L "https://github.com/NVIDIA/aistore/releases/latest/download/ais-${arch}" -o "ais"

echo "Downloading checksum file..."
curl -L "https://github.com/NVIDIA/aistore/releases/latest/download/ais-${arch}.sha256" -o "checksum"

echo "Calculating sha256 checksum for ais..."
sha256sum ais

echo "Comparing checksums..."
sha256sum -c checksum 2>&1 | grep OK

echo "Installing ais at /usr/local/bin"
chmod +x ais
mkdir -p /usr/local/bin
sudo cp ais /usr/local/bin

popd
