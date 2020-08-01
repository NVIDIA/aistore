#!/bin/bash
set -e

# TODO: support multi platform
arch=amd64
os=linux

bin_program=$1

echo "Creating a temporary directory..."
tmpdir=$(mktemp -d)
pushd $tmpdir

echo "Downloading ais..."
curl -Lo ais "https://github.com/NVIDIA/aistore/releases/latest/download/${bin_program}-${os}-${arch}"

echo "Downloading checksum file..."
curl -Lo checksum "https://github.com/NVIDIA/aistore/releases/latest/download/${bin_program}-${os}-${arch}.sha256"

echo "Calculating sha256 checksum for ais..."
sha256sum ${bin_program}

echo "Comparing checksums..."
sha256sum -c checksum 2>&1

echo "Installing ais at /usr/local/bin"
chmod +x ${bin_program}
mkdir -p /usr/local/bin
sudo cp ${bin_program} /usr/local/bin

popd
