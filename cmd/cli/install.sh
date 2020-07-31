#!/bin/bash
set -e

arch=$(uname -s | tr '[:upper:]' '[:lower:]')
echo "Creating a temporary directory..."
tmpdir=$(mktemp -d)
pushd $tmpdir

echo "Downloading ais..."
curl -Lo ais "https://github.com/NVIDIA/aistore/releases/latest/download/ais-${arch}"

echo "Downloading checksum file..."
curl -Lo checksum "https://github.com/NVIDIA/aistore/releases/latest/download/ais-${arch}.sha256"

echo "Calculating sha256 checksum for ais..."
sha256sum ais

echo "Comparing checksums..."
sha256sum -c checksum 2>&1

echo "Installing ais at /usr/local/bin"
chmod +x ais
mkdir -p /usr/local/bin
sudo cp ais /usr/local/bin

echo "Downloading autocomplete scripts..."
curl -Lo bash https://raw.githubusercontent.com/NVIDIA/aistore/master/cmd/cli/autocomplete/bash
curl -Lo zsh https://raw.githubusercontent.com/NVIDIA/aistore/master/cmd/cli/autocomplete/zsh
curl -Lo autocomplete.sh https://raw.githubusercontent.com/NVIDIA/aistore/master/cmd/cli/autocomplete/install.sh

echo "Enabling autocomplete..."
chmod +x autocomplete.sh
./autocomplete.sh
popd
