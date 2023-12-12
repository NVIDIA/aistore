#!/bin/bash
echo "Downloading uninstall scripts..."
echo "Creating a temporary directory..."
tmpdir=$(mktemp -d)
pushd $tmpdir

echo "Removing ais..."
sudo rm /usr/local/bin/ais

echo "Removing autocomplete..."
curl -Lo remove_autocomplete.sh https://raw.githubusercontent.com/NVIDIA/aistore/main/cmd/cli/autocomplete/uninstall.sh
chmod +x remove_autocomplete.sh
./remove_autocomplete.sh
popd
