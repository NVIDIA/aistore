#!/bin/bash
set -e

echo "Downloading binary installer..."
curl -Lo bin_installer.sh https://raw.githubusercontent.com/NVIDIA/aistore/master/cmd/bin_installer.sh
chmod +x bin_installer.sh
./bin_installer.sh ais

echo "Creating a temporary directory..."
tmpdir=$(mktemp -d)
pushd $tmpdir


echo "Downloading autocomplete scripts..."
curl -Lo bash https://raw.githubusercontent.com/NVIDIA/aistore/master/cmd/cli/autocomplete/bash
curl -Lo zsh https://raw.githubusercontent.com/NVIDIA/aistore/master/cmd/cli/autocomplete/zsh
curl -Lo autocomplete.sh https://raw.githubusercontent.com/NVIDIA/aistore/master/cmd/cli/autocomplete/install.sh

echo "Enabling autocomplete..."
chmod +x autocomplete.sh
./autocomplete.sh
popd
