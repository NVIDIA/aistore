#!/bin/bash
set -e

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
