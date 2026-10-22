#!/bin/bash
set -ex

# --replace: overwrite same-named assets already on the release
clobber=()
if [[ "$1" == "--replace" ]]; then clobber=(--clobber); fi

# gh reads its token and target repo from the environment
export GH_TOKEN="${GITHUB_OAUTH_TOKEN}"
export GH_REPO="${GITHUB_OWNER}/${GITHUB_REPO}"

echo "Checking if gh is installed"
if ! command -v gh &> /dev/null; then
  echo "gh (https://cli.github.com) is required" >&2
  exit 1
fi

# sha256sum is absent on macOS
if command -v sha256sum &> /dev/null; then
  sha256="sha256sum"
else
  sha256="shasum -a 256"
fi

assets=()

function package() {
  bin_name=$1
  upload_name=$2

  echo "Building ${upload_name}..."
  pushd $GOPATH/bin
  tar -czvf "/tmp/${upload_name}" $bin_name || exit 1
  popd

  echo "Computing checksum..."
  pushd /tmp
  cksum="${upload_name}.sha256"
  $sha256 "${upload_name}" > ${cksum} || exit 1
  popd

  assets+=("/tmp/${upload_name}" "/tmp/${cksum}")
}

# Building the binaries
os=$(uname -s | tr '[:upper:]' '[:lower:]')
arch=$(uname -m)
if [[ "$arch" == "x86_64" ]]; then arch="amd64"; fi # normalize arch naming (x86_64 => amd64) for consistency

echo "Architecture set to: $arch"

package ais "ais-${os}-${arch}.tar.gz"
package aisloader "aisloader-${os}-${arch}.tar.gz"

echo "Uploading release assets: ${assets[*]}"
gh release upload "${GITHUB_RELEASE_TAG}" "${assets[@]}" "${clobber[@]}"
