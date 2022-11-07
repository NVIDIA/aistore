#!/bin/bash
set -ex

function upload() {
  bin_name=$1
  upload_name=$2

  echo "Building ${upload_name}..."
  pushd $GOPATH/bin
  tar -czvf "/tmp/${upload_name}" $bin_name || exit 1
  popd

  echo "Computing checksum..."
  pushd /tmp
  cksum="${upload_name}.sha256"
  sha256sum  "${upload_name}" > ${cksum} || exit 1
  popd

  echo "Uploading release asset: ${upload_name}"
  GH_ASSET="https://uploads.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}/releases/${GITHUB_RELEASE_ID}/assets?name=${upload_name}"
  curl --progress-bar -H "Authorization: token ${GITHUB_OAUTH_TOKEN}" -H "Content-Type: application/octet-stream" $GH_ASSET -T "/tmp/${upload_name}" | jq

  echo "Uploading the asset's checksum: ${upload_name}.sha256"
  GH_CHECKSUM="https://uploads.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}/releases/${GITHUB_RELEASE_ID}/assets?name=${upload_name}.sha256"
  curl --progress-bar -H "Authorization: token ${GITHUB_OAUTH_TOKEN}" -H "Content-Type: application/octet-stream" $GH_CHECKSUM -T "/tmp/${cksum}" | jq
}

GITHUB_OWNER="NVIDIA"
GITHUB_REPO="aistore"
echo "Fetching release id for the release tag: ${GITHUB_RELEASE_TAG}"
GITHUB_RELEASE_ID=$(curl -H  "Authorization: token ${GITHUB_OAUTH_TOKEN}" "https://api.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}/releases/tags/${GITHUB_RELEASE_TAG}" | jq '.id')

# Building the binaries
# TODO: add cross-platform support including macOS.
os="linux"
arch="amd64"
echo "Building binaries"
pushd ../../
make cli
make aisloader
make authn
popd

echo "Checking if jq is installed"
if ! command -v jq 2>&1 /dev/null; then
  sudo apt-get install jq
fi

echo "Checking if sha256sum is installed"
if ! command -v sha256sum 2>&1 /dev/null; then
  sudo apt-get install coreutils
fi


upload ais "ais-${os}-${arch}.tar.gz"
upload authn "authn-${os}-${arch}.tar.gz"
upload aisloader "aisloader-${os}-${arch}.tar.gz"
