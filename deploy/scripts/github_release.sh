#!/bin/bash
set -ex

function upload() {
  filename=$1
  upload_name=$2
  cksum="/tmp/${upload_name}.sha256"

  echo "Computing checksum..."
  sha256sum $filename > ${cksum}

  echo "Uploading release asset: ${upload_name}"
  GH_ASSET="https://uploads.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}/releases/${GITHUB_RELEASE_ID}/assets?name=$upload_name"
  curl --progress-bar -H "Authorization: token ${GITHUB_OAUTH_TOKEN}" -H "Content-Type: application/octet-stream" $GH_ASSET -T "$filename" | jq

  echo "Uploading asset checskum: ${upload_name}.sha256"
  curl --progress-bar -H "Authorization: token ${GITHUB_OAUTH_TOKEN}" -H "Content-Type: application/octet-stream" "${GH_ASSET}.sha256" -T "$cksum" | jq
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
make aisfs
make aisloader
popd

echo "Checking if jq is installed"
if ! command -v jq 2>&1 /dev/null; then
  sudo apt-get install jq
fi

echo "Checking if sha256sum is installed"
if ! command -v sha256sum 2>&1 /dev/null; then
  sudo apt-get install coreutils
fi


upload "${GOPATH}/bin/ais" "ais-${os}-${arch}"
upload "${GOPATH}/bin/aisfs" "aisfs-${os}-${arch}"
upload "${GOPATH}/bin/aisloader" "aisloader-${os}-${arch}"

