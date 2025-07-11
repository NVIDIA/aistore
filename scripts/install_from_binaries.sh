#!/bin/bash

# Examples:
# $ install_from_binaries.sh --dstdir /tmp/www --release v1.3.23   # install a given version
# $ install_from_binaries.sh --dstdir /tmp/www                     # install latest release
# (more options below)

set -e

# Command line options and their respective defaults
tmpdir="/tmp" 		# temp directory, e.g. $HOME/tmp
dstdir="/usr/local/bin"	# installation destination
completions="false"	# install and enable _only_ CLI autocompletions (ie., skip installing binaries)
release="latest" 	# e.g.: v1.3.15, v1.3.16, latest (default: latest)

script=$(basename $0)

SUDO=sudo
[[ $(id -u) == 0 ]] && SUDO=""

usage="NAME:
  $script - install 'ais' (CLI) and 'aisloader' from release binaries

USAGE:
  ./$script [options...]

OPTIONS:
  --tmpdir <dir>  	work directory, e.g. $HOME/tmp
  --dstdir <dir>  	installation destination
  --release      	release tag, e.g. v1.3.22, v1.3.23, latest (default: latest)
  --completions		install and enable _only_ CLI autocompletions (ie., skip installing binaries)
  -h, --help      	show this help
"

while (( "$#" )); do
  case "${1}" in
    -h|--help) echo -n "${usage}"; exit;;

    --tmpdir) tmpdir=$2; shift; shift;;
    --dstdir) dstdir=$2; shift; shift;;
    --release) release=$2; shift; shift;;
    --completions) completions="true"; shift;;
    *) echo "fatal: unknown argument '${1}'"; exit 1;;
  esac
done

cleanup() {
  rc=$?
  popd >/dev/null
  if [[ ${rc} == 0 ]]; then
    echo
    echo "Done."
  fi
  rm -rf $tmp_dir
  exit $rc
}

install_completions() {
  echo "Downloading CLI autocompletions (bash & zsh)..."
  curl -Lo bash https://raw.githubusercontent.com/NVIDIA/aistore/main/cmd/cli/autocomplete/bash
  curl -Lo zsh https://raw.githubusercontent.com/NVIDIA/aistore/main/cmd/cli/autocomplete/zsh

  echo "NOTE:"

  #
  # NOTE: cmd/cli/autocomplete/install.sh provides for zsh completions and more options.
  #

  source ./bash
  $SUDO cp ./bash /etc/bash_completion.d/ais
  if [[ $? -eq 0 ]]; then
    echo "   *** CLI autocompletions are now copied to /etc/bash_completion.d/ais ***"
    echo "   *** To enable, simply run: source /etc/bash_completion.d/ais         ***"
  fi
}

if [ ! -w "$tmpdir" ]; then
  echo "$tmpdir is not writable - exiting"
  exit 1
fi

tmp_dir=$(mktemp -d -t ais-cli-XXXXXXX --tmpdir=$tmpdir) || exit $?
pushd $tmp_dir >/dev/null

trap cleanup EXIT

if [[ ${completions} == "true" ]]; then
  install_completions
  exit 0
fi

if [ ! -w "$dstdir" ]; then
  echo "$dstdir is not writable - exiting"
  exit 1
fi


os=$(uname -s | tr '[:upper:]' '[:lower:]')
arch=$(uname -m)
case "$arch" in
  x86_64) arch="amd64" ;;
  arm64|aarch64) arch="arm64" ;;
  *) echo "Unsupported architecture: $arch"; exit 1 ;;
esac

echo "Detected OS: $os, Architecture: $arch"

echo "Installing aisloader => $dstdir/aisloader"
case ${release} in
  latest|"")
    curl -LO https://github.com/NVIDIA/aistore/releases/latest/download/aisloader-${os}-${arch}.tar.gz
    tar -xzvf aisloader-${os}-${arch}.tar.gz
    ;;
  v1.3.15|3.13)
    curl -LO https://github.com/NVIDIA/aistore/releases/download/v1.3.15/aisloader-${os}-${arch}.tar.gz
    tar -xzvf aisloader-${os}-${arch}.tar.gz
    ;;
  *)
    curl -LO https://github.com/NVIDIA/aistore/releases/download/$release/aisloader-${os}-${arch}.tar.gz
    tar -xzvf aisloader-${os}-${arch}.tar.gz
    ;;
esac
$SUDO mv ./aisloader $dstdir/.

echo "Installing CLI => $dstdir/ais"
case ${release} in
  latest|"")
    curl -LO https://github.com/NVIDIA/aistore/releases/latest/download/ais-${os}-${arch}.tar.gz
    tar -xzvf ais-${os}-${arch}.tar.gz
    ;;
  v1.3.15|3.13)
    curl -LO https://github.com/NVIDIA/aistore/releases/download/v1.3.15/ais-${os}-${arch}.tar.gz
    tar -xzvf ais-${os}-${arch}.tar.gz
    ;;
  *)
    curl -LO https://github.com/NVIDIA/aistore/releases/download/$release/ais-${os}-${arch}.tar.gz
    tar -xzvf ais-${os}-${arch}.tar.gz
    ;;
esac
$SUDO mv ./ais $dstdir/.

install_completions
