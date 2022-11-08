#!/bin/bash
set -e

# Command line options and their respective defaults
tmpdir="/tmp" 			# temp directory, e.g. $HOME/tmp
dstdir="/usr/local/bin"		# installation destination
previous="false"        	# install from the release _prior_ to the latest
completions="false"        	# install and enable _only_ CLI autocompletions (skip installing binaries)

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
  --previous      	install from the release _prior_ to the latest
  --completions		install and enable _only_ CLI autocompletions (skip installing binaries)
  -h, --help      	show this help
"

while (( "$#" )); do
  case "${1}" in
    -h|--help) echo -n "${usage}"; exit;;

    --tmpdir) tmpdir=$2; shift; shift;;
    --dstdir) dstdir=$2; shift; shift;;
    --previous) previous="true"; shift;;
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
  curl -Lo bash https://raw.githubusercontent.com/NVIDIA/aistore/master/cmd/cli/autocomplete/bash
  curl -Lo zsh https://raw.githubusercontent.com/NVIDIA/aistore/master/cmd/cli/autocomplete/zsh

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


echo "Installing aisloader => $dstdir/aisloader"
if [[ ${previous} == "true" ]]; then
  reltag="3.11"
  curl -Lo aisloader https://github.com/NVIDIA/aistore/releases/download/$reltag/aisloader-linux-amd64
  chmod +x aisloader
else
  reltag="v1.3.12"
  curl -LO https://github.com/NVIDIA/aistore/releases/download/$reltag/aisloader-linux-amd64.tar.gz
  tar -xzvf aisloader-linux-amd64.tar.gz
fi
$SUDO mv ./aisloader $dstdir/.

echo "Installing CLI => $dstdir/ais"
if [[ ${previous} == "true" ]]; then
  reltag="3.11"
  curl -Lo ais https://github.com/NVIDIA/aistore/releases/download/$reltag/ais-linux-amd64
  chmod +x ais
else
  reltag="v1.3.12"
  curl -LO https://github.com/NVIDIA/aistore/releases/download/$reltag/ais-linux-amd64.tar.gz
  tar -xzvf ais-linux-amd64.tar.gz
fi
$SUDO mv ./ais $dstdir/.

install_completions
