#!/bin/bash
print_error() {
  echo "Error: $1."
  exit 1
}

is_number() {
  if ! [[ "$1" =~ ^[0-9]+$ ]] ; then
    print_error "'$1' is not a number"
  fi
}

is_boolean() {
  if ! [[ "$1" =~ ^[yn]+$ ]] ; then
    print_error "input is not a 'y' or 'n'"
  fi
}

parse_cld_providers() {
  AIS_CLD_PROVIDERS=""
  echo "Select the cloud providers you wish to support:"
  echo "Amazon S3: (y/n) ? "
  read  CLD_AWS
  is_boolean $CLD_AWS
  echo "Google Cloud Storage: (y/n) ? "
  read  CLD_GCP
  is_boolean $CLD_GCP
  echo "Azure: (y/n) ? "
  read  CLD_AZURE
  is_boolean $CLD_AZURE
  if  [[ "$CLD_AWS" == "y" ]] ; then
    AIS_CLD_PROVIDERS="${AIS_CLD_PROVIDERS} aws"
  fi
  if  [[ "$CLD_GCP" == "y" ]] ; then
    AIS_CLD_PROVIDERS="${AIS_CLD_PROVIDERS} gcp"
  fi
  if  [[ "$CLD_AZURE" == "y" ]] ; then
    AIS_CLD_PROVIDERS="${AIS_CLD_PROVIDERS} azure"
  fi
}

is_command_available() {
  if [[ -z $(command -v "$1") ]]; then
    print_error "command '$1' not available"
  fi
}

run_cmd() {
  set -x
  $@ &
  { set +x; } 2>/dev/null
}


