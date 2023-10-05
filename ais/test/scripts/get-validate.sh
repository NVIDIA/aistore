#!/bin/bash

## Example usage:
## ./ais/test/scripts/get-validate.sh --bucket s3://abc

lorem='Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.'
duis='Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum. Et harum quidem..'

## Command line options (and their respective defaults)
bucket="s3://abc"

while (( "$#" )); do
  case "${1}" in
    --bucket) bucket=$2; shift; shift;;
    --nocleanup) nocleanup="true"; shift;;
    *) echo "fatal: unknown argument '${1}'"; exit 1;;
  esac
done

## establish existence
ais show bucket $bucket -c 1>/dev/null || exit $?

## PUT out-of-band, and cold GET
echo $lorem | s3cmd put - "$bucket/lorem" 1>/dev/null
ais get "$bucket/lorem" /dev/null 1>/dev/null
ais ls "$bucket/lorem" --cached 
