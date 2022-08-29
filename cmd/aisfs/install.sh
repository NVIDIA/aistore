#!/bin/bash

printError() {
  echo "Error: $1."
  exit 1
}

# Check if fusermount/umount is installed on the system
OS=$(uname -s)
case $OS in
  Linux) # Linux
    if ! [[ -x $(command -v fusermount) ]]; then
      printError "fusermount not found"
    fi
    ;;
  Darwin) # macOS
    if ! [[ -x $(command -v umount) ]]; then
      printError "umount not found"
    fi
    echo "WARNING: Darwin architecture is not yet fully supported. You may stumble upon bugs and issues when testing on Mac."
    ;;
  *)
    printError "'${OS}' is not supported"
    ;;
esac