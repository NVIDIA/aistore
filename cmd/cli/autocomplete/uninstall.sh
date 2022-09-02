#!/bin/bash

AUTOCOMPLETE_DIR_BASH="/usr/share/bash-completion/completions"
AUTOCOMPLETE_FILE_BASH="${AUTOCOMPLETE_DIR_BASH}/ais"

AUTOCOMPLETE_DIR_OH_MY_ZSH="$HOME/.oh-my-zsh/completions"
AUTOCOMPLETE_FILE_OH_MY_ZSH="${AUTOCOMPLETE_DIR_OH_MY_ZSH}/_ais"
AUTOCOMPLETE_DIR_ZSH="$HOME/.zsh/completion"
AUTOCOMPLETE_FILE_ZSH="${AUTOCOMPLETE_DIR_ZSH}/_ais"

SUDO=sudo
[[ $(id -u) == 0 ]] && SUDO=""

echo -n "Uninstalling AIS CLI autocompletions..."
[[ -f ${AUTOCOMPLETE_FILE_BASH} ]] && $SUDO rm ${AUTOCOMPLETE_FILE_BASH}
[[ -f ${AUTOCOMPLETE_FILE_ZSH} ]] && rm ${AUTOCOMPLETE_FILE_ZSH}
[[ -f ${AUTOCOMPLETE_FILE_OH_MY_ZSH} ]] && rm ${AUTOCOMPLETE_FILE_OH_MY_ZSH}
rm ~/.zcompdump* &> /dev/null # Sometimes needed for zsh users (see: https://github.com/robbyrussell/oh-my-zsh/issues/3356)
sleep 0.5
echo " Done"
