#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

AUTOCOMPLETE_DIR_BASH="/usr/share/bash-completion/completions"
AUTOCOMPLETE_FILE_BASH="${AUTOCOMPLETE_DIR_BASH}/ais"

AUTOCOMPLETE_DIR_ZSH="/usr/share/zsh/vendor-completions"
AUTOCOMPLETE_FILE_ZSH="${AUTOCOMPLETE_DIR_ZSH}/_ais"

BASH_AUTOCOMPLETE_SOURCE_FILE="${DIR}/bash"
ZSH_AUTOCOMPLETE_SOURCE_FILE="${DIR}/zsh"

echo "*** Installing AIS CLI autocompletions into:"
echo "***     ${AUTOCOMPLETE_DIR_BASH} and"
echo "***     ${AUTOCOMPLETE_DIR_ZSH}"
echo "*** You can always uninstall autocompletions by running:"
echo "***     ${DIR}/uninstall.sh"
echo "*** To enable autocompletions in your current shell, run:"
echo "***     source ${BASH_AUTOCOMPLETE_SOURCE_FILE} or"
echo "***     source ${ZSH_AUTOCOMPLETE_SOURCE_FILE}"
echo "*** To enable them in all future shells, copy the corresponding line (above)"
echo "*** into your .bashrc and/or .zshrc"
echo "***"
read -r -p "Proceed? [Y/n] " response
case "$response" in
    [yY]|"")
        [[ -d ${AUTOCOMPLETE_DIR_BASH} ]] && sudo cp ${BASH_AUTOCOMPLETE_SOURCE_FILE} ${AUTOCOMPLETE_FILE_BASH}
        if [[ $? -ne 0 ]]; then
            echo "Bash completions not installed (some error occurred)."
	    exit 1
        fi

        [[ -d ${AUTOCOMPLETE_DIR_ZSH} ]] && sudo cp ${ZSH_AUTOCOMPLETE_SOURCE_FILE} ${AUTOCOMPLETE_FILE_ZSH}
        if [[ $? -eq 0 ]]; then
            rm ~/.zcompdump* &> /dev/null # Sometimes needed for zsh users (see: https://github.com/robbyrussell/oh-my-zsh/issues/3356)
        else
            echo "Zsh completions not installed (some error occurred)."
	    exit 1
        fi
        echo "Done."
        ;;
esac
