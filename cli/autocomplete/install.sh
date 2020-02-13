#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

AUTOCOMPLETE_DIR_BASH="/usr/share/bash-completion/completions"
AUTOCOMPLETE_FILE_BASH="${AUTOCOMPLETE_DIR_BASH}/ais"

AUTOCOMPLETE_DIR_ZSH="/usr/share/zsh/vendor-completions"
AUTOCOMPLETE_FILE_ZSH="${AUTOCOMPLETE_DIR_ZSH}/_ais"

BASH_AUTOCOMPLETE_SOURCE_FILE="${DIR}/bash"
ZSH_AUTOCOMPLETE_SOURCE_FILE="${DIR}/zsh"

SUDO=sudo
[[ $(id -u) == 0 ]] && SUDO=""

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
        if [[ -d ${AUTOCOMPLETE_DIR_BASH} ]]; then
            $SUDO cp ${BASH_AUTOCOMPLETE_SOURCE_FILE} ${AUTOCOMPLETE_FILE_BASH}
            [[ $? -ne 0 ]] && echo "Bash completions not installed (some error occurred)."
        else
            echo "Skipping bash completions - target directory absent"
        fi

        if [[ -d ${AUTOCOMPLETE_DIR_ZSH} ]]; then
            $SUDO cp ${ZSH_AUTOCOMPLETE_SOURCE_FILE} ${AUTOCOMPLETE_FILE_ZSH}
            if [[ $? -eq 0 ]]; then
                rm ~/.zcompdump* &> /dev/null # Sometimes needed for zsh users (see: https://github.com/robbyrussell/oh-my-zsh/issues/3356)
            else
                echo "Zsh completions not installed (some error occurred)."
            fi
        else
            echo "Skipping zsh completions - target directory absent"
        fi
        echo "Done."
        ;;
esac
