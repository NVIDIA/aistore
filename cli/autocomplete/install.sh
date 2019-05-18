#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

AUTOCOMPLETE_DIR_BASH="/usr/share/bash-completion/completions"
AUTOCOMPLETE_FILE_BASH="${AUTOCOMPLETE_DIR_BASH}/ais"

AUTOCOMPLETE_DIR_ZSH="/usr/share/zsh/vendor-completions"
AUTOCOMPLETE_FILE_ZSH="${AUTOCOMPLETE_DIR_ZSH}/_ais"

BASH_AUTOCOMPLETE_SOURCE_FILE="${DIR}/bash"
ZSH_AUTOCOMPLETE_SOURCE_FILE="${DIR}/zsh"

echo "*** Installing autocompletions..."
echo "***"
echo "*** BEWARE: this step will install AIS CLI autocompletions into:"
echo "***     ${AUTOCOMPLETE_DIR_BASH} and"
echo "***     ${AUTOCOMPLETE_DIR_ZSH}."
echo "*** You can always uninstall them by running:"
echo "***     \`sudo bash ${DIR}/uninstall.sh\`."
echo "***"
echo "*** You can install them later by running:"
echo "***     \`sudo bash ${DIR}/install.sh\`."
echo "*** You can also enable them only for your current shell by running:"
echo "***     \`source ${BASH_AUTOCOMPLETE_SOURCE_FILE}\` or"
echo "***     \`source ${ZSH_AUTOCOMPLETE_SOURCE_FILE}\`".
echo "***"
read -r -p "Proceed? [Y/n] " response
case "$response" in
    [yY]|"")
        [[ -d ${AUTOCOMPLETE_DIR_BASH} ]] && sudo cp ${BASH_AUTOCOMPLETE_SOURCE_FILE} ${AUTOCOMPLETE_FILE_BASH}
        if [[ $? -eq 0 ]]; then
            echo "Bash completions installed. (You may need to restart your shell or \`source ${AUTOCOMPLETE_FILE_BASH}\` to use them)"
        else
            echo "Bash completions not installed (some error occurred)."
        fi

        [[ -d ${AUTOCOMPLETE_DIR_ZSH} ]] && sudo cp ${ZSH_AUTOCOMPLETE_SOURCE_FILE} ${AUTOCOMPLETE_FILE_ZSH}
        if [[ $? -eq 0 ]]; then
            rm ~/.zcompdump* &> /dev/null # Sometimes needed for zsh users (see: https://github.com/robbyrussell/oh-my-zsh/issues/3356)
            echo "Zsh completions installed. (You man need to restart your shell or run \`source ${AUTOCOMPLETE_FILE_ZSH}\` to use them)"
        else
            echo "Zsh completions not installed (some error occurred)."
        fi
        ;;
esac
