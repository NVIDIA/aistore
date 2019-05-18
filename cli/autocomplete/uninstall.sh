#!/bin/bash

AUTOCOMPLETE_DIR_BASH="/usr/share/bash-completion/completions"
AUTOCOMPLETE_FILE_BASH="${AUTOCOMPLETE_DIR_BASH}/ais"

AUTOCOMPLETE_DIR_ZSH="/usr/share/zsh/vendor-completions"
AUTOCOMPLETE_FILE_ZSH="${AUTOCOMPLETE_DIR_ZSH}/_ais"

echo "*** Uninstalling shell completions..."
echo "***"
echo "*** You can also uninstall them manually by running:"
echo "***     \`sudo rm ${AUTOCOMPLETE_FILE_BASH} && sudo rm ${AUTOCOMPLETE_FILE_ZSH}\`".
echo "***"
read -r -p "Proceed? [Y/n] " response
case "$response" in
    [yY]|"")
        [[ -f ${AUTOCOMPLETE_FILE_BASH} ]] && sudo rm ${AUTOCOMPLETE_FILE_BASH}
        [[ -f ${AUTOCOMPLETE_FILE_ZSH} ]] && sudo rm ${AUTOCOMPLETE_FILE_ZSH}
        rm ~/.zcompdump* &> /dev/null # Sometimes needed for zsh users (see: https://github.com/robbyrussell/oh-my-zsh/issues/3356)

        echo "Autocompletions uninstalled."
        ;;
esac
