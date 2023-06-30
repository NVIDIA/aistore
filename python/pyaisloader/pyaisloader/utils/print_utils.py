from colorama import Back, Fore, Style

from pyaisloader.const import BOLD, END, UNDERLINE
from pyaisloader.utils.cli_utils import terminate


def bold(msg):
    return f"{BOLD}{msg}{END}"


def underline(msg):
    return f"{UNDERLINE}{msg}{END}"


def print_sep():
    print("\n" + "=" * 101)


def confirm_continue():
    decision = input("\n" + "Would you like to proceed? " + bold("(Y/N)") + ": ")
    if decision.lower() in ["n", "no"]:
        terminate()
    elif decision.lower() in ["y", "yes"]:
        return
    else:
        confirm_continue()


def print_in_progress(msg, icon="\U0001F552"):
    print(
        "\n"
        + Back.LIGHTBLACK_EX
        + Fore.BLACK
        + bold("IN PROGRESS")
        + Style.RESET_ALL
        + bold(": ")
        + msg
        + f" {icon}"
    )


def print_caution(msg):
    print(
        "\n"
        + Back.LIGHTYELLOW_EX
        + Fore.BLACK
        + bold("CAUTION")
        + Style.RESET_ALL
        + bold(": ")
        + msg
    )


def print_success(msg):
    print(
        "\n"
        + Back.LIGHTGREEN_EX
        + Fore.BLACK
        + bold("SUCCESS")
        + Style.RESET_ALL
        + bold(": ")
        + msg
        + Fore.GREEN
        + " \u2714"
        + Style.RESET_ALL
    )
