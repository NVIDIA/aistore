import sys

from tabulate import tabulate

from colorama import Back, Fore, Style

from pyaisloader.const import BOLD, END, UNDERLINE
from pyaisloader.utils.parse_utils import format_size, format_time


def bold(msg):
    return f"{BOLD}{msg}{END}"


def underline(msg):
    return f"{UNDERLINE}{msg}{END}"


def print_sep():
    print("\n" + "=" * 101 + "\n")


def print_in_progress(msg, icon="\U0001f552"):
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
        + msg,
        file=sys.stderr,
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


def print_results(result):
    headers_values = [
        ("# Ops Completed", result["ops"]),
        ("Total Size", format_size(result["bytes"])),
        ("Throughput", f"{format_size(result['throughput'])}/s"),
        ("Latency Min", format_time(result["latency_min"])),
        ("Latency Avg", format_time(result["latency_avg"])),
        ("Latency Max", format_time(result["latency_max"])),
    ]
    table = [
        [f"{BOLD}{name}{END}" for name, _ in headers_values],
        [value for _, value in headers_values],
    ]
    print("\n" + tabulate(table, tablefmt="simple_grid") + "\n")


def confirm(msg, assume_yes=False):
    if assume_yes:
        return True
    while True:
        print(f"\n{msg}{bold('(Y/N)')}: ", end="", file=sys.stderr, flush=True)
        decision = input().lower()
        if decision in ["y", "yes"]:
            return True
        if decision in ["n", "no"]:
            return False


def confirm_continue(assume_yes=False):
    if not confirm("Would you like to proceed? ", assume_yes=assume_yes):
        terminate(code=0)


def terminate(msg=None, code=1):
    if msg:
        print(
            "\n"
            + Back.LIGHTRED_EX
            + Fore.BLACK
            + bold("TERMINATING")
            + Style.RESET_ALL
            + bold(": ")
            + msg
            + "\n",
            file=sys.stderr,
        )
    else:
        print(
            "\n"
            + Back.LIGHTRED_EX
            + Fore.BLACK
            + bold("TERMINATING")
            + Style.RESET_ALL
            + bold("...")
            + "\n",
            file=sys.stderr,
        )
    sys.exit(code)
