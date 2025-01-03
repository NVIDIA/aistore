import humanfriendly
import pendulum


def parse_time(time_str):
    return humanfriendly.parse_timespan(time_str)


def parse_size(size_str):
    return humanfriendly.parse_size(size_str)


def format_time(duration):
    d = pendulum.duration(seconds=duration)

    if d.minutes > 0:
        return d.in_words()
    if d.seconds > 0:
        return f"{d.seconds} seconds"
    if d.microseconds > 0:
        if d.microseconds >= 1e6:
            return f"{d.microseconds/1e6} seconds"
        if d.microseconds >= 1e3:
            return f"{d.microseconds/1e3} milliseconds"
        return f"{d.microseconds} microseconds"

    return "0 seconds"


def format_size(byte_count):
    return humanfriendly.format_size(byte_count)
