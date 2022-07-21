import signal

# WARNING!! This timeout mechanism only works on Unix systems!
# But since this test suite is meant to be run as part of the Jenkins promotion process on a CentOS system, that's fine.
# Just keep in mind that while the tests run perfectly on your local Windows workstation, they won't fail in case of a
# timeout.


class TimeoutException(Exception):
    pass


# noinspection PyUnusedLocal
def raise_timeout_exception(signum, frame):
    raise TimeoutException("timeout")


# pylint: disable=unused-variable
def timeout(timeout_in_seconds=300):
    """
    A timeout decorator for conveniently adding a fail-safe timeout to any function definition.
    This decorator uses the SIGALRM signal which is only supported on Unix systems. When using this decorator under
    Windows, the TimeoutException will *not* get raised!
    :param timeout_in_seconds: Time after which a TimeoutException is raised (defaults to 300)
    :return: The decorated function
    """

    def decorator(fn):
        def f(*args, **kwargs):
            prev_sig_handler = signal.signal(signal.SIGALRM, raise_timeout_exception)
            signal.alarm(timeout_in_seconds)
            try:
                result = fn(*args, **kwargs)
            finally:
                signal.alarm(0)
                signal.signal(signal.SIGALRM, prev_sig_handler)
            return result

        return f

    return decorator


# pylint: disable=unused-variable
def object_variable_timeout(timeout_variable):
    """
    A version of the timeout decorator for objects, which define their timeouts as object variables and therefore cannot
    be supplied as decorator arguments at the time of object creation.
    :param: timeout_variable: name of the object member variable specifying timeout duration
    :return: The decorated function
    """

    def decorator(fn):
        def f(self, *args, **kwargs):
            prev_sig_handler = signal.signal(signal.SIGALRM, raise_timeout_exception)
            timeout_in_seconds = getattr(self, timeout_variable)
            if not timeout_in_seconds:
                return fn(*args, **kwargs)
            signal.alarm(timeout_in_seconds)
            try:
                result = fn(self, *args, **kwargs)
            finally:
                signal.alarm(0)
                signal.signal(signal.SIGALRM, prev_sig_handler)
            return result

        return f

    return decorator
