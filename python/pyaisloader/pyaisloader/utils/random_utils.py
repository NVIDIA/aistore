import os
import random
import string


def generate_random_str():
    chars = string.ascii_lowercase + string.digits
    return "".join(random.choice(chars) for _ in range(5))


def generate_bytes(min_size, max_size):
    size = random.randint(min_size, max_size)
    content = os.urandom(size)
    return content, size
