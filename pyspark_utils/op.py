"""The missing PySpark operators."""
#!/usr/bin/env python

def do_key(func):
    """Return a function which transforms the key."""
    return (lambda kv: (func(kv[0]), kv[1]))

def do_tuple(func):
    """Return a function which transforms a tuple."""
    return (lambda kv: func(*kv))

def filter_key(func):
    """Return a function which filters the key."""
    return (lambda kv: func(kv[0]))

def filter_value(func):
    """Return a function which filters the value."""
    return (lambda kv: func(kv[1]))

def swap_kv():
    """An operator to swap a key-value tuple."""
    return (lambda kv: (kv[1], kv[0]))
