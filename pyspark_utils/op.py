#!/usr/bin/env python
"""The missing PySpark operators."""

def map_key(func):
    """Return a function which transforms the key."""
    def func_wrapper(key_value):
        """The function which transforms the key."""
        key, value = key_value
        return func(key), value
    return func_wrapper

def filter_key(func):
    """Return a function which filters the key."""
    def func_wrapper(key_value):
        """The function which filters the key."""
        key, _ = key_value
        return func(key)
    return func_wrapper

def filter_value(func):
    """Return a function which filters the value."""
    def func_wrapper(key_value):
        """The function which filters the value."""
        _, value = key_value
        return func(value)
    return func_wrapper
