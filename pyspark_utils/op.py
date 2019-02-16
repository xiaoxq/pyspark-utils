"""The missing PySpark operators."""
#!/usr/bin/env python

import operator


def do_key(func):
    """Return a function which transforms the key."""
    return (lambda kv: (func(kv[0]), kv[1]))

def do_tuple(func):
    """Return a function which transforms a tuple."""
    return (lambda a_tuple: func(*a_tuple))

def filter_key(func):
    """Return a function which filters the key."""
    return (lambda kv: func(kv[0]))

def filter_value(func):
    """Return a function which filters the value."""
    return (lambda kv: func(kv[1]))

def identity(elem):
    """Return the argument itself."""
    return elem

def filter_keys(pair_rdd, key_rdd):
    """
    Only keep entries of pair_rdd whose key is in the key_rdd.
    The given key_rdd should be distinct.
    """
    return pair_rdd.join(key_rdd.keyBy(identity)).mapValues(operator.itemgetter(0))

def substract_keys(pair_rdd, key_rdd):
    """Remove entries of pair_rdd whose key is in the key_rdd."""
    return pair_rdd.subtractByKey(key_rdd.keyBy(identity))

def swap_kv(kv):
    """Swap a key-value tuple."""
    return (kv[1], kv[0])
