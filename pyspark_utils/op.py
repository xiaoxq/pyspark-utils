"""The missing PySpark operators."""
#!/usr/bin/env python

import operator

import colored_glog as glog


def do_key(func):
    """Return a function which transforms the key."""
    return lambda kv: (func(kv[0]), kv[1])

def do_tuple(func):
    """Return a function which transforms a tuple."""
    return lambda a_tuple: func(*a_tuple)

def do_tuple_elems(func):
    """Return a function which transforms all elements of the input."""
    return lambda elems: (func(elem) for elem in elems)

def do_list_elems(func):
    """Return a function which transforms all elements of the input."""
    return lambda elems: [func(elem) for elem in elems]

def filter_key(func):
    """Return a function which filters the key."""
    return lambda kv: func(kv[0])

def filter_value(func):
    """Return a function which filters the value."""
    return lambda kv: func(kv[1])

def filter_path(patterns):
    """Return a function which filters paths by fnmatch style patterns."""
    return lambda path: next((True for pat in patterns if fnmatch.fnmatch(path, pat)), False)

def identity(elem):
    """Return the argument itself."""
    return elem

def substract_keys(pair_rdd, key_rdd):
    """Remove entries of pair_rdd whose key is in the key_rdd."""
    return pair_rdd.subtractByKey(key_rdd.keyBy(identity))

def filter_keys(pair_rdd, whitelist_key_rdd, blacklist_key_rdd=None):
    """
    Only keep entries of pair_rdd whose key is in the whitelist and not in the blacklist.
    The given keys rdd should be distinct.
    """
    pair_rdd = pair_rdd.join(whitelist_key_rdd.keyBy(identity)).mapValues(operator.itemgetter(0))
    if blacklist_key_rdd is not None:
        pair_rdd = pair_rdd.subtractByKey(blacklist_key_rdd.keyBy(identity))
    return pair_rdd

def swap_kv(kv):
    """Swap a key-value tuple."""
    return (kv[1], kv[0])

def not_none(elem):
    """Check if an element is not None."""
    return elem is not None

def value_by(func):
    """Return a function which generate value from the element."""
    return lambda key: (key, func(key))

def cache_and_log(rdd_name, rdd, log_func=glog.info):
    """Cache and pretty log an RDD, then return the rdd itself."""
    rdd = rdd.cache()
    elem_count = rdd.count()
    if elem_count > 0:
        log_func('{} has {} elements: [{}, ...]'.format(rdd_name, elem_count, rdd.first()))
    else:
        log_func('{} has 0 elements'.format(rdd_name))
    return rdd

# To be retired.
def log_rdd(rdd, rdd_name='RDD', log_func=None):
    """Pretty log an RDD, then return the rdd itself."""
    rdd = rdd.cache()
    elem_count = rdd.count()
    msg = '{} has {} elements: [{}, ...]'.format(
        rdd_name, elem_count, rdd.first() if elem_count > 0 else 'None')
    if log_func is not None:
        log_func(msg)
    else:
        print (msg)
    return rdd
