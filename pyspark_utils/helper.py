"""Spark related utils."""
#!/usr/bin/env python

import colored_glog as glog


def cache_and_log(rdd_name, rdd, items=1,
                  info_func=glog.info, warn_func=glog.warn):
    """Cache and pretty log an RDD, then return the rdd itself."""
    rdd = rdd.cache()
    elem_count = rdd.count()
    if elem_count > 0:
        info_func('{} has {} elements: [{}, ...]'.format(
            rdd_name, elem_count, rdd.take(items)))
    else:
        warn_func('{} is empty!'.format(rdd_name))
    return rdd
