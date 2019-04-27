"""Spark related utils."""
#!/usr/bin/env python

import colored_glog as glog


def cache_and_log(rdd_name, rdd, show_items=1, info_func=glog.info, warn_func=glog.warn):
    """Cache and pretty log an RDD, then return the rdd itself."""
    rdd = rdd.cache()
    count = rdd.count()
    if count == 0:
        warn_func('{} is empty!'.format(rdd_name))
    elif show_items == 0:
        info_func('{} has {} elements.'.format(rdd_name, count))
    elif show_items == 1:
        info_func('{} has {} elements: [{}, ...]'.format(rdd_name, count, rdd.first()))
    else:
        info_func('{} has {} elements: [{}, ...]'.format(rdd_name, count,
                                                         ', '.join(rdd.take(show_items))))
    return rdd
