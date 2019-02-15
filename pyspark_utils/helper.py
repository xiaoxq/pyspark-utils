"""Spark related utils."""
#!/usr/bin/env python

import pyspark


CURRENT_CONTEXT = None

def get_context(app_name='SparkJob'):
    """Get new Spark context."""
    global CURRENT_CONTEXT
    if CURRENT_CONTEXT is None:
        conf = pyspark.SparkConf().setAppName(app_name)
        CURRENT_CONTEXT = pyspark.SparkContext(conf=conf)
    return CURRENT_CONTEXT
