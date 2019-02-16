# PySpark Utils

The missing PySpark utils.

## Usage

To install:

```bash
pip install pyspark-utils
```

### helper

```python
import pyspark_utils.helper as spark_helper

# A SparkContext named as "TestPipeline".
spark_context = spark_helper.get_context('TestPipeline')
```

### op

```python
import pyspark_utils.op as spark_op

# RDD<key, value>  ->  RDD<new_key, value>
pair_rdd.map(spark_op.do_key(lambda key: new_key))

# RDD<key, value>  ->  RDD<result>
pair_rdd.map(spark_op.do_tuple(lambda key, value: result))

# RDD<key, value>  ->  RDD<value, key>
pair_rdd.map(spark_op.swap_kv())

# RDD<key, value>  ->  RDD<key, value> if func(key)
pair_rdd.filter(spark_op.filter_key(lambda key: true_or_false))

# RDD<key, value>  ->  RDD<key, value> if func(value)
pair_rdd.filter(spark_op.filter_value(lambda value: true_or_false))

# RDD<element>  ->  RDD<element, element>
rdd.keyBy(spark_op.identity)

# RDD<key, value>   ->   RDD<key, value> with keys in key_rdd
subset_pair_rdd = spark_op.filter_keys(pair_rdd, key_rdd)

# RDD<key, value>   ->   RDD<key, value> with keys not in key_rdd
subset_pair_rdd = spark_op.substract_keys(pair_rdd, key_rdd)
```
