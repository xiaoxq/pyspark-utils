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

spark_context = spark_helper.get_context('TestPipeline')
```

### op

```python
import pyspark_utils.op as spark_op

some_pair_rdd.map(spark_op.do_key(lambda key: result))
some_pair_rdd.map(spark_op.do_tuple(lambda key, value: result))
some_pair_rdd.map(spark_op.swap_kv())
some_pair_rdd.filter(spark_op.filter_key(lambda key: true_or_false))
some_pair_rdd.filter(spark_op.filter_value(lambda value: true_or_false))
```
