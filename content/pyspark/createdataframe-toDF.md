---
title: "Creating PySpark DataFrames"
date: 2021-02-23T22:00:00-05:00
draft: false
---

# Creating PySpark DataFrames

There are a few ways to manually create PySpark DataFrames:

* createDataFrame
* create_df
* toDF

This post shows the different ways to create DataFrames and explains when the different approaches are advantageous.

## createDataFrame

Here's how to create a DataFrame with `createDataFrame`:

```python
df = spark.createDataFrame([("joe", 34), ("luisa", 22)], ["first_name", "age"])

df.show()
```

```
+----------+---+
|first_name|age|
+----------+---+
|       joe| 34|
|     luisa| 22|
+----------+---+
```

Check out the DataFrame schema with `df.printSchema()`:

```
root
 |-- first_name: string (nullable = true)
 |-- age: long (nullable = true)
```

You can also pass `createDataFrame` a RDD and schema to construct DataFrames with more precision:

```python
from pyspark.sql import Row
from pyspark.sql.types import *

rdd = spark.sparkContext.parallelize([
    Row(name='Allie', age=2),
    Row(name='Sara', age=33),
    Row(name='Grace', age=31)])

schema = schema = StructType([
   StructField("name", StringType(), True),
   StructField("age", IntegerType(), False)])

df = spark.createDataFrame(rdd, schema)

df.show()
```

```
+-----+---+
| name|age|
+-----+---+
|Allie|  2|
| Sara| 33|
|Grace| 31|
+-----+---+
```

Run `df.printSchema()` to verify the schema is exactly as specified:

```
root
 |-- name: string (nullable = true)
 |-- age: integer (nullable = false)
```

`createDataFrame` is nice because it allows for terse syntax (with limited schema control) or verbose syntax (with full schema control).

Let's look at another option that's not quite as verbose as `createDataFrame`, but with the level of fine-grained control.

## create_df

The `create_df` method defined in [quinn](https://github.com/MrPowers/quinn) allows for precise schema definition when creating DataFrames.

```python
from pyspark.sql.types import *
from quinn.extensions import *

df = spark.create_df(
    [("jose", "a"), ("li", "b"), ("sam", "c")],
    [("name", StringType(), True), ("blah", StringType(), True)]
)

df.show()
```

```
+----+----+
|name|blah|
+----+----+
|jose|   a|
|  li|   b|
| sam|   c|
+----+----+
```

Run `df.printSchema()` to confirm the schema is exactly as specified:

```
root
 |-- name: string (nullable = true)
 |-- blah: string (nullable = true)
```

`create_df` is generally the best option in your test suite.

See [here](https://mungingdata.com/pyspark/testing-pytest-chispa/) for more information on testing PySpark code.

## toDF

You can also create a RDD and convert it to a DataFrame with `toDF`:

```python
from pyspark.sql import Row

rdd = spark.sparkContext.parallelize([
    Row(name='Allie', age=2),
    Row(name='Sara', age=33),
    Row(name='Grace', age=31)])
df = rdd.toDF()
df.show()
```

```
+-----+---+
| name|age|
+-----+---+
|Allie|  2|
| Sara| 33|
|Grace| 31|
+-----+---+
```

It's usually easier to use `createDataFrame` than `toDF`.

## Conclusion

There are multiple ways to manually create PySpark DataFrames.

`create_df` is the best when you're working in a test suite and can easily add an external dependency.

For quick experimentation, use `createDataFrame`.

