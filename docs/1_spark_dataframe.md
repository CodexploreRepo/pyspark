# Spark Data Frame
## createDataFrame
- Schema: 
  - When schema is a list of column names, the type of each column will be inferred from data.
  - When schema is `None`, it will try to **infer the schema** (column names and types) from data, which should be an *RDD of either Row, namedtuple, or dict*.

```Python
# create spark Session
spark = SparkSession.builder\
        .getOrCreate()
# create spark context
sc = spark.sparkContext

# define schema
schema = StructType([
   StructField("name", StringType(), True),
   StructField("age", IntegerType(), True)
 ])
```
- create Spark DataFrame from *RDD of either Row, namedtuple, or dict*
```Python
rdd = sc.parallelize([('Alice', 1)]) # spark context -> to create a parallelized session
spark_df = spark.createDataFrame(rdd, schema)
spark_df.collect()
# [Row(name='Alice', age=1)]
```
- create Spark DataFrame from Pandas DataFrame
```Python
# create Spark DataFrame from Pandas DataFrame
spark_df = spark.createDataFrame(pandas_df, schema)
```

