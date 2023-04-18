# Daily Knowledge
## Day 1
### Spark DataFrame
#### createDataFrame
- Schema: 
  - When schema is a list of column names, the type of each column will be inferred from data.
  - When schema is `None`, it will try to **infer the schema** (column names and types) from data, which should be an RDD of either Row, namedtuple, or dict.

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

# create Spark DataFrame from RDD
rdd = sc.parallelize([('Alice', 1)]) # spark context -> to create a parallelized session
spark_df = spark.createDataFrame(rdd, schema)
spark_df.collect()
# [Row(name='Alice', age=1)]

# create Spark DataFrame from Pandas DataFrame
spark_df = spark.createDataFrame(pandas_df, schema)
```

### UDF
#### Why UDF is Slow ?
- Py4J helps to connect the JVM & Python runtime, so if there are native or SQL spark transformation, they will be executed in JVM.
- Howerver, UDF code cannot be executed in JVM, because the UDF code is python-based, so only can run in Python runtime of the worker nodes and return the data to JVM
  - Therefore, each row of the data will be serialized and sent to Python runtime to transformed via UDF and return to JVM

Solution: Apache Arrow, or write UDF in native languages (Scala or Java)
<p align="center">
<img src="https://user-images.githubusercontent.com/64508435/225969155-fa353902-c5d4-4984-a5aa-35b9104b8950.png" width=400/></p>
