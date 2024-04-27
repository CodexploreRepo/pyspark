# Daily Knowledge

## Day 3

### How to store the Spark Dataframe

- **Method 1**: write the `spark_df` into S3 as `.parquet` format
  - Note: only need to specify the table name `your_table_name` without appending `.parquet` extension to the table name

```Python
table_path = "s3a://your_bucket_name/your_folder_path/your_table_name"
# write the spark dataframe to the underlying s3 storage location of the hive table "your_hive_table_name"
df.write \
  .mode("overwrite") \
  .partitionBy("partition_col1", "partition_col2") \
  .parquet(table_path)
```

- Method 2: if the Hive table exists, we can write the `spark_df` to the Hive table via the Hive table name and which Hive schema the table is belong to using `write.saveAsTable`

```Python
df.write \
  .mode("overwrite") \
  .partitionBy("partition_col1", "partition_col2") \
  .format("parquet") \
  .saveAsTable("your_database_name.your_hive_table_name")
```

## Day 2

### Hive

- **Hive** stores schema’s **metadata** in a database (such as Apache Derby, MySQL, Postgres or MariaDB) and processes data from connected storage (such as HDFS or S3).
  - It provides SQL-like query language, HiveQL.
- **Hive metastore** to store the `metadata` of **hive tables** and **table's partitions**
  - Hive metastore can have multiple schema (databases), and each schema (database) can contain multiple Hive table metadata.
- Hive tables: Hive supports two types of tables:
  - **Managed Tables (Internal Tables)**: these tables manage both the data and metadata in Hive's default warehouse directory (specified by `hive.metastore.warehouse.dir`
  - **External Tables**: these tables reference data files stored outside Hive, often in HDFS or S3.
    - Note :star: : Since the data files are not managed by Hive, so altering or dropping an external Hive table doesn't delete the underlying data. You might have to delete the data from the underlying HDSF or S3 location.
- Create Hive table: below is example of creating external Hive table via `spark.sql`. Read more on full documentation of creating Hive table [here](./docs/hive.md#how-to-create-hive-table)

```Python
spark.sql("""
    CREATE TABLE IF NOT EXISTS your_database_name.your_external_table_name (
        id INT, name STRING, age INT
    )
    PARTITIONED BY (country STRING, city STRING)
    STORED AS PARQUET
    LOCATION 'hdfs://your_hdfs_path/your_external_table_name/'
""")
```

### PySpark SQL `expr()` (Expression) Function

```Python
# Create a spark dataframe
data = [("James","M"),("Michael","F"),("Jen","")]
columns = ["name","gender"]
df = spark.createDataFrame(data = data, schema = columns)

from pyspark.sql.functions import expr
df2=df.withColumn("gender", expr("CASE WHEN gender = 'M' THEN 'Male' " +
           "WHEN gender = 'F' THEN 'Female' ELSE 'unknown' END"))
df2.show()
```

#### Stack operation in PySpark SQL using `expr()`

##### Unpivot columns into row

```Python
# original df
"""
+---+-----+-----+-----+
|id |team1|team2|team3|
+---+-----+-----+-----+
|1  |30   |300  |3000 |
|2  |50   |500  |5000 |
|3  |100  |1000 |10000|
|4  |200  |2000 |20000|
+---+-----+-----+-----+
"""
# Target df
"""
+---+---------+------+
| id|     team|points|
+---+---------+------+
|  1|team1_new|    30|
|  1|team2_new|   300|
|  1|team3_new|  3000|
|  2|team1_new|    50|
......................
+---+---------+------+
"""
# where 3 is total number of cols to be stacked into single row, in this case is 3 (team1, team2, team3)
unpivotExpr = "STACK(3, 'team1_new', team1, 'team2_new', team2, 'team3_new', team3) AS (team, points)"
unpivot_df = df.select('id', expr(unpivotExpr)) # to create 3 cols {'id', 'team' and 'points'}
```

## Day 1

### Spark DataFrame

#### `toPandas`

- Issue 1: Pyspark `.toPandas()` results in `object` column where expected **numeric one**
  - _Reason_: During the conversion, there is a coalesce of data types, such as `int/long -> int64`, `double -> float64`, `string -> obj`. For all unknown data types, it will be converted to obj type.
    - In Pandas data frame, there is no decimal data type, so all columns of decimal data type are converted to obj type.
  - _Solution_: casting `decimal` col to pandas-known data type, say `double` before toPandas()
  - ```Python
    from pyspark.sql.functions import *
    from pyspark.sql.types import *
    df = df.withColumn('AMD_4', col('AMD_4').cast(DoubleType()))
    pdf = df.toPandas()
    ```
- Issue 2: Converting pyspark DataFrame with date column to Pandas results in `AttributeError`

  - For the following pyspark DataFrame, if we toPandas() will return the error `AttributeError: Can only use .dt accessor with datetimelike values` as the `BUSINESS_DATE` has the type `date` in PySpark

  ```shell
  |-- BUSINESS_DATE: date (nullable = true)
  |-- ID: string (nullable = true)
  |-- A: double (nullable = true)
  |-- B: double (nullable = true)
  ```

  - _Solution_: converting your date column to `timestamp` (this is more aligned with pandas' `datetime` type)

  ```python
  from pyspark.sql.functions import to_timestamp
  # this will result in |2017-02-15 00:00:00|
  spark_df.withColumn('BUSINESS_DATE', to_timestamp(F.col('BUSINESS_DATE'), 'yyyy-MM-dd')).toPandas()
  # this will result in |2017-02-15|
  from pyspark.sql.functions import date_format
  spark_df.withColumn('BUSINESS_DATE', date_format(F.col('BUSINESS_DATE'), 'yyyy-MM-dd').toPandas()
  ```

  - The generalised solution:

  ```Python
  from pyspark.sql.functions import col

  def spark_type_converter(sdf):
      target_cols = [i for i,j in sdf.dtypes if j.startswith("date")]
      for col_name in target_cols:
          sdf = sdf.withColumn(col_name, date_format(F.col(col_name), 'yyyy-MM-dd'))
      return sdf

  ```

#### createDataFrame

##### Schema

- When creating Spark DataFrame from Pandas DataFrame, PySpark schema needs to be the same order with Pandas DataFrame
  - Order: PySpark schema does not map by the column names, but by the order of `StructField`s in the `StructType` list
- Create Spark from Numpy or Pandas Series:
  - Spark usually [struggles with **numpy** dtypes](https://stackoverflow.com/questions/66204342/typeerror-field-value-floattype-can-not-accept-object-0-016354798954796402-in)
    - For example `TypeError: field value: FloatType can not accept object -0.016354798954796402 in type <class 'numpy.float64'>`
    - So you can cast them from `Numpy dtype` to `Python dtype` before converting to a Spark dataframe, in this case, we convert `np.float64` to Python's `float`
  ```Python
  df = spark.createDataFrame(
    [float(x) for x in pd_series],
    FloatType()
  )
  ```
  - **Best Practise**: do not convert Numpy or Pandas Series to Spark DataFrame

### UDF

#### Why UDF is Slow ?

- Py4J helps to connect the JVM & Python runtime, so if there are native or SQL spark transformation, they will be executed in JVM.
- Howerver, UDF code cannot be executed in JVM, because the UDF code is python-based, so only can run in Python runtime of the worker nodes and return the data to JVM
  - Therefore, each row of the data will be serialized and sent to Python runtime to transformed via UDF and return to JVM

Solution: Apache Arrow, or write UDF in native languages (Scala or Java)

<p align="center">
<img src="https://user-images.githubusercontent.com/64508435/225969155-fa353902-c5d4-4984-a5aa-35b9104b8950.png" width=400/></p>

### Spark Session

- `SparkSession` is a driver process to control the Spark application
  - When you start Spark in this interactive mode, you implicitly create a `SparkSession` that manages the Spark Application.
  - When you start it through a standalone application, you must create the `SparkSession` **object** yourself in your application code.

#### `SparkSession` vs `SparkContext`

- In Spark 1.X, it had effectively two contexts: `SparkContext` and `SQLContext`
  - `SparkContext` focused on more fine-grained control of Spark’s central abstractions
  - `SQLContext` focused on the higher-level tools like Spark SQL.
- In Spark 2.X, the communtiy combined the two APIs into the centralized `SparkSession` that we have today.
  - `SparkSession` is more robustly instantiates the **Spark** and **SQL Contexts** and ensures that there is no context conflict
  - NOTE: both `SparkContext` and `SQLContext` are still available under `SparkSession`, but both of them are rarely used.

#### Creating a SparkSession

- A SparkSession is an entry point into all functionality in Spark, and is required if you want to build a dataframe in PySpark.

```Python
spark = SparkSession \
        .builder \
        .appName("SparkExample") \
        .config("spark.memory.offHeap.enabled","true") \ # data was cached in off-heap memory to avoid storing it directly on disk
        .config("spark.memory.offHeap.size","10g") \
        .getOrCreate()                                   # creates the Spark session with the specified configurations
```

- Another example of creating Spark with more config

```Python
def create_spark_session():
    '''
    Function for creating spark session.
    Input:
        None
    Output:
        1) SparkSession
    '''
    try:
        spark = SparkSession \
             .builder \
             .appName("SparkExample") \
            #  .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \ # S3A configuration for accessing data from AWS S3
            #  .config("spark.hadoop.fs.s3a.path.style.access",True) \
            #  .config("spark.hadoop.hadoop.security.authentication", "kerberos") \
            #  .config("spark.driver.extraJavaOptions",'') \
            #  .config("spark.authenticate",False) \
            #  .config("spark.network.crypto.enabled",False) \
            #  .config("spark.hadoop.fs.s3a.security.credential.provider.path", f"jceks://hdfs@accdpprodns:8020/user/haquannguyenh/haquannguyenh.jceks") \
            #  .config("spark.hadoop.fs.s3a.endpoint", 'https://s3i-sg.sgp.abc.com') \

        custom_spark_config = {
            "spark.driver.memory": "9g",
            "spark.executor.cores": "2",
            "spark.executor.memory": "9g",
            "spark.yarn.queue": "root.tnm.ada_analytics_tnm", # need to update with the YARN Queue
         }


        for key, value in custom_spark_config.items():
            spark = spark.config(key, value)

        spark = spark.getOrCreate()  # creates the Spark session with the specified configurations

        return spark

    except RuntimeError as e:
        print('Error in creating spark context')
        print(e)
```
