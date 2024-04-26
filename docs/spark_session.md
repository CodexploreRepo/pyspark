# Spark Session

## Introduction

### What is `SparkSession`

- The first step of any Spark Application is creating a `SparkSession`.
- `SparkSession` is a driver process to control the Spark application
  - When you start Spark in this interactive mode, you implicitly create a `SparkSession` that manages the Spark Application.
  - When you start it through a standalone application, you must create the `SparkSession` **object** yourself in your application code.

### `SparkSession` vs `SparkContext`

- In Spark 1.X, it had effectively two contexts: `SparkContext` and `SQLContext`
  - `SparkContext` focused on more fine-grained control of Spark’s central abstractions
  - `SQLContext` focused on the higher-level tools like Spark SQL.
- In Spark 2.X, the communtiy combined the two APIs into the centralized `SparkSession` that we have today.
  - `SparkSession` is more robustly instantiates the **Spark** and **SQL Contexts** and ensures that there is no context conflict
  - NOTE: both `SparkContext` and `SQLContext` are still available under `SparkSession`, but both of them are rarely used.

## `SparkSession` Creation

- A SparkSession is an entry point into all functionality in Spark, and is required if you want to build a dataframe in PySpark.

```Python
spark = SparkSession \
        .builder \
        .appName("SparkExample") \
        .enableHiveSupport() # to work with the Hive metastore. \
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
             .enableHiveSupport() # to work with the Hive metastore. \
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
            "spark.yarn.queue": "root.tnm.ada_analytics_tnm", # need to update with the YARN Queue's name
         }


        for key, value in custom_spark_config.items():
            spark = spark.config(key, value)

        spark = spark.getOrCreate()  # creates the Spark session with the specified configurations

        return spark

    except RuntimeError as e:
        print('Error in creating spark context')
        print(e)
```
