# Introduction

## What is Apache Spark ?

- Apache Spark is a unified computing engine and a set of libraries for parallel data processing on computer clusters.
- Spark is written in Scala, and runs on the Java Virtual Machine (JVM), so therefore to run Spark either on your laptop or a cluster, all you need is an installation of Java.
  - If you want to use the Python API, you will also need a Python interpreter

## Important Terms in PySpark

- `Dataframe` a distributed collection of data organised into named columns
- `Partitioning` a way to split the data into multiple partitions so that you can execute transformations on multiple partitions in parallel which allows completing the job faster.
- A `Partition` is a collection of rows that sit on one physical machine in your cluster. A DataFrame’s partitions represent how the data is physically distributed across the cluster of machines during execution.
- `Shuffle` movement of data accross partitions
- `RDD` Resilient Distributed Dataset - used for low level computations and unstructured data
- `Transformation` applying the function produces new RDD. Lazy in nature
- `Action` evaluate the corresponding transformations and action to show the result

## Starting Spark

- To start an interactive Spark Application:
  - `./bin/spark-shell` to access the Scala console
  - `./bin/pyspark` to access the Python console
- `spark-submit` to submit a precompiled standalone application to Spark
  - `spark-submit` offers several controls with which you can specify the resources your application needs as well as how it should be run and its command-line arguments.

```shell
./bin/spark-submit \
  --master local \
  ./pyspark_folder/src/main/python/pi.py 10
```

## `SparkSession`

- `SparkSession` is a driver process to control the Spark application
  - When you start Spark in this interactive mode, you implicitly create a `SparkSession` that manages the Spark Application.
  - When you start it through a standalone application, you must create the `SparkSession` **object** yourself in your application code.

### Creating a SparkSession

- A SparkSession is an entry point into all functionality in Spark, and is required if you want to build a dataframe in PySpark.

```Python
spark = SparkSession \
        .builder \
        .appName("Spark Example") \
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
             .appName("Spark Example") \
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
