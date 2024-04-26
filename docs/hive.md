# Hive

## Introduction

- Apache Hive is a data warehousing system built on top of Hadoop. It allows users to query and analyze large datasets stored in Hadoop-distributed file systems (HDFS) or S3 using SQL-like queries.

## How to create Hive table

- Step 1: create the SparkSession with the Hive configuration
  - `spark.sql.warehouse.dir` this configuration property specifies the default location where Spark SQL will store _managed tables_ in Hive's warehouse directory.
    - _Managed tables_ are those tables whose (**data** and **metadata**) are managed by Spark, and such tables are created using the `CREATE TABLE` SQL command
    - If you don't explicitly set this configuration, Spark will use the Hive default warehouse location, which is typically set to something like `/user/hive/warehouse`

```Python
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("Hive Table Example") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .enableHiveSupport() \ # enable Hive Support
    .getOrCreate()
```
