# Hive

## Introduction

### Hive

- Apache Hive is a data warehousing system built on top of Hadoop, supporting analysis of large data sets stored in Hadoop-distributed file systems (HDFS) or S3 using SQL-like queries.
- It stores schema’s **metadata** in a database (such as Postgres) and processes data from connected storage (such as HDFS or S3).
- It provides SQL-like query language, HiveQL.

### Hive Metastore

- Hive Metastore records all the structure information (`metadata`) of **tables** and **partitions** in the warehouse:
  - Column information and data type definitions
  - Serializers and de-serializers necessary to read and write data
  - Locations of relevant data in HDFS or S3
- The `metadata` in Metastore is stored in an open-source RDBMS namely Apache Derby (MySQL/ Postgres/ MariaDB can also be used)

### Hive Table

- Hive supports two types of tables: **Managed Tables (Internal Tables)** & **External Tables**

#### Managed Tables (Internal Tables)

- **Managed Tables**: Also known as `internal table`, these tables manage both the data and metadata in Hive's default warehouse directory (specified by `hive.metastore.warehouse.dir`).

#### External Tables

- **External Tables**: These tables reference data files stored outside Hive, often in HDFS or S3.
  - Since the data files are not managed by Hive, so altering or dropping an external Hive table doesn't delete the underlying data. You might have to delete the data via the underlying HDSF or S3 location.

## How to create Hive Table

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

- Step 2: There are two methods to create the Hive table

### Method 1: Hive Table Creation using DataFrame API with `write.saveAsTable`

- Using the DataFrame API with `write.saveAsTable`, the Hive table is created in the default database unless you explicitly specify the database using the `option` method
  - `.option("database", "your_database_name")` you can choose the database `your_database_name` where the Hive table will be created.

```Python
# Type 1: Managed Table (Internal Table)
df.write \
  .mode("overwrite") \
  .option("database", "your_database_name") \ # you can choose the database `your_database_name` where the Hive table will be created.
  .saveAsTable("hive_table_name") # Hive table_name
```

### Method 2: Hive Table Creation using SQL Query with `spark.sql`

- Using SQL queries with `spark.sql`, you can indeed specify the database directly in the SQL statement.

```Python
spark.sql("""
    CREATE EXTERNAL TABLE IF NOT EXISTS your_database_name.test99 (
        c0 STRING, c1 STRING, c2 STRING, c3 STRING, c4 STRING, c5 STRING, c6 STRING
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS PARQUET
    LOCATION 'hdfs://my_path/table/'
""")
```
