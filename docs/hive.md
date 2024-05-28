# Hive

## Introduction

### Hive

- Apache Hive is a data warehousing system built on top of Hadoop, supporting _analysis_ of large data sets stored in Hadoop-distributed file systems (HDFS) or S3 using SQL-like queries.
- It stores schema’s **metadata** in a database (such as Apache Derby, MySQL, Postgres or MariaDB) and processes data from connected storage (such as HDFS or S3).
- It provides SQL-like query language, HiveQL.

### Hive metastore

- Hive metastore records all the structure information (`metadata`) of a **hive table** and its **partitions** in the warehouse:
  - Column information and data type definitions
  - Serializers and de-serializers necessary to read and write data
  - Locations of relevant data in HDFS or S3
- The `metadata` in Hive metastore is stored in an open-source RDBMS namely Apache Derby (MySQL/ Postgres/ MariaDB can also be used)
- Hive metastore can have multiple schema (databases), and each schema (database) can contain multiple Hive table metadata.
  - Below is the pyspark code to list all database available in Hive metastore and Hive tables in each of the database

```Python
# Option 1: using the spark session to list database & hive table available in Hive metastore
# List all databases
databases = spark.catalog.listDatabases()
print("List of Hive Databases:")
for db in databases:
    print(db.name)

# List all tables in each database
for db in databases:
    print(f"\nTables in database: {db.name}")
    tables = spark.catalog.listTables(db.name)
    for table in tables:
        print(f"\t{table.name}")
```

```Python
# Option 2: using the spark.sql to list database & hive table available in Hive metastore
# List all databases
databases_df = spark.sql("SHOW DATABASES")
databases = databases_df.select("databaseName").rdd.flatMap(lambda x: x).collect() # flatten the databases to a list

# Iterate through each database and list its tables
for database in databases:
    spark.sql(f"USE {database}")
    tables_df = spark.sql("SHOW TABLES")
    tables = tables_df.select("tableName").rdd.flatMap(lambda x: x).collect()
    print(f"Database: {database}")
    print("Tables:")
    for table in tables:
        print(f"  {table}")
```

### Hive Table

- Hive supports two types of tables: **Managed Tables (Internal Tables)** & **External Tables**

#### Managed Tables (Internal Tables)

- **Managed Tables**: also known as `internal table`, these tables manage both the data and metadata in Hive's default warehouse directory (specified by `hive.metastore.warehouse.dir`).

#### External Tables

- **External Tables**: these tables reference data files stored outside Hive, often in HDFS or S3.
  - Since the data files are not managed by Hive, so altering or dropping an external Hive table doesn't delete the underlying data. You might have to delete the data from the underlying HDSF or S3 location.

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
  .partitionBy("partition_col1", "partition_col2") \ # partition cols
  .saveAsTable("hive_table_name") # Hive table_name
```

- By default, `write.saveAsTable` method supports the managed (internal) creation. Hence, to create an external table, you need to specify the `path` option in the `saveAsTable` method.

```Python
# Type 2: External Table
df.write \
  .mode("overwrite") \
  .option("database", "your_database_name") \ # you can choose the database `your_database_name` where the Hive table will be created.
  .option("path", "s3a://your_bucket_name/your_folder_path/your_external_table_name") \ # specifies the Amazon S3 path where the data for the external table will be stored
  .partitionBy("partition_col1", "partition_col2") \
  .format("parquet") \
  .saveAsTable("hive_table_name")
```

### Method 2: Hive Table Creation using SQL Query with `spark.sql`

- Using SQL queries with `spark.sql`, you can using `CREATE TABLE` or `CREATE EXTERNAL` SQL statement to create the Hive managed (internal) table or external table respectively and to specify the database which the table belong to.

```Python
# Type 1: Managed Table (Internal Table)
spark.sql("""
    CREATE TABLE IF NOT EXISTS your_database_name.your_managed_table_name (
        id INT, name STRING, age INT
    )
    PARTITIONED BY (country STRING, city STRING)
    USING PARQUET
""")
```

- For the external table, the external location, where the table's data is stored, is also specified in the SQL statement with `LOCATION` keyword.

```Python
# Type 2: External Table
spark.sql("""
    CREATE TABLE IF NOT EXISTS your_database_name.your_external_table_name (
        id INT, name STRING, age INT
    )
    PARTITIONED BY (country STRING, city STRING)
    STORED AS PARQUET
    LOCATION 'hdfs://your_hdfs_path/your_external_table_name/'
""")
```

- Note:
  - `USING PARQUET` is more commonly used in Spark SQL for directly creating managed tables with specific data sources.
  - `STORED AS PARQUET` and `LOCATION` is Hive DDL syntax used for specifying the storage format (parquet) + location (external like HDSF or S3)

## How to insert data to Hive Table

- Once the Hive Table is created and stored all the necessary information to the Hive Metastore, there is no difference in inserting the data into managed tables or external tables as only need to mention `your_database_name.your_hive_table_name`

### Method 1: directly writing via `write.saveAsTable` to Hive Table

```Python
df.write \
  .mode("overwrite") \
  .partitionBy("partition_col1", "partition_col2") \
  .format("parquet") \
  .saveAsTable("your_database_name.your_hive_table_name")
```

### Method 2: writing to the underlying storage location & refresh the Hive metadata

- Another way to update the Hive Table is to write the spark dataframe to the underlying storage location & then refresh the Hive Table with the `REFRESH TABLE` and `MSCK REPAIR` SQL commands to update the metadata in Hive metastore with the latest partition information.
- `REFRESH TABLE` and `MSCK REPAIR` are related to maintaining metadata and ensuring the correctness of Hive tables, especially external tables with partitions.
  - `REFRESH TABLE` update the metadata of a table in Hive. It reloads the metadata information of the table from the underlying file system or storage location.
  - `MSCK REPAIR` stands for "Metastore Check Repair" is specifically used for **external tables** with **partitions**.
    - It's used to repair the metadata for partitions that were added or removed externally.

```Python
table_path = "s3a://your_bucket_name/your_folder_path/your_hive_table_name"
# write the spark dataframe to the underlying s3 storage location of the hive table "your_hive_table_name"
df.write \
  .mode("overwrite") \
  .partitionBy("partition_col1", "partition_col2") \
  .parquet(table_path)

# update the metadata of a table in Hive
spark.sql("REFRESH TABLE your_database_name.your_hive_table_name")

# if table is external and have partitions, MSCK REPAIR is used to repair the metadata for partitions that were added or removed externally.
spark.sql("MSCK REPAIR your_database_name.your_hive_table_name")
```

## How to view the Hive Table details

```Python
# Get detailed metadata about the table
table_metadata = spark.sql("DESCRIBE FORMATTED db.table_name")
table_metadata.show(truncate=False)

# +----------------------------+----------------------------------------------------+-------+
# |col_name                    |data_type                                           |comment|
# +----------------------------+----------------------------------------------------+-------+
# |# Detailed Table Information|                                                    |       |
# |Database                    |db                                                  |       |
# |Table                       |table_name                                          |       |
# |Owner                       |user                                                |       |
# |Created Time                |Fri May 28 13:57:49 UTC 2023                        |       |
# |Last Access                 |UNKNOWN                                             |       |
# |Created By                  |Spark 2.4.0                                         |       |
# |Type                        |EXTERNAL                                            |       |
# |Provider                    |parquet                                             |       |
# |Location                    |s3://path/to/table                                  |       |
# |Serde Library               |org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe|       |
# |InputFormat                 |org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat|       |
# |OutputFormat                |org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat|       |
# |Partition By                |:partition_col1, partition_col2                     |       |
# +----------------------------+----------------------------------------------------+-------+
```

- Get the table partition information

```Python
# Get the partition information (if the table is partitioned)
partitions = spark.sql("SHOW PARTITIONS db.table_name")
partitions.show(truncate=False)

# +--------------------------+
# |partition                 |
# +--------------------------+
# |partition_col1=value1/... |
# |partition_col1=value2/... |
# |partition_col1=value3/... |
# +--------------------------+
```
