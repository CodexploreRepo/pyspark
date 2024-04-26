# Spark Config

## Set & Get Spark Config

### Set Spark Config

- **Method 1**: specify spark config when creating the `SparkSession` via `.config`

```Python
spark = SparkSession
          .builder \
          .appName("SparkExample") \
          .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
          .getOrCreate()

# if you need to set multiple config, you can define them in a dictionary or in yaml file and set accordingly via a loop
spark = SparkSession \
          .builder \
          .appName("SparkExample") \

custom_spark_config = {
    "spark.driver.memory": "9g",
    "spark.executor.cores": "2",
    "spark.executor.memory": "9g",
    "spark.yarn.queue": "root.tnm.ada_analytics_tnm", # need to update with the YARN Queue's name
}

# for-loop to set each spark config in the dictionary
for key, value in custom_spark_config.items():
    spark = spark.config(key, value)

spark = spark.getOrCreate()  # creates the Spark session with the specified configurations
```

- **Method 2**: to set the spark config after the `SparkSession` object is created

```Python
# in this case, we have the "spark" is the created SparkSession object
spark.conf.set("spark.sql.shuffle.partitions", "200")
```

- **Method 3**: specify the spark config in the `spark-submit` command as the above two methods are hardcoded and pretty much static, and you want to have different parameters for different jobs

```shell
spark-submit --executor-memory 16G
```

### Get Spark Config

- `spark.conf.get("spark.executor.memory")` to check the spark config if it is set properly

## List of configs

- `("spark.sql.warehouse.dir", "/user/hive/warehouse")` this configuration property specifies the default location where Spark SQL will store _managed tables_ in Hive's warehouse directory.
  - _Managed tables_ are those tables whose (**data** and **metadata**) are managed by Spark, and such tables are created using the `CREATE TABLE` SQL command
  - If you don't explicitly set this configuration, Spark will use the Hive default warehouse location, which is typically set to something like `/user/hive/warehouse`
- `("spark.sql.shuffle.partitions", "200")` to set the number of the output partitions from the shuffle (default is 200)
  - Experimenting with different values, you should see drastically different runtimes.
