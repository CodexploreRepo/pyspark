# Spark SQL

## How to create Temp View from Spark DataFrame

- You can register any DataFrame as a table or view (a temporary table) and query it using pure SQL with `spark.sql` function (remember, spark is our `SparkSession` variable)
- There is no performance difference between writing SQL queries or writing DataFrame code.

```Python
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("TempViewExample").getOrCreate()

df.createOrReplaceTempView("flight_data")

sql_way = spark.sql("""
    SELECT DEST_COUNTRY_NAME, count(1)
    FROM flight_data
    GROUP BY DEST_COUNTRY_NAME
""")

df_way = df \
  .groupBy("DEST_COUNTRY_NAME")\
  .count()
```

- You check how many temporary views (tempViews) are available in a Spark Session

```Python
# Get the list of tempViews from the catalog
temp_views_list = spark.catalog.listTables()

# Count the number of tempViews
num_temp_views = len(temp_views_list)

# Print the list of tempViews and the count
print(f"List of {num_temp_views} TempViews:")
for temp_view in temp_views_list:
    print(temp_view.name)
```

## `createOrReplaceTempView` and `registerTempTable`

- Both `createOrReplaceTempView` and `registerTempTable` are used to create temporary views from DataFrames, making them available for SQL queries.
- However, `createOrReplaceTempView` is the recommended method for creating temporary views in Apache Spark, offering better compatibility and functionality compared to the deprecated registerTempTable.
