# Daily Knowledge

## Day 2

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
