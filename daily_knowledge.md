# Daily Knowledge
## Day 1
### Spark DataFrame
#### createDataFrame
##### Schema
- When creating Spark DataFrame from Pandas DataFrame, PySpark schema needs to be the same order with Pandas DataFrame
  - Order: PySpark schema does not map by the column names, but by the order of `StructField`s in the `StructType` list
- Create Spark from Numpy or Pandas Series: 
  - Spark usually [struggles with **numpy** dtypes](https://stackoverflow.com/questions/66204342/typeerror-field-value-floattype-can-not-accept-object-0-016354798954796402-in), so you can cast them to Python float type before converting to a Spark dataframe
  ```Python
  df = spark.createDataFrame(
    [float(x) for x in pd_series],
    FloatType()
  )
  ```
  - Best Practise: do not convert Numpy or Pandas Series to Spark DataFrame

### UDF
#### Why UDF is Slow ?
- Py4J helps to connect the JVM & Python runtime, so if there are native or SQL spark transformation, they will be executed in JVM.
- Howerver, UDF code cannot be executed in JVM, because the UDF code is python-based, so only can run in Python runtime of the worker nodes and return the data to JVM
  - Therefore, each row of the data will be serialized and sent to Python runtime to transformed via UDF and return to JVM

Solution: Apache Arrow, or write UDF in native languages (Scala or Java)
<p align="center">
<img src="https://user-images.githubusercontent.com/64508435/225969155-fa353902-c5d4-4984-a5aa-35b9104b8950.png" width=400/></p>
