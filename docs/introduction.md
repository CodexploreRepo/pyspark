# Introduction
## Important Terms in PySpark
- `Dataframe` a distributed collection of data organised into named columns
- `Partitioning`  a way to split the data into multiple partitions so that you can execute transformations on multiple partitions in parallel which allows completing the job faster.
- `Shuffle` movement of data accross partitions
- `RDD` Resilient Distributed Dataset - used for low level computations and unstructured data
- `Transformation` applying the function produces new RDD. Lazy in nature
- `Action` evaluate the corresponding transformations and action to show the result
