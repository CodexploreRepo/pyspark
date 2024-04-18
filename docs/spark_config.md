# Spark Config

- `spark.conf.set("spark.sql.shuffle.partitions", "200")` to set the number of the output partitions from the shuffle (default is 200)
  - Experimenting with different values, you should see drastically different runtimes.
