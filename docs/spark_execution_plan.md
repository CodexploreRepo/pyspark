# Spark Execution Plan

## Introduction

- Use `explain()` function on any DataFrame object to see the DataFrame’s lineage (or how Spark will execute this query)
- The execution plan is read from top to bottom, the top being the end result, and the bottom being the source(s) of data
  - In below example, take a look at the first keywords for each row: `FileScan`, `Exchange`, `Sort`. Sorting the data is actually a wide transformation because rows will need to be compared with one another.

```Python
flight_df = spark\
  .read\
  .option("inferSchema", "true")\ # schema inference, which means that we want Spark to take a best guess at what the schema of our DataFrame should be
  .option("header", "true")\
  .csv("/data/flight-data/csv/2015-summary.csv")

flight_df.sort("count").explain()
# == Physical Plan ==
# *Sort [count#195 ASC NULLS FIRST], true, 0
# +- Exchange rangepartitioning(count#195 ASC NULLS FIRST, 200)
#    +- *FileScan csv [DEST_COUNTRY_NAME#193,ORIGIN_COUNTRY_NAME#194,count#195]
```
