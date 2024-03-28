# PySpark Catalogue

This file contains code snippets with the most important functions in PySpark. For more detailed information, check [spark_big_data_guide](https://github.com/mxagar/spark_big_data_guide).

Mikel Sagardia, 2023.  
No guarantees.

## Table of Contents

- [PySpark Catalogue](#pyspark-catalogue)
  - [Table of Contents](#table-of-contents)
  - [Setup](#setup)
    - [Install and Run PySpark](#install-and-run-pyspark)
    - [Running on a Notebook](#running-on-a-notebook)
    - [Creating a Spark Session](#creating-a-spark-session)
  - [Basics](#basics)
  - [Data Manipulation](#data-manipulation)
    - [Aggregation Functions](#aggregation-functions)
  - [Machine Learning](#machine-learning)

## Setup

### Install and Run PySpark

To install PySpark locally:

```bash
conda activate ds # select an environment
pip install pyspark
pip install findspark
```

We can launch a Spark session in the Terminal locally as follows:

```bash
conda activate ds
pyspark
# SparkContext available as 'sc'
# Web UI at: http://localhost:4040/
# SparkSession available as 'spark'

sc # <SparkContext master=local[*] appName=PySparkShell>
sc.version # '3.4.0'

# Now, we execute the scripts we want, which are using the sc SparkContext

```

An example script can be:

```python
import random
num_samples = 100000000
def inside(p):
    x, y = random.random(), random.random()
    return x*x + y*y < 1

# Note that sc is not imported, but it's already available
# This will execute the function inside() with spark
# It might take some time if run locally, because Spark is optimized
# for large and distributed datasets
count = sc.parallelize(range(0, num_samples)).filter(inside).count()
pi = 4 * count / num_samples

print(pi)
sc.stop()
```

### Running on a Notebook

If we want to use PySpark on a Jupyter notebook, we need to change the environment variables in `~/.bashrc` or `~/.zshrc`:

```bash
export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS='notebook'
```

Then, we restart `pyspark` and launch jupyter from it:

```bash
conda activate ds
pyspark
jupyter
```

**Alternatively**, we can use `findspark` without modifying the environment variables and without starting pyspark from outside:

```bash
conda activate ds
jupyter lab
```

Then, the notebook could contain the code as follows:

```python
import findspark
findspark.init()

import pyspark
import random

sc = pyspark.SparkContext(appName="Pi") # AppName: Pi
num_samples = 100000000

def inside(p):     
    x, y = random.random(), random.random()
    return x*x + y*y < 1

# This will execute the function inside() with spark
# It might take some time if run locally, because Spark is optimized
# for large and distributed datasets
count = sc.parallelize(range(0, num_samples)).filter(inside).count()
pi = 4 * count / num_samples

print(pi) # 3.14185392
sc.stop()
```

### Creating a Spark Session

Creating multiple `SparkSession`s and `SparkContext`s can cause issues, use the `SparkSession.builder.getOrCreate()` method instead, which returns an existing `SparkSession` if there's already one in the environment, or creates a new one if necessary. Usually, in a notebook, we run first

```python
import findspark
findspark.init()
```

And then, we create a session and work with it. If we shut down the notebook/kernel, the session disappears.

```python
# Import SparkSession from pyspark.sql
from pyspark.sql import SparkSession

# Create or get a (new) SparkSession: session
session = SparkSession.builder.getOrCreate()

# Print session: our SparkSession
print(session)
```

## Basics

Source: [`01_Basics.ipynb`](./02_Spark/lab/02_Intro_PySpark/01_Basics.ipynb).

- Upload a CSV to Spark
- Register a table view
- Inspect table
- Run SQL queries
- Convert Spark table/dataframe into Pandas dataframe and vice versa

```python
# Print the tables in the catalog
# catalog provides access to all the data inside the cluster
# catalog.listTables() lists all tables
# Currently, there is no data on the cluster
print(session.catalog.listTables()) # []

# Load the CSV file as a DataFrame
# We use the SparkSession
# We get back flights_df, which is a Spark SQL dataframe.
# NOTE: It is also possible to convert a Pandas dataframe into a Spark SQL Dataframe,
# shown later
flights_df = session.read.csv("../data/flights_small.csv", header=True, inferSchema=True)

# Register the DataFrame as a temporary view.
# This allows us to query the data using SQL-like syntax in the used session.
# Notes:
# - The contents in flights_df are not registered in the session catalog, by default!
# - flights_df is linked to the session
# - We create a temporary view of flights_df named flights in the catalog
flights_df.createOrReplaceTempView("flights")

# Print the list of tables in the SparkSession/catalog
print(session.catalog.listTables())
# [Table(name='flights', catalog=None, namespace=[], description=None, tableType='TEMPORARY', isTemporary=True)]

# We can loop the table names
for t in session.catalog.listTables():
    print(t.name)

# Once we know the name of a table, we can get its
# Spark SQL Dataframe as follows
flights_df_ = session.table("flights")

# Equivalent to .head(2) on flights_df
flights_df.show(2)

# Equivalent to .head(2) on second flights_df_: It's the same table
flights_df_.show(2)

# SQL query
query = "FROM flights SELECT * LIMIT 10"

# Get the first 10 rows of flights
# The returning object is a Spark SQL dataframe, as flights_df
# but this time it contains only 10 rows
flights10 = session.sql(query)

# Show the results: equivalent to the pandas .head(20)
flights10.show()

# SQL query
query = "SELECT origin, dest, COUNT(*) as N FROM flights GROUP BY origin, dest"

# Run the query
flight_counts = session.sql(query)

# Convert the results to a pandas DataFrame
pd_counts = flight_counts.toPandas()

# Print the head of pd_counts
pd_counts.head()
# 	origin	dest	N
# 0	SEA	    RNO	  8
# 1	SEA	    DTW	  9
# ...

airports_df = pd.read_csv('../data/airports.csv')
planes_df = pd.read_csv('../data/planes.csv')

# Examine the tables in the catalog: only one table - 'flights'
print(session.catalog.listTables())

# Create spark_temp from airports_df
airports_sdf = session.createDataFrame(airports_df)
# Add airports_sdf to the session's catalog
airports_sdf.createOrReplaceTempView("airports")

# Create spark_temp from planes_df
planes_sdf = session.createDataFrame(planes_df)
# Add planes_sdf to the session's catalog
planes_sdf.createOrReplaceTempView("planes")

# Examine the tables in the catalog again: now 3 tables - 'flights', 'airports', 'planes'
print(session.catalog.listTables())
```

## Data Manipulation

Source: [`02_Manipulating_Data.ipynb`](./02_Spark/lab/02_Intro_PySpark/02_Manipulating_Data.ipynb).

- Creating, Renaming, and Casting Columns
- See [SQL in a Nutshell](./02_Spark/README.md#322-sql-in-a-nutshell)
- Filtering: SQL `WHERE`
- Selecting: SQL `SELECT`
- Grouping: SQL `GROUP BY`
- Joins
- Aggregation functions

```python
from pyspark.sql.functions import col

# Create/get the DataFrame flights
flights = session.table("flights")

# Show the head
flights.show(5)

# Add a new column: duration_hrs
# General syntax: df = df.withColumn("newCol", df.oldCol + 1)
# A new dataframe is returned! That's because dataframes and their columns are inmutable
# To modify a colum: df = df.withColumn("col", df.col + 1)
# BUT: in reality, we create a new dataframe with the modified column
flights = flights.withColumn("duration_hrs", flights.air_time/60)

# Convert air_time and dep_delay (strings) to double to use math operations on them
flights = flights.withColumn("air_time", col("air_time").cast("double"))
flights = flights.withColumn("dep_delay", col("dep_delay").cast("double"))

# Rename column and keep old
flights = flights.withColumn("flight_duration", flights.air_time)

# Another way to rename column names
# This option replaces the old column
# flights = flights.withColumnRenamed("flight_duration", "air_time")
flights.printSchema()

# Filter flights by passing a string
long_flights1 = flights.filter("distance > 1000")

# Filter flights by passing a column of boolean values
long_flights2 = flights.filter(flights.distance > 1000)

# Print the data to check they're equal
long_flights1.show(2)
long_flights2.show(2)

# Select the first set of columns
selected1 = flights.select("tailnum", "origin", "dest")

# Select the second set of columns
temp = flights.select(flights.origin, flights.dest, flights.carrier)

# Define first filter
filterA = flights.origin == "SEA"

# Define second filter
filterB = flights.dest == "PDX"

# Filter the data, first by filterA then by filterB
selected2 = temp.filter(filterA).filter(filterB)

# Define avg_speed
# We define a new column object
avg_speed = (flights.distance/(flights.air_time/60)).alias("avg_speed")

# Select the correct columns
# We can pass comma separated strings or column objects to select();
# each column is a comma-separated element.
speed1 = flights.select("origin", "dest", "tailnum", avg_speed)

# Create the same table using a SQL expression
# We can pass comma separated SQL strings to selectExpr(); each colum operation is
# a comma-separated element.
speed2 = flights.selectExpr("origin", "dest", "tailnum", "distance/(air_time/60) as avg_speed")

# Group all flights by destination and for them
# pick the minimum distance
flights.groupBy("dest").min("distance").show(5)

# Find the shortest flight from PDX in terms of distance
# Note that in this case we don't pass a column to groupBy, but 
# concatenate an aggregation function with the column, which applies to all rows
flights.filter(flights.origin == "PDX").groupBy().min("distance").show()

# Find the longest flight from SEA in terms of air time
# Note that in this case we don't pass a column to groupBy, but 
# concatenate an aggregation function with the column, which applies to all rows
flights.filter(flights.origin == "SEA").groupBy().max("air_time").show()

# Average duration of Delta flights
# Note that in this case we don't pass a column to groupBy, but 
# concatenate an aggregation function with the column, which applies to all rows
flights.filter(flights.carrier == "DL").filter(flights.origin == "SEA").groupBy().avg("air_time").show()

# Total hours in the air
# Note that in this case we don't pass a column to groupBy, but 
# concatenate an aggregation function with the column, which applies to all rows
flights.withColumn("duration_hrs", flights.air_time/60).groupBy().sum("duration_hrs").show()

# Group by tailnum
by_plane = flights.groupBy("tailnum")

# Number of flights each plane made
by_plane.count().show(5)

# Group by origin
by_origin = flights.groupBy("origin")

# Average duration of flights from PDX and SEA
by_origin.avg("air_time").show()

# Joins (Pandas)
dj_joined = df_left.join(other=df_right,
                         on="key_col",
                         how="left_outer")
# Possible values for how: 
#    "inner" (default), "outer", "left_outer", "right_outer", "leftsemi", and "cross"
# Other arguments for join():
# - suffixes: tuple of strings to append to the column names that overlap between the two DataFrames.
#    By default: "_x" and "_y"
# - broadcast: boolean value indicating whether to broadcast the smaller DataFrame to all nodes in the cluster
#    to speed up the join operation. By default: False

# Examine the data
print(airports.show(5))

# Rename the faa column to be dest
airports = airports.withColumnRenamed("faa", "dest")

# Examine the data
print(airports.show(5))

# Join the DataFrames
flights_with_airports = flights.join(airports,
                                     on="dest",
                                     how="left_outer")

# Examine the new DataFrame
print(flights_with_airports.show(5))
```

### Aggregation Functions

The module `pyspark.sql.functions` contains many aggregation functions which we can use with the generic `agg()` method which is applied after any `groupBy()`:

- `abs()`: Computes the absolute value of a column.
- `avg()`: Computes the average of a column.
- `stddev()`: Computes the standard deviation of a column.
- `col()`: Returns a column based on the given column name.
- `concat()`: Concatenates multiple columns together.
- `count()`: Counts the number of non-null values in a column.
- `date_format()`: Formats a date or timestamp column based on a specified format string.
- `dayofmonth()`: Extracts the day of the month from a date or timestamp column.
- `explode()`: Transforms an array column into multiple rows.
- `first()`: Returns the first value of a column in a group.
- `lit()`: Creates a column with a literal value.
- `max()`: Computes the maximum value of a column.
- `min()`: Computes the minimum value of a column.
- `month()`: Extracts the month from a date or timestamp column.
- `round()`: Rounds a column to a specified number of decimal places.
- `split()`: Splits a string column based on a delimiter.
- `sum()`: Computes the sum of a column.
- `to_date()`: Converts a string column to a date column.
- `to_timestamp()`: Converts a string column to a timestamp column.
- `udf()`: Defines a user-defined function that can be used in PySpark.

... and many more.

```python
# Import pyspark.sql.functions as F
import pyspark.sql.functions as F

# Group by month and dest
by_month_dest = flights.groupBy("month", "dest")

# Average departure delay by month and destination
by_month_dest.avg("dep_delay").show(5)

# Standard deviation of departure delay
by_month_dest.agg(F.stddev("dep_delay")).show(5)
```

## Machine Learning



