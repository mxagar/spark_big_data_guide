# PySpark Catalogue

This file contains code snippets with the most important functions in PySpark. For more detailed information, check [spark_big_data_guide](https://github.com/mxagar/spark_big_data_guide).

Mikel Sagardia, 2023.  
No guarantees.

## Table of Contents

- [PySpark Catalogue](#pyspark-catalogue)
  - [Table of Contents](#table-of-contents)
  - [Setup](#setup)
    - [Install PySpark](#install-pyspark)
    - [Run PySpark](#run-pyspark)
    - [Running on a Notebook](#running-on-a-notebook)
    - [Creating a Spark Session (or a Spark Context)](#creating-a-spark-session-or-a-spark-context)
  - [Basics](#basics)
  - [Data Manipulation](#data-manipulation)
    - [Aggregation Functions](#aggregation-functions)
    - [Functional Programming: Pure Functions](#functional-programming-pure-functions)
    - [Music Service Example: Data Wrangling with the Python API](#music-service-example-data-wrangling-with-the-python-api)
    - [Music Service Example: Data Wrangling with the SQL API](#music-service-example-data-wrangling-with-the-sql-api)
  - [Machine Learning](#machine-learning)
    - [Data Processing Pipeline](#data-processing-pipeline)
      - [Load Data](#load-data)
      - [Data processing](#data-processing)
      - [Feature Engineering](#feature-engineering)
      - [Pipeline](#pipeline)
      - [Modeling](#modeling)

## Setup

### Install PySpark

To install PySpark locally:

```bash
conda activate ds # select an environment
pip install pyspark
pip install findspark
```

If we are using Windows and run the cluster locally, there is an issue with the Hadoop's file system libraries, which need to be installed separately.

To resolve this issue, you need to:

1. **Download WinUtils**: Spark on Windows requires `WinUtils.exe`, which is part of Hadoop binaries but not included with Spark. You can download pre-compiled WinUtils binaries compatible with your Hadoop version from various GitHub repositories:
   
   - [cdarlint/winutils](https://github.com/cdarlint/winutils)
   - [steveloughran/winutils](https://github.com/steveloughran/winutils)
   - or by searching for "Hadoop WinUtils" online.

2. **Set up HADOOP_HOME**:
    - Clone the selected repository, e.g., to `C:\...\git_repositories\winutils`
      - We choose the version we're going to use, e.g. `hadoop-3.3.5`
      - We check that in the `<version>\bin` folder, there is a `winutils.exe` file
    - Set `HADOOP_HOME` to the parent directory of `bin`, e.g.: `C:\...\git_repositories\winutils\hadoop-3.3.5`
      - System Properties -> Advanced -> Environment Variables -> System Variables -> New.
    - Add `%HADOOP_HOME%\bin` to your system's `Path` environment variable so that the WinUtils binaries are accessible from anywhere.

### Run PySpark

We can launch a Spark session in the Terminal locally as follows:

```bash
conda activate spark
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
conda activate spark
pyspark
jupyter
```

**Alternatively**, we can use `findspark` without modifying the environment variables and without starting pyspark from outside:

```bash
conda activate spark
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

### Creating a Spark Session (or a Spark Context)

`SparkContext` was the main entry point to Spark and is used to connect to a Spark cluster. It is responsible for coordinating the distribution of tasks, managing memory, and scheduling operations. However, since Spark 2.0, `SparkSession` has been introduced as a higher-level API that provides a unified entry point to Spark, SQL, and streaming functionality. It includes `SparkContext` under the hood and provides additional functionality for working with structured data using Spark's SQL, DataFrame, and Dataset APIs. It also provides built-in support for working with Hive, Avro, Parquet, and other file formats.

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
# Note that we get a link to the UI!
session
print(session)

# Get information of the SparkContext
session.sparkContext.getConf().getAll()
```

## Basics

Source: [`01_Basics.ipynb`](./02_Spark/lab/02_Intro_PySpark/01_Basics.ipynb).

Topics:

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
# In this case, we're runnig Spark locally and use a local dataset
# but we could also use a URL and another format like JSON: "hdfs://ec2-path/my_file.json"
#   path = "../data/sparkify_log_small.json"
#   flights_df = session.read.json(path)

# To save a dataset, we can use .write.save() and choose the desired format
# Many available formats: Parquet, Avro, ORC, JSON, CSV,...
# Note that the resulting output is a folder!
# Inside that folder:
# - the resulting CSV is partitioned into several CSVs,
#   because they are created in parallel/distributedly
# - we have also CRC files of the CSVs: Cyclic Redundancy Check,
#   i.e., checksums of the files to allow for data consistency checks
out_path = "../data/flights_small.json"
flights_df.write.save(out_path, format="json", header=True)

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
flights_df.take(2)

# Get column names and types
flights_df.describe().show()

# Number of entries
flights_df.count()

# Get dataset Schema
flights_df.printSchema()

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

Sources: 

- [`02_Manipulating_Data.ipynb`](./02_Spark/lab/02_Intro_PySpark/02_Manipulating_Data.ipynb).
- [`4_data_wrangling.ipynb`](./02_Spark/lab/03_Data_Wrangling/4_data_wrangling.ipynb).

In general, we have two ways of manipulating data on Spark Dataframes:

- Python API (*imperative*: we define all steps to final state): we use **functional programming**, i.e., we don't use for-loops, instead, transformation functions are defined and passed to `map()`, `apply()` or similar application methods. The approach where we use for loops and Co. is called **procedural programming**.
- SQL (*declarative*: we request final state).

Under the hood:

- Spark is implemented in Scala (object-oriented and functional), which uses the Java Virtual Machine (JVM); Python instructions are transformed with [py4j](https://www.py4j.org/) to interact with the JVM. Functional programming is very well suited for distributed systems and programming and it is related to how MapReduce and Hadoop work.
- We have two structures: **Direct Acyclic Graphs (DAGs)**, which chain operations or functions applied to the data and **Resilient Distributed Datasets (RDD)**, which are abstracted by Spark Dataframes. In other words, DAGs are the list of functions applied on RDDs. However, we perform **lazy evaluation**: all sub-functions are chained in Direct Acyclic Graphs (DAGs) and they are not run on the data until it is really necessary. The combinations of sub-functions or chained steps before touching any data are called **stages**.

General functions:

- `df.select()`: returns a new DataFrame with the selected columns
- `df.filter()`: filters rows using the given condition
- `df.where()`: is just an alias for `filter()`
- `df.groupBy()`: groups the DataFrame using the specified columns, so we can run aggregation on them
- `df.sort()`: returns a new DataFrame sorted by the specified column(s). By default the second parameter 'ascending' is True.
- `df.dropDuplicates()`: returns a new DataFrame with unique rows based on all or just a subset of columns
- `df.withColumn()`: returns a new DataFrame by adding a column or replacing the existing column that has the same name. The first parameter is the name of the new column, the second is an expression of how to compute it.

Practical topics covered in the code below:

- Creating, Renaming, and Casting Columns
- See [SQL in a Nutshell](./02_Spark/README.md#322-sql-in-a-nutshell)
- Filtering: SQL `WHERE`
- Selecting: SQL `SELECT`
- Grouping: SQL `GROUP BY`
- Joins
- Aggregation functions
- Functional programming: Pure functions
- Music Service Example: Data wrangling with the Python API
- Music Service Example: Data wrangling with the SQL API

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

### Functional Programming: Pure Functions

Source: [`2_spark_maps_and_lazy_evaluation.ipynb`](./02_Spark/lab/03_Data_Wrangling/2_spark_maps_and_lazy_evaluation.ipynb).

If we use functional programming, we pass functions to `map()`, `apply()` or `filter()`. These function we pass are called **pure functions** and:

- they should have no side effects on variables outside their scope,
- they should not alter the data which is being processed.

Note: 

```python
### -- Setup

# Find Spark
import findspark
findspark.init()

import pyspark

# We initialize a SparkContext (an alternative to working with Sessions)
# We use SparkContext to be able to use parallelize() later on
sc = pyspark.SparkContext(appName="maps_and_lazy_evaluation_example")

# Dataset: list of song names
log_of_songs = [
        "Despacito",
        "Nice for what",
        "No tears left to cry",
        "Despacito",
        "Havana",
        "In my feelings",
        "Nice for what",
        "Despacito",
        "All the stars"
]

# Parallelize the log_of_songs to use with Spark
# sc.parallelize() takes a list and creates an
# RDD = Resilient Distributed Dataset, i.e., 
# a dataset distributed across the Spark nodes.
# This RDD is represented by distributed_song_log
distributed_song_log = sc.parallelize(log_of_songs)

def convert_song_to_lowercase(song):
    return song.lower()

convert_song_to_lowercase("Havana") # 'havana'

# We map() our function to the RDD
# BUT it is not executed, due to the lazy evaluation principle.
# We need to run an action, e.g., collect().
# With collect() the results from all of the clusters
# are taken and gathered into a single list on the master node
distributed_song_log.map(convert_song_to_lowercase)

# With collect() the results from all of the clusters
# are taken and gathered into a single list on the master node
distributed_song_log.map(convert_song_to_lowercase).collect()
# ['despacito',
#  'nice for what',
#  'no tears left to cry',
#  'despacito',
#  'havana',
#  'in my feelings',
#  'nice for what',
#  'despacito',
#  'all the stars']

# Usually, the map() functions are defined as lambdas
# or anonymoud functions.
# Note that we are using the Pythons built-in lower() function
# inside Spark!
distributed_song_log.map(lambda song: song.lower()).collect()
```

### Music Service Example: Data Wrangling with the Python API

Source: [`4_data_wrangling.ipynb`](./02_Spark/lab/03_Data_Wrangling/4_data_wrangling.ipynb)

The Python API is similar to Pandas. Functional programming is used, i.e., we avoid loops or similar constructs, and instead we apply functions.
PySpark has also the SQL API, with the advantage that we don't need to learn a new API: we just use SQL! (see next section).

Topics:

- Select + Where
- Drop duplicates
- UDFs: user-defined functions
- Filter, group by, count, order by
- Window functions

```python
### -- 1. Setup

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf # User-Defined Function
# https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html#module-pyspark.sql.functions
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import desc
from pyspark.sql.functions import asc
from pyspark.sql.functions import sum as Fsum

import datetime

import numpy as np
import pandas as pd
%matplotlib inline
import matplotlib.pyplot as plt

spark = SparkSession \
    .builder \
    .appName("Wrangling Data") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# We can use an URL, too; e.g., "hdfs://ec2-path/my_file.json"
path = "../data/sparkify_log_small.json"
user_log = spark.read.json(path)

### -- 2. Data Exploration

# Equivalent to pd.head()
user_log.take(5)

# Column names & type
# Important columns:
# - level: paid or free; type of subscription - that's what we're interested in
# - page: where the user is: "Next Song", "Home", "Submit Downgrade"
# - userId
user_log.printSchema()
# root
#  |-- artist: string (nullable = true)
#  |-- auth: string (nullable = true)
#  |-- firstName: string (nullable = true)
#  |-- gender: string (nullable = true)
#  |-- itemInSession: long (nullable = true)
#  |-- lastName: string (nullable = true)
#  |-- length: double (nullable = true)
#  |-- level: string (nullable = true)
#  |-- location: string (nullable = true)
#  |-- method: string (nullable = true)
#  |-- page: string (nullable = true)
#  |-- registration: long (nullable = true)
#  |-- sessionId: long (nullable = true)
#  |-- song: string (nullable = true)
#  |-- status: long (nullable = true)
#  |-- ts: long (nullable = true)
#  |-- userAgent: string (nullable = true)
#  |-- userId: string (nullable = true)

# Equivalent to pd.describe()
user_log.describe().show()

# Statistics of column "artist"
user_log.describe("artist").show()

# Statistics of column "artist"
user_log.describe("sessionId").show()

user_log.count() # 1000

# Column "page", drop duplicates, sort according to content in "page"
# Later we focus on the users that ar ein the page "Submit Downgrade"
user_log.select("page").dropDuplicates().sort("page").show()

# This is equivalent to an SQL query:
# We take the columns we want (i.e., the events) for one userId
# This is a user-event log
user_log.select(["userId", "firstname", "page", "song"]).where(user_log.userId == "1046").collect()

### -- 3. Calculating Statistics by Hour

# UDF, udf = User-Defined Function
# This is the core of Functional Programming:
# We create a function which we'd like to apply to a column,
# then we use an applying method
get_hour = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000.0).hour, IntegerType())

# withColumn() returns the entire table/dataframe with a new column
# df.colName` is a Column object, and we can apply our udf to it
user_log = user_log.withColumn("hour", get_hour(user_log.ts))

user_log.head()

# Get number of songs played every hour
songs_in_hour = user_log.filter(user_log.page == "NextSong")\
                        .groupby(user_log.hour)\
                        .count()\
                        .orderBy(user_log.hour.cast("float"))

songs_in_hour.show()

# To plot the result, we need to convert it to a Pandas
# dataframe and use Matplotlib
songs_in_hour_pd = songs_in_hour.toPandas()
songs_in_hour_pd.hour = pd.to_numeric(songs_in_hour_pd.hour)

plt.scatter(songs_in_hour_pd["hour"], songs_in_hour_pd["count"])
plt.xlim(-1, 24);
plt.ylim(0, 1.2 * max(songs_in_hour_pd["count"]))
plt.xlabel("Hour")
plt.ylabel("Songs played")

### -- 4. Drop Rows with Missing Values

# Drop NAs
user_log_valid = user_log.dropna(how = "any", subset = ["userId", "sessionId"])

# There were no NAs aparently, because we still have 1000 entries
user_log_valid.count()

# There were no NAs, but we see there are some suspicious
# empty userIds
user_log.select("userId").dropDuplicates().sort("userId").show()

# We filter out the empty userIds
user_log_valid = user_log_valid.filter(user_log_valid["userId"] != "")

# Now we have less entries
user_log_valid.count()

### -- 5. Users Downgrade Their Accounts

# Let's get the entries in wich a user downgrades
# There's only one entry (because we have a small/reduced dataset); we take its userId
user_log_valid.filter("page = 'Submit Downgrade'").show()

# We investigate the events associated with this userId
# Kelly (userId 1138) played several songs after she decided to downgrade
user_log.select(["userId", "firstname", "page", "level", "song"])\
        .where(user_log.userId == "1138")\
        .collect()

# We are going to flag the transition from level paid to free
# when a user is in the page Submit Downgrade
# We created a UDF for that; we specify the output type
flag_downgrade_event = udf(lambda x: 1 if x == "Submit Downgrade" else 0, IntegerType())

# We apply the UDF and create a new column: "downgraded"
user_log_valid = user_log_valid.withColumn("downgraded", flag_downgrade_event("page"))

user_log_valid.head()

# The example goes beyond and computes a "phase" column in which a user is.
# In general, a user could be in several phases: trial, paid, free, etc.
# In this example, we have only 2 phase: 1 (paid), 0 (free); however,
# case could be extended to more phases.
# Here, it seems a bit of a over-complication, but in more complex cases,
# that's how we can operate.
# To compute the phase, we use trick:
# - we sort in chronologically descending order the entries of a user
# - we compute a cummulative sum of the "downgraded" column using a window function
# ???
from pyspark.sql import Window

# Take user entries in descending order and consider the window of preceeding values,
# take all previous rows but no rows afterwards
# ???
windowval = Window.partitionBy("userId")\
                  .orderBy(desc("ts"))\
                  .rangeBetween(Window.unboundedPreceding, 0) # 

# Create phase column
user_log_valid = user_log_valid.withColumn("phase", Fsum("downgraded")\
                               .over(windowval))

# If we have more than one phase, we'll see the phase values decreasing
user_log_valid.select(["userId", "firstname", "ts", "page", "level", "phase"])\
              .where(user_log.userId == "1138")\
              .sort("ts")\
              .collect()
# [Row(userId='1138', firstname='Kelly', ts=1513729066284, page='Home', level='paid', phase=1),
#  Row(userId='1138', firstname='Kelly', ts=1513729066284, page='NextSong', level='paid', phase=1),
#  Row(userId='1138', firstname='Kelly', ts=1513729313284, page='NextSong', level='paid', phase=1),
#  Row(userId='1138', firstname='Kelly', ts=1513729552284, page='NextSong', level='paid', phase=1),
#  ...
#  Row(userId='1138', firstname='Kelly', ts=1513821430284, page='Home', level='free', phase=0),
#  Row(userId='1138', firstname='Kelly', ts=1513833144284, page='NextSong', level='free', phase=0)]
```

### Music Service Example: Data Wrangling with the SQL API

Source: [`7_data_wrangling-sql.ipynb`](./02_Spark/lab/03_Data_Wrangling/7_data_wrangling-sql.ipynb).

Instead of using the Python API, we can use the SQL API: with it, we just use SQL, no need to learn any new API!

The major difference when using SQL is that we do need to register the uploaded Dataframe using `df.createOrReplaceTempView("tableName")`. This step creates a temporary view in Spark which allows to query SQL-like statements to analyze the data.

Also, note that we can chain several retrieval/analysis functions one after the other. However, these are not executed due to the lazy evaluation principle until we `show()` or `collect()` them:

- `show()` returns a dataframe with n (20, default) entries from the RDD; use for exploration.
- `collect()` returns the complete result/table from the RDD in Row elements; use only when needed.

```python
### -- 1. Setup

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import desc
from pyspark.sql.functions import asc
from pyspark.sql.functions import sum as Fsum

import datetime

import numpy as np
import pandas as pd
%matplotlib inline
import matplotlib.pyplot as plt

spark = SparkSession \
    .builder \
    .appName("Data wrangling with Spark SQL") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

path = "../data/sparkify_log_small.json"
user_log = spark.read.json(path)

user_log.take(1)

user_log.printSchema()

### -- 2. Create a View And Run Queries

# Register the DataFrame as a temporary view.
# This is necessary for SQL data wrangling.
# This allows us to query the data using SQL-like syntax in the used session.
# Notes:
# - The contents in user_log are not registered in the session catalog, by default!
# - user_log is linked to the session
# - We create a temporary view of user_log named "user_log_table" in the catalog
user_log.createOrReplaceTempView("user_log_table")

# Once the table(s) we want have been uploaded and registered,
# the interface is .sql()
# BUT because of the *lazy evaluation*,
# we need to either show() or collect():
# - show() returns a dataframe with n (20, default) entries from the RDD; use for exploration
# - collect() returns the complete result/table in Row elements; use only when needed
spark.sql("SELECT * FROM user_log_table LIMIT 2").show()

# Multi-line queries
spark.sql('''
          SELECT * 
          FROM user_log_table 
          LIMIT 2
          '''
          ).show()

spark.sql('''
          SELECT COUNT(*) 
          FROM user_log_table 
          '''
          ).show()

spark.sql('''
          SELECT userID, firstname, page, song
          FROM user_log_table 
          WHERE userID == '1046'
          '''
          ).collect()

# All unique pages
spark.sql('''
          SELECT DISTINCT page
          FROM user_log_table 
          ORDER BY page ASC
          '''
          ).show()

### -- 3. User Defined Functions

# We can also use User-Defined Functions (UDFs)
# but we need to register them to we used
# as part of the SQL statement
spark.udf.register("get_hour",
                   lambda x: int(datetime.datetime.fromtimestamp(x / 1000.0).hour),
                   IntegerType())

spark.sql('''
          SELECT *, get_hour(ts) AS hour
          FROM user_log_table 
          LIMIT 1
          '''
          ).collect()

# SQL statement with the freshly defined UDF
# Note that the statement is not evaluated
# due to the *lazy evaluation* principle.
# We need to show/collect the query to get the results.
songs_in_hour = spark.sql('''
          SELECT get_hour(ts) AS hour, COUNT(*) as plays_per_hour
          FROM user_log_table
          WHERE page = "NextSong"
          GROUP BY hour
          ORDER BY cast(hour as int) ASC
          '''
          )
songs_in_hour.show()

### -- 4. Converting Results to Pandas

# The chain of statements/requests
# is also executed and the result
# transformed into a pd.DataFrame with toPandas()
songs_in_hour_pd = songs_in_hour.toPandas()
print(songs_in_hour_pd)
#    hour  plays_per_hour
# 0     0             456
# 1     1             454
# 2     2             382
# ...

```

## Machine Learning

Source: [`03_Machine_Learning.ipynb`](./02_Spark/lab/02_Intro_PySpark/03_Machine_Learning.ipynb).

In this section, basic data processing is performed in form of a pipeline and a logistic regression model is trained with grid search.

We have two types of classes defined in the module `pyspark.ml`:

- `Transformer` classes: they take a Spark SQL Dataframe and `.transform()` it to yield a new Spark SQL Dataframe.
- `Estimator` classes: they take a Spark SQL Dataframe and `.fit()` a model to it to deliver back an object, which can be a trained `Transformer` ready to `transform()` the data. For instance, a model is an `Estimator` which returns a `Transformer`; then, scoring a model consists in calling `transform()` on the returned `Transformer` using the desired dataset.

In spark, data processing and feature engineering and done with **Pipelines**:

- First we perform all data processing we need on the data frames: joining, casting, missing values, etc.
- Then, we perform feature engineering by defining all `Transformer` objects we need with their inputs and output: encoding, etc.
- After that we define the feature vector from the resulting data frame with `VectorAssembler` and assemble a `Pipeline` which contains all the `Transfomer` objects.
- Finally, the `Pipeline` is used for fitting a model.

### Data Processing Pipeline

Source: [`03_Machine_Learning.ipynb`](./02_Spark/lab/02_Intro_PySpark/03_Machine_Learning.ipynb)

Steps:

- Load data
- Data processing
  - Join tables
  - Cast types: numeric values are required for modeling
- Feature engineering
  - New Features/Columns
  - Remove Missing Values
  - Encode Categoricals
- Pipeline: Assemble a Vector and Create a Pipeline
  - Fit and Transform the Pipeline
  - Train/Test Split
- Modeling
  - Model instantiation (e.g., logistic regression)
  - Cross-validation and hyperparameter space sampler (grid search)
  - Fitting / training
  - Prediction
  - Evaluation

#### Load Data

```python
import findspark
findspark.init()

# Import SparkSession from pyspark.sql
from pyspark.sql import SparkSession

# Create or get a (new) SparkSession: session
session = SparkSession.builder.getOrCreate()

# Print session: our SparkSession
print(session)

# Load and register flights dataframe
flights = session.read.csv("../data/flights_small.csv", header=True, inferSchema=True)
flights.createOrReplaceTempView("flights")

# Load and register airports dataframe
airports = session.read.csv("../data/airports.csv", header=True, inferSchema=True)
airports.createOrReplaceTempView("airports")

# Load and register planes dataframe
planes = session.read.csv("../data/planes.csv", header=True, inferSchema=True)
planes.createOrReplaceTempView("planes")

print(session.catalog.listTables()) # airports, flights, planes
```

#### Data processing

```python
# Rename year column
planes = planes.withColumnRenamed("year", "plane_year")

# Join the DataFrames
model_data = flights.join(planes, on="tailnum", how="leftouter")

# Cast the columns to integers
model_data = model_data.withColumn("arr_delay", model_data.arr_delay.cast("integer"))
model_data = model_data.withColumn("air_time", model_data.air_time.cast("integer"))
model_data = model_data.withColumn("month", model_data.month.cast("integer"))
model_data = model_data.withColumn("plane_year", model_data.plane_year.cast("integer"))
model_data.printSchema()
```

#### Feature Engineering

```python
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml import Pipeline

# Create the column plane_age
model_data = model_data.withColumn("plane_age", model_data.year - model_data.plane_year)

# Create is_late
model_data = model_data.withColumn("is_late", model_data.arr_delay > 0)

# Convert to an integer: booleans need to be converted to integers, too
model_data = model_data.withColumn("label", model_data.is_late.cast("integer"))

# Remove missing values
model_data = model_data.filter("""arr_delay is not NULL 
                                  and dep_delay is not NULL
                                  and air_time is not NULL
                                  and plane_year is not NULL""")

# Create a StringIndexer: Estimator that needs to be fit() and returns a Transformer
# StringIndexer: map all unique categorical levels to numbers
carr_indexer = StringIndexer(inputCol="carrier",
                             outputCol="carrier_index")

# Create a OneHotEncoder: Estimator that needs to be fit() and returns a Transformer
carr_encoder = OneHotEncoder(inputCol="carrier_index",
                             outputCol="carrier_fact")

# Create a StringIndexer: Estimator that needs to be fit() and returns a Transformer
# StringIndexer: map all unique categorical levels to numbers
dest_indexer = StringIndexer(inputCol="dest",
                             outputCol="dest_index")

# Create a OneHotEncoder: Estimator that needs to be fit() and returns a Transformer
dest_encoder = OneHotEncoder(inputCol="dest_index",
                             outputCol="dest_fact")
```

#### Pipeline

```python
# Make a VectorAssembler: Transformer
vec_assembler = VectorAssembler(inputCols=["month",
                                           "air_time",
                                           "carrier_fact",
                                           "dest_fact",
                                           "plane_age"],
                                outputCol="features")

# Make the pipeline: we append in series all the Estimator/Transformer objects
# and the VectorAssembler
flights_pipe = Pipeline(stages=[dest_indexer,
                                dest_encoder,
                                carr_indexer,
                                carr_encoder,
                                vec_assembler])

# Fit and transform the data:
# - first, the Estimators are fit, which generate trained Transformers
# - then, the dataset is passed through the trained Transformers
piped_data = flights_pipe.fit(model_data).transform(model_data)
piped_data.printSchema()

# Split the data into training and test sets
# train 60%, test 40%
# Always split after the complete dataset has been processed!
training, test = piped_data.randomSplit([.6, .4])
```

#### Modeling

```python
# Import LogisticRegression: Estimator
from pyspark.ml.classification import LogisticRegression
# Import the evaluation submodule
import pyspark.ml.evaluation as evals
# Import the tuning submodule
import numpy as np
import pyspark.ml.tuning as tune

# Create a LogisticRegression Estimator
lr = LogisticRegression()

# Create a BinaryClassificationEvaluator
evaluator = evals.BinaryClassificationEvaluator(metricName="areaUnderROC")

# Create the parameter grid
grid = tune.ParamGridBuilder()

# Add the hyperparameters to be tried in the grid
grid = grid.addGrid(lr.regParam, np.arange(0, .1, .01))
grid = grid.addGrid(lr.elasticNetParam, [0, 1])

# Build the grid
grid = grid.build()

# Create the CrossValidator
cv = tune.CrossValidator(estimator=lr,
                         estimatorParamMaps=grid,
                         evaluator=evaluator)

# Fit cross validation models
models = cv.fit(training)

# Extract the best model
best_lr = models.bestModel

# We can also train the model
# without cross validation and grid search
not_best_lr = lr.fit(training)

# Use the model to predict the test set
# Note that the model does not have a predict() function
# but it transforms() the data into predictions!
test_results = best_lr.transform(test)

# Evaluate the predictions
print(evaluator.evaluate(test_results))
```
