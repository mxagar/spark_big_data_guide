# Spark

I made these notes while following the Udacity course [Spark](https://www.udacity.com/course/learn-spark-at-udacity--ud2002).

Additionally, I extended the  notes with contents from the following courses at the Datacamp track [Big Data with PySpark](https://app.datacamp.com/learn/skill-tracks/big-data-with-pyspark):

- Introduction to PySpark
- Big Data Fundamentals with PySpark
- Cleaning Data with PySpark
- Feature Engineering with PySpark
- Machine Learning with PySpark
- Building Recommendation Engines with PySpark

Mikel Sagardia, 2023.  
No guarantees.

Table of contents:

- [Spark](#spark)
  - [1. Introduction](#1-introduction)
    - [Udacity Project Overview](#udacity-project-overview)
    - [Installation and Setup](#installation-and-setup)
  - [2. The Power of Spark](#2-the-power-of-spark)
    - [Hardware in Numbers](#hardware-in-numbers)
      - [Examples](#examples)
    - [Big Data in Numbers](#big-data-in-numbers)
    - [Hadoop and Spark](#hadoop-and-spark)
    - [MapReduce](#mapreduce)
    - [Spark Modes](#spark-modes)
    - [Spark Use Cases](#spark-use-cases)
  - [3. Introduction to PySpark](#3-introduction-to-pyspark)
    - [3.1 Basics: Getting to know PySpark](#31-basics-getting-to-know-pyspark)
      - [3.1.1 Setup](#311-setup)
        - [Install and Run PySpark](#install-and-run-pyspark)
        - [Running on a Notebook](#running-on-a-notebook)
      - [3.1.2 Creating a Spark Session](#312-creating-a-spark-session)
      - [3.1.3 Spark SQL Dataframes: Uploading and Consulting](#313-spark-sql-dataframes-uploading-and-consulting)
      - [3.1.4 Common Methods and Attributes of the SQL Dataframe](#314-common-methods-and-attributes-of-the-sql-dataframe)
      - [3.1.5 SQL Queries](#315-sql-queries)
      - [3.1.6 Pandafying: Convert a Spark SQL Dataframe into a Pandas Dataframe](#316-pandafying-convert-a-spark-sql-dataframe-into-a-pandas-dataframe)
      - [3.1.7 Sparkifying: Convert a Pandas Dataframe into a Spark SQL Dataframe](#317-sparkifying-convert-a-pandas-dataframe-into-a-spark-sql-dataframe)
    - [3.2 Manipulating Data](#32-manipulating-data)
      - [Setup: Create Session + Upload Data](#setup-create-session--upload-data)
      - [3.2.1 Creating, Renaming, and Casting Columns](#321-creating-renaming-and-casting-columns)
      - [3.2.2 SQL in a Nutshell](#322-sql-in-a-nutshell)
      - [3.2.3 Filtering Data: WHERE - filter()](#323-filtering-data-where---filter)
      - [3.2.4 Selecting Columns: SELECT - select(), selectExpr(), alias()](#324-selecting-columns-select---select-selectexpr-alias)
      - [3.2.5 Grouping and Aggregating: GROUP BY, MIN, MAX, COUNT, SUM, AVG, AGG](#325-grouping-and-aggregating-group-by-min-max-count-sum-avg-agg)
        - [Aggregation Functions](#aggregation-functions)
      - [3.2..6 Joining: JOIN - join()](#326-joining-join---join)
    - [3.3 Introduction to Machine Learning Pipelines](#33-introduction-to-machine-learning-pipelines)
      - [Setup: Create Session + Upload Data](#setup-create-session--upload-data-1)
      - [3.3.1 Introduction to Machine Learning in Spark](#331-introduction-to-machine-learning-in-spark)
      - [3.3.2 Data Processing Pipeline](#332-data-processing-pipeline)
        - [Join](#join)
        - [Cast Types](#cast-types)
        - [New Features/Columns](#new-featurescolumns)
        - [Remove Missing Values](#remove-missing-values)
        - [Encode Categoricals](#encode-categoricals)
        - [Assemble a Vector and Create a Pipeline](#assemble-a-vector-and-create-a-pipeline)
        - [Fit and Transform the Pipeline](#fit-and-transform-the-pipeline)
        - [Train/Test Split](#traintest-split)
      - [3.3.3 Model Tuning and Selection](#333-model-tuning-and-selection)
        - [Instantiate Logistic Regression Model](#instantiate-logistic-regression-model)
        - [Instantiate Evaluation Metric](#instantiate-evaluation-metric)
        - [Instantiate Parameter Grid](#instantiate-parameter-grid)
        - [Cross Validation Objec](#cross-validation-objec)
        - [Fit the Model with Grid Search](#fit-the-model-with-grid-search)
        - [Evaluate the Model](#evaluate-the-model)
  - [4. Data Wrangling with Spark](#4-data-wrangling-with-spark)
    - [4.1 Functional Programming](#41-functional-programming)
  - [5. Setting up Spark Clusters with AWS](#5-setting-up-spark-clusters-with-aws)
  - [6. Debugging and Optimization](#6-debugging-and-optimization)
  - [7. Machine Learning with PySpark](#7-machine-learning-with-pyspark)

## 1. Introduction

This module is an introductory module of Udacity, where the basic course structure is explained, as well as a project.

Section videos:

- [Welcome](https://www.youtube.com/watch?v=D7vrej8uLzs&t=28s)
- [Instructors](https://www.youtube.com/watch?v=1h6ToHj7mcM)
- [Course Overview](https://www.youtube.com/watch?v=P7YAisWITAs)
- [Project Overview](https://www.youtube.com/watch?v=lPCzCEG2yRs)

### Udacity Project Overview

Video: [Project Overview](https://www.youtube.com/watch?v=lPCzCEG2yRs)

I made a dedicated repository for the Udacity final project: [sparkify_customer_churn](https://github.com/mxagar/sparkify_customer_churn). I have non-committed link to that repository in the folder [`lab`](./lab/); additionally, all coding examples from this module are collected in that folder [`lab`](./lab/).

Key ideas of the project:

- Music streaming service, similar to Spotify: Users can listen to streamed music.
- We have: (1) free-tier, (2) subscription plan.
- Every time an user is involved in an event, it is logged with a timestamp; example events: songplay, logout, like, ad_heard, downgrade, etc.
- Goal: predict churn, either (1) as downgrade from premium to free or (2) as leaving the service.
- With churn predictions, the company can target those users with incentives: discounts, etc.

### Installation and Setup

There are two major modes to run Spark:

- Locally
- In a cluster

To leverage the power of distributed system for big data, we need to use the cluster-mode; however, by running Spark on our desktop computer (i.e., locally on one machine) helps test an learn the framework.

- To see how to install and use PySpark locally, check Section [3. Introduction to PySpark](#3-introduction-to-pyspark).
- To set a Spark cluster on AWS, check Section [5. Setting up Spark Clusters with AWS](#5-setting-up-spark-clusters-with-aws).

## 2. The Power of Spark

Module videos:

- [Introduction to Spark](https://www.youtube.com/watch?v=RWtS_ErlmXE)
- [What Qualifies As Big Data](https://www.youtube.com/watch?v=tGHCCvKKpuo)
- [Numbers Everyone Should Know](https://www.youtube.com/watch?v=XGQT-uzt4v8)
- [Numbers Everyone Should Know: CPU](https://www.youtube.com/watch?v=LNv-urROvr0)
- [Numbers Everyone Should Know: Memory](https://www.youtube.com/watch?v=Wvz1UeYkjsw)
- [Memory Efficiency](https://www.youtube.com/watch?v=Gx0_7CUFInM)
- [Numbers Everyone Should Know: Storage](https://www.youtube.com/watch?v=3nL6JM3QbQQ)
- [Numbers Everyone Should Know: Network](https://www.youtube.com/watch?v=MP9fIYT5Vvg)
- [Hardware: Key Ratios](https://www.youtube.com/watch?v=VPVGYKQcG7Q)
- [Big Data Numbers Part 1](https://www.youtube.com/watch?v=314zCU4O-f4)
- [Big Data Numbers Part 2](https://www.youtube.com/watch?v=QjPr7qeJTQk)
- [Medium Data Numbers](https://www.youtube.com/watch?v=5E0VLIhch6I)
- [History Of Parallel Computing Distributed Systems](https://www.youtube.com/watch?v=9CRZURNg2zs)
- [The Hadoop Ecosystem And New Technologies For Data Processing](https://www.youtube.com/watch?v=0CgMtPYwLR8)
- [MapReduce](https://www.youtube.com/watch?v=ErgNIy7z4SE)
- [Spark Cluster Configuration](https://www.youtube.com/watch?v=DpPD5hhvspg)

### Hardware in Numbers

The numbers everyone should know (Peter Norveig, Google): We should learn at least the following hardware-related speed numbers and concepts:

- **CPU** operation: 0.4 ns
  - Mutex lock/unlock: 17 ns
  - Registers store small pieces of data that the CPU is crunching at the moment.
  - However, the CPU sits idle most of the time, because the bottleneck is the data access from the memory.
- **Memory** (RAM) reference: 100 ns
  - Read 1 MB sequentially in memory: 3 microsec.
  - Getting data from memory is 250x slower than the CPU! We need the memory to pass the data to the CPU, since the registers contain only small amounts of data, which is being crunched at the moment.
  - Data arrangement in memory is important: is all data is sequential, loading it to the CPU is much faster!
  - Memory is ephemeral (it does not persist) and expensive.
  - Google was a pioneer at preferring distributed systems built with commodity hardware instead of large and expensive memory-equipped systems; today, Google's model is an industry standard.
- **Storage**: Random read from SSD: 16 microsec.
  - Read 1 MB sequentially in memory: 49 microsec.
  - On average, SSDs are 15x slower than memory.
  - Spark is designed to avoid using disk; instead, it uses memory.
- **Network**: Round trip data from EU to US: 150 millisec.
  - Send 2 KB in local commodity network: 44 ns
  - It is currently the bottleneck in moving data.
  - Spark needs to move data around, because it uses distributed systems.

![Hardware Numbers](./pics/hardware_numbers.jpg)

![Hardware Numbers: Most Important](./pics/hardware_numbers_important.jpg)

![Hardware Numbers: Key Ratios](./pics/hardware_key_ratios.jpg)

**Summary of key ratios:**

- **CPU is 200x faster than memory**
- **Memory is 15x faster than SSD**
- **SSD is 20x faster than Network**

#### Examples

> A 2.5 Gigahertz CPU means that the CPU processes 2.5 billion operations per second. Let's say that for each operation, the CPU processes 8 bytes of data. How many bytes could this CPU process per second? 
>
> **Answer**: 2.5 10^9 * 8 byte / sec = 20 10^9 byte / sec. However, not all that data can be loaded instantaneously from memory!

> Twitter generates about 6,000 tweets per second, and each tweet contains 200 bytes. So in one day, Twitter generates data on the order of:
>
> (6000 tweets / second) x (86400 seconds / day) x (200 bytes / tweet) = 104 billion bytes / day
>
> Knowing that tweets create approximately 104 billion bytes of data per day, how long would it take the 2.5 GigaHertz CPU to analyze a full day of tweets?
>
> **Answer**: 104 / 20 = 5.2 sec. However, not all that data can be loaded instantaneously from memory!

Links:

- [Peter Norveig: Teach Yourself Programming in Ten Years](http://norvig.com/21-days.html)
- [Interactive: Latency Numbers Every Programmer Should Know](https://colin-scott.github.io/personal_website/research/interactive_latency.html)

### Big Data in Numbers

Characteristics of Big Data:

- Data stored in several machines, distributed.
- High volume of data.
- High speed of data, velocity.
- High variety of data.

If we have a dataset of 4 GB and a computer memory of 8 GB, the problem is **not a big data** problem, because the data fits in memory.

If we have a dataset which does not fit in memory by large, e.g., of a size of 200 GB, then, we do have a **big data** problem. Those sizes easily appear in logs of web services that have a considerable user base, e.g., in the music streaming project introduced before.

When we have a dataset of 200 GB, a single computer will start processing it but collapse:

- First 8 GB are loaded to memory, then CPU, and processed by CPU
- Processed data (e.g., 15 MB) is returned to memory, then, SSD
- Next 8 GB are fetched and loaded.
- The CPU is switching the context very often, not really processing the data most of the time; that context-switching is called *thrashing* and causes the process to collapse.

Also, note that Pandas has a functionality to read a dataset in chunks: [Iterating through files chunk by chunk](https://pandas.pydata.org/pandas-docs/stable/user_guide/io.html#io-chunking). In cases where the dataset slightly exceeds the size of the memory, it might make sense to use that functionality to process the dataset in chunks.

### Hadoop and Spark

Probably, the boom of Big Data started with the release of Hadoop, which was implemented after the publication of the MapReduce paper by Jeff Dean.

See the other module on Hadoop: [`01_Intro_Hadoop`](../01_Intro_Hadoop).

Hadoop is older than Spark; Spark uses the concepts introduced in Hadoop. Most important elements of Hadoop:

- Hadoop Distributed File System, HDFS: storage, distributed.
- Hadoop MapReduce: processing in parallel/distributed systems.
- Hadoop YARN: resource management.

Other tools:

- Apache Pig: SQL-like language running on top of Hadoop MapReduce.
- Apache Hive: also an SQL-like interface.

Spark vs. Hadoop:

- Spark is an a newer and equivalent framework to Hadoop.
- Spark is faster than Hadoop; this is mainly because Spark works with data in memory, avoiding to write them to disk (as done by Hadoop).
- Spark does not have a file system: we can use HDFS if we want, or Amazon S3, for instance.
- Spark comes with ML, SQL and Streaming libraries (although Flink and Storm are faster), among others.

### MapReduce

MapReduce is a way of processing data in parallel; for instance, we have a large dataset of logs related to the events in the music streaming service. We want to know the ranking of most listened songs. The MapReduce process works as follows:

- The data is stored in a distributed manner (in chunks) in commodity nodes. We know where each chunk is, and there is also redundancy.
- To create a ranking, we need to count the reproduction of each song.
- In each node with a chunk, we iterate through all lines and create tuples or key-value pairs which contain the song name and a counter of play, i.e., `1`: `(Let it be, 1)`. That process is **map**.
- Then, all tuples are **shuffled**, i.e., all pairs with the same key are packed together.
- Finally, all stacked pairs are aggregated: we count the number of pairs for each key.

Example: [`lab/01_map_reduce/`](./lab/01_map_reduce/).

![MapReduce Process](./pics/map_reduce_process.jpg)

### Spark Modes

Spark can work in two major modes:

- Local-mode: we install Spark on our computer and do everything there.
- Cluster-mode: we create a cluster of nodes and leverage the distributed computing capabilities for big data.

Obviously, we want to use the cluster-mode; the local-mode is used to learn and test our implementations.

Additionally, we have:

- A **master node**, which has the **driver program**, and within it sits the **SparkContext**. We always have and interact with the Spark context.
- The Spark context talks to the **cluster manager**, which is outside from the **master node**. That manager can be, for instance Yarn and it takes care of the resource distribution.
- The **cluster manager** handles the **worker nodes**, which are independent from the manager and are usually distributed. It requests containers with certain capacities within them depending on the workload.

In the cluster-mode, we can have several cluster managers:

- Stand-alone, i.e., Spark itself.
- Apache Mesos.
- Hadoop Yarn.
- Kubernetes.

![Spark Modes](./pics/spark_modes.jpg)

![Spark Architecture](../00_Intro_Big_Data/pics/spark_architecture.jpeg)

### Spark Use Cases

Typical usage with large, distributed datasets:

- [Data analytics](https://spark.apache.org/sql/)
- [Machine learning](https://spark.apache.org/mllib/)
- [Streaming](https://spark.apache.org/streaming/)
- [Graph analytics](https://spark.apache.org/graphx/)

Limitations of Spark:

- Streaming has a latency of 500 ms; faster alternatives are: [Apache Storm](https://storm.apache.org), [Apex](https://apex.apache.org), [Flink](https://flink.apache.org).
- Deep learning is not available, but there are projects which integrate, e.g., Spark with Tensorflow.
- Machine learning algorithms that scale linearly with data are possible only.

## 3. Introduction to PySpark

This section is based on the Datacamp course [Introduction to PySpark](https://app.datacamp.com/learn/courses/introduction-to-pyspark). The course has the following chapters:

1. Basics: Getting to know PySpark
2. Manipulating data
3. Getting started with machine learning pipelines
4. Model tuning and selection

The code and notebooks of this section are in [`lab/02_Intro_PySpark`](./lab/02_Intro_PySpark).

### 3.1 Basics: Getting to know PySpark

This entire section is contained in one single notebook and objects in one subsection might be defined in subsections prior to it.

The notebook: [`01_Basics.ipynb`](./lab/02_Intro_PySpark/01_Basics.ipynb).

#### 3.1.1 Setup

##### Install and Run PySpark

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

##### Running on a Notebook

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

#### 3.1.2 Creating a Spark Session

Creating multiple `SparkSession`s and `SparkContext`s can cause issues, use the `SparkSession.builder.getOrCreate()` method instead, which returns an existing `SparkSession` if there's already one in the environment, or creates a new one if necessary. Usually, in a notebook, we run first

```python
import findspark
findspark.init()
```

And then, we create a session and work with it:

```python
# Import SparkSession from pyspark.sql
from pyspark.sql import SparkSession

# Create or get a (new) SparkSession: session
session = SparkSession.builder.getOrCreate()

# Print session: our SparkSession
print(session) # <pyspark.sql.session.SparkSession object at 0x7f8128abd210>
```

If we shut down the notebook/kernel, the session disappears.

#### 3.1.3 Spark SQL Dataframes: Uploading and Consulting

One of the advantages of Spark is that we can consult the data one the cluster with SQL-like queries. To that end, first we need to upload the data and check that it's accessible. Here, a CSV file is uploaded to Spark and registered as a temporary table view in the `session`. The resulting object we get in the Python environment is a Spark SQL dataframe. In later sections, instead of directly reading from CSV files, Pandas dataframes are converted to Spark SQL dataframes.

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
```

#### 3.1.4 Common Methods and Attributes of the SQL Dataframe

You can use several attributes and methods of the `flights_df` DataFrame object to explore the data. Here are some of the most important ones (ficticious column values):

- `printSchema()`: This method prints the schema of the DataFrame, which shows the column names and their data types.

- `show()`: This method displays the first n rows of the DataFrame in a tabular format. You can specify the number of rows to show using the n parameter (default is 20). For example, `flights_df.show(5)` will show the first 5 rows of the DataFrame.

- `head()`: This method returns the first n rows of the DataFrame as a list of Row objects. You can specify the number of rows to return using the n parameter (default is 1).

- `count()`: This method returns the number of rows in the DataFrame.

- `columns`: This attribute returns a list of the column names in the DataFrame.

- `dtypes`: This attribute returns a list of tuples, where each tuple contains the column name and its data type

- `describe()`: This method computes summary statistics for the numerical columns in the DataFrame, such as count, mean, standard deviation, minimum, and maximum values.

- `select()`: This method allows you to select one or more columns from the DataFrame. For example, `flights_df.select("origin", "dest").show()` will display the "origin" and "dest" columns of the DataFrame.

- `filter()`: This method allows you to filter the rows of the DataFrame based on a condition. For example, `flights_df.filter(flights_df["delay"] > 0).show()` will display the rows where the "delay" column is greater than 0.

- `groupBy()`: This method allows you to group the rows of the DataFrame by one or more columns and perform an aggregation operation, such as sum, count, or average. For example, `flights_df.groupBy("origin").count().show()` will display the number of flights for each origin airport.

- `agg()`: This method allows you to perform one or more aggregation operations on the DataFrame. For example, `flights_df.agg({"delay": "mean", "distance": "max"}).show()` will display the mean delay and maximum distance of all flights in the DataFrame.

- `join()`: This method allows you to join two DataFrames based on a common column. For example, `flights_df.join(airports_df, flights_df["dest"] == airports_df["iata"]).show()` will join the "flights_df" DataFrame with the "airports_df" DataFrame on the "dest" column.

#### 3.1.5 SQL Queries

One of the big advantages of Spark is that we can access the data on the cluster using SQL-like queries! We get as response a Spark SQL dataframe.

```python
# SQL query
query = "FROM flights SELECT * LIMIT 10"

# Get the first 10 rows of flights
# The returning object is a Spark SQL dataframe, as flights_df
# but this time it contains only 10 rows
flights10 = session.sql(query)

# Show the results: equivalent to the pandas .head(20)
flights10.show()
```

#### 3.1.6 Pandafying: Convert a Spark SQL Dataframe into a Pandas Dataframe

Sometimes, it is more convenient to work **locally** with a Pandas dataframe! A common use-case is when we have computed a table with aggregated values, for instance.

```python
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
```

#### 3.1.7 Sparkifying: Convert a Pandas Dataframe into a Spark SQL Dataframe

In previous sections, a CSV was directly loaded to Spark andd the resulting Spark SQL Dataframe registered as a temporary view to the catalog of the session.

Now, instead of reading CSV tables from Spark, we upload Pandas dataframes.

```python
import pandas as pd

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

### 3.2 Manipulating Data

This chapter deals with methods for Spark SQL dataframe manipulation. Many of these methods have an equivalent SQL operator. check my [SQL guide](https://github.com/mxagar/sql_guide) if you need a refresher.

The notebook: [`02_Manipulating_Data.ipynb`](./lab/02_Intro_PySpark/02_Manipulating_Data.ipynb).

#### Setup: Create Session + Upload Data

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

#### 3.2.1 Creating, Renaming, and Casting Columns

Once we get the Spark SQL dataframe, we can add a new column to it with

```python
df = df.withColumn("newCol", df.oldCol + 1)
```

However, Spark SQL dataframes are inmutable, i.e., we are creating a new dataframe. Notes:

- `df.colName` is a `Column` object, which comes up often. We can also convert a column name string into a `Column` with `pyspark.sql.functions.col`.
- `withColumn()` returns the **entire table/dataframe** with a new column. If we want to change a column content, we need to write `"oldCol"` instead of `"newCol"` in the first argument. We can use it to rename columns, too. The second argment **must** be a `Column` object, created as `df.colName` or `col("colName")`.

It might happen that we need to cast the type of a column; to check the types we use `printSchema()` and to cast 

```python
from pyspark.sql.functions import col

# Print schema with types
df.printSchema()

# Cast from string to double: new table is created
df = df.withColumn("air_time", col("air_time").cast("double"))
df = df.withColumn("air_time", df.arr_delay.cast("double"))

# Rename column "air_time" -> "flight_duration"
# BUT, the old column is still there if it has another name;
# we can drop it using .select(), as shown below
df = df.withColumn("flight_duration", flights.air_time)

# Another way to rename column names:
# this function allows to use two column name strings
# AND replaces the previous column
df = df.withColumnRenamed("flight_duration", "air_time")
```

Notebook examples:

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
```

#### 3.2.2 SQL in a Nutshell

Many Spark SQL Dataframe methods have an equivalent SQL operation.

Most common SQL operators: `SELECT`, `FROM`, `WHERE`, `AS`, `GROUP BY`, `COUNT()`, `AVG()`, etc.

```sql
-- Get all the contents from the table my_table: we get a table
SELECT * FROM my_table;

-- Get specified columns and compute a new column value: we get a table
SELECT origin, dest, air_time / 60 FROM flights;

-- Filter according to value in column: we get a table
SELECT * FROM students
WHERE grade = 'A';

-- Get the table which contains the destination and tail number of flights that last +10h
SELECT dest, tail_num FROM flights WHERE air_time > 600;

-- Group by: group by category values and apply an aggregation function for each group
-- In this case: number of flights for each unique origin
SELECT COUNT(*) FROM flights
GROUP BY origin;

-- Group by all unique combinations of origin and dest columns
SELECT origin, dest, COUNT(*) FROM flights
GROUP BY origin, dest;

-- Group by unique origin-carrier combinations and for each
-- compute average air time in hrs
SELECT AVG(air_time) / 60 FROM flights
GROUP BY origin, carrier;

-- Flight duration in hrs, new column name
SELECT air_time / 60 AS duration_hrs
FROM flights
```

Also, recall we can combine tables in SQL using the `JOIN` operator:

```sql
-- INNER JOIN: note it is symmetrical, we can interchange TableA and B
SELECT * FROM TableA
INNER JOIN TableB
ON TableA.col_match = TableB.col_match;

-- FULL OUTER JOIN
SELECT * FROM TableA
FULL OUTER JOIN TableB
ON TableA.col_match = Table_B.col_match

-- LEFT OUTER JOIN
-- Left table: TableA; Right table: TableB
SELECT * FROM TableA
LEFT OUTER JOIN TableB
ON TableA.col_match = Table_B.col_match

-- RIGHT OUTER JOIN
-- Left table: TableA; Right table: TableB
SELECT * FROM TableA
RIGHT OUTER JOIN TableB
ON TableA.col_match = Table_B.col_match
```

#### 3.2.3 Filtering Data: WHERE - filter()

The `filter()` function is equivalent to `WHERE`. We can pass either a string we would write after `WHERE` or we can use the typical Python syntax:

```sql
SELECT * FROM flights WHERE air_time > 120
```

```python
flights.filter("air_time > 120").show()
flights.filter(flights.air_time > 120).show()
```

Notebook example:

```python
# Filter flights by passing a string
long_flights1 = flights.filter("distance > 1000")

# Filter flights by passing a column of boolean values
long_flights2 = flights.filter(flights.distance > 1000)

# Print the data to check they're equal
long_flights1.show(2)
long_flights2.show(2)
```

#### 3.2.4 Selecting Columns: SELECT - select(), selectExpr(), alias()

The `select()` method is equivalent to the `SELECT` SQL operator: it can take several comma-separated column names or `Column` objects (`df.col`) and returns a table with them. In contrast, the `withColumn()` method returns the entire table. Therefore, if we want to drop unnecessary columns, we can use `select()`.

If we want to perform a more sophisticated selection, as in SQL, we can use `selectExpr()` and pass comma-separated SQL strings; if we want to change the name of the selected/transformed column, we can use `alias()`, equivalent to `AS` in SQL.

Notebook examples:

```python
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
```

#### 3.2.5 Grouping and Aggregating: GROUP BY, MIN, MAX, COUNT, SUM, AVG, AGG

Similarly as it is done in SQL, we can create a `pyspark.sql.GroupedData` object with `groupBy()` and then apply aggregation functions like `min()`, `max()`, `count()`, `sum()`, `avg()`, `agg()`, etc. Note that we can use `groupBy()` in two ways:

- If we pass one or more column names to `groupBy()`, i.e., `groupBy("col")`, it will group the table in the classes/unique values of the passed column(s); then, we apply an aggregation function on those groups. This is equivalent to SQL.
- If we don't pass a column name to `groupBy()`, each row is a group. This seems not to be useful, but it is in practice, because thanks to it we can apply aggregation functions on the rows that would not be possible otherwise; e.g., `df.min("col")` is not possible, but we need to do `df.groupBy().min("col")`.

Notebook examples:

```python
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
```

##### Aggregation Functions

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

Notebook examples:

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

#### 3.2..6 Joining: JOIN - join()

Joining Spark SQL dataframes is very similar to joining in SQL: we combine tables given the index values on key columns.

```python
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

```

Notebook examples:

```python
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

### 3.3 Introduction to Machine Learning Pipelines

In this section, basic data processing is perform in form of a pipeline and a logistic regression model is trained with grid search.

The notebook: [`03_Machine_Learning.ipynb`](./lab/02_Intro_PySpark/03_Machine_Learning.ipynb).

#### Setup: Create Session + Upload Data

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

#### 3.3.1 Introduction to Machine Learning in Spark

We have two types of classes defined in the module `pyspark.ml`:

- `Transformer` classes: they take a Spark SQL Dataframe and `.transform()` it to yield a new Spark SQL Dataframe.
- `Estimator` classes: they take a Spark SQL Dataframe and `.fit()` a model to it to deliver back an object, which can be a trained `Transformer` ready to `transform()` the data. For instance, a model is an `Estimator` which returns a `Transformer`; then, scoring a model consists in calling `transform()` on the returned `Transformer` using the desired dataset.

#### 3.3.2 Data Processing Pipeline

In this section, basic and typical data processing steps are carried out on the loaded datasets. In spark, feature engineering is done with Pipelines. Shown steps:

- Join tables.
- Cast types: numeric values are required for modeling.
- New Features/Columns
- Remove Missing Values
- Encode Categoricals
- Assemble a Vector and Create a Pipeline
- Fit and Transform the Pipeline
- Train/Test Split

##### Join

```python
# Rename year column
planes = planes.withColumnRenamed("year", "plane_year")

# Join the DataFrames
model_data = flights.join(planes, on="tailnum", how="leftouter")
```

##### Cast Types

```python
model_data.printSchema()

# Cast the columns to integers
model_data = model_data.withColumn("arr_delay", model_data.arr_delay.cast("integer"))
model_data = model_data.withColumn("air_time", model_data.air_time.cast("integer"))
model_data = model_data.withColumn("month", model_data.month.cast("integer"))
model_data = model_data.withColumn("plane_year", model_data.plane_year.cast("integer"))
```

##### New Features/Columns

```python
# Create the column plane_age
model_data = model_data.withColumn("plane_age", model_data.year - model_data.plane_year)

# Create is_late
model_data = model_data.withColumn("is_late", model_data.arr_delay > 0)

# Convert to an integer: booleans need to be converted to integers, too
model_data = model_data.withColumn("label", model_data.is_late.cast("integer"))
```

##### Remove Missing Values

```python
# Remove missing values
model_data = model_data.filter("""arr_delay is not NULL 
                                  and dep_delay is not NULL
                                  and air_time is not NULL
                                  and plane_year is not NULL""")
```

##### Encode Categoricals

We need to instantiate `StringIndexer` to map all unique categorical levels to numbers and a `OneHotEncoder` to create dummy variables from the numbers. All these objects need to be insstantiated and arranged in a vector which is then `fit()` on the dataframe. After that, we can `transform()` the data.

```python
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml import Pipeline

# Create a StringIndexer: Estimator that needs to be fit() and returns a Transformer
carr_indexer = StringIndexer(inputCol="carrier",
                             outputCol="carrier_index")

# Create a OneHotEncoder: Estimator that needs to be fit() and returns a Transformer
carr_encoder = OneHotEncoder(inputCol="carrier_index",
                             outputCol="carrier_fact")

# Create a StringIndexer: Estimator that needs to be fit() and returns a Transformer
dest_indexer = StringIndexer(inputCol="dest",
                             outputCol="dest_index")

# Create a OneHotEncoder: Estimator that needs to be fit() and returns a Transformer
dest_encoder = OneHotEncoder(inputCol="dest_index",
                             outputCol="dest_fact")
```

##### Assemble a Vector and Create a Pipeline

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
```

##### Fit and Transform the Pipeline

```python
# Fit and transform the data:
# - first, the Estimators are fit, which generate trained Transformers
# - then, the dataset is passed through the trained Transformers
piped_data = flights_pipe.fit(model_data).transform(model_data)

piped_data.printSchema()
```

##### Train/Test Split

```python
# Split the data into training and test sets
# train 60%, test 40%
# Always split after the complete dataset has been processed!
training, test = piped_data.randomSplit([.6, .4])
```


#### 3.3.3 Model Tuning and Selection

In this section, a logistic regression model is tuned and trained.

##### Instantiate Logistic Regression Model

```python
# Import LogisticRegression: Estimator
from pyspark.ml.classification import LogisticRegression

# Create a LogisticRegression Estimator
lr = LogisticRegression()
```

##### Instantiate Evaluation Metric

```python
# Import the evaluation submodule
import pyspark.ml.evaluation as evals

# Create a BinaryClassificationEvaluator
evaluator = evals.BinaryClassificationEvaluator(metricName="areaUnderROC")
```

##### Instantiate Parameter Grid

```python
# Import the tuning submodule
import numpy as np
import pyspark.ml.tuning as tune

# Create the parameter grid
grid = tune.ParamGridBuilder()

# Add the hyperparameters to be tried in the grid
grid = grid.addGrid(lr.regParam, np.arange(0, .1, .01))
grid = grid.addGrid(lr.elasticNetParam, [0, 1])

# Build the grid
grid = grid.build()
```

##### Cross Validation Objec

```python
# Create the CrossValidator
cv = tune.CrossValidator(estimator=lr,
                         estimatorParamMaps=grid,
                         evaluator=evaluator)
```

##### Fit the Model with Grid Search

```python
# Fit cross validation models
models = cv.fit(training)

# Extract the best model
best_lr = models.bestModel

# We can also train the model
# without cross validation and grid search
not_best_lr = lr.fit(training)

# Print best_lr
print(best_lr)
```

##### Evaluate the Model

```python
# Use the model to predict the test set
# Note that the model does not have a predict() function
# but it transforms() the data into predictions!
test_results = best_lr.transform(test)

# Evaluate the predictions
print(evaluator.evaluate(test_results)) # 0.6962630071607577
```

## 4. Data Wrangling with Spark

This section introduces the following topics:

- A
- B

The notebooks with the exercises are located in [`lab/03_Data_Wrangling`](./lab/03_Data_Wrangling/).

The data is not uploaded to the repository, but it can be downloaded from the Udacity Spark course link. I have a local non-committed file [`lab/data/mini_sparkify_event_data.json`](./lab/data/mini_sparkify_event_data.json).

Lecture videos:

- [Lesson Overview](https://www.youtube.com/watch?v=XWT2nkoy474)
- [Functional Programming](https://www.youtube.com/watch?v=ZTbFxpcvmSk)
- [Why Functional Programming](https://www.youtube.com/watch?v=jSwfZ8wks_E)
- [Procedural Code](https://www.youtube.com/watch?v=CJtXhcG3MLc)
- [Pure Functions](https://www.youtube.com/watch?v=AHIGpJaAL1U)
- [The Spark DAGs](https://www.youtube.com/watch?v=lrgHpuIJxfM)
- [Maps And Lambda Functions](https://www.youtube.com/watch?v=cOWpvYouMA8)
- ...
- [Data Wrangling](https://www.youtube.com/watch?v=pDOlgj0FBdU)
- ...

### 4.1 Functional Programming

General purpose programming languages are **procedural**: they use for-loops and the like to process data. However, Spark is written in [**Scala**](https://en.wikipedia.org/wiki/Scala_(programming_language)), which is both OOP and **functional**; when using the Python API PySpark, we need to employ the **functional methods** if we want to be fast. Under the hood, the Python code uses [py4j](https://www.py4j.org/) to make calls to the Java Virtual Machine (JVM) where the Scala library is running.

Functional programming uses methods like `map()`, `apply()`, `filter()`, etc. In those, we pass a function to the method, which is the applied to the entire dataset, without the need to using for-loops.

This **functional programming** style is very well suited for distributed systems and it is related to how MapReduce and Hadoop work:

![Functional Programming](./pics/functional_programming.jpg)

![Functional vs. Procedural Programming](./pics/functional_vs_procedural_programming.jpg)

#### Pure Functions and Direct Acyclic Graphs (DAGs)

So that a function passed to a method such as `map()`, `apply()` or `filter()` works properly:

- it should have no side effects on variables outside its scope,
- they should not alter the data which is being processed.

These functions are called **pure functions**.

In Spark, every node makes a copy of the data being processed, so the data is *immutable*. Additionally, the pure functions we apply are usually very simple; we chain them one after the other to define a more complex processing. So a function seems to be composed of multiple subfunctions. All sub-functions need to be pure.

The data is not copied for each of the sub-functions; instead, we perform **lazy evaluation**: all sub-functions are chained in **Direct Acyclic Graphs (DAGs)** and they are not run on the data until it is really necessary. The combinations of sub-functions or chained steps before touching any data are called **stages**.

This is similar to baking bread: we collect all necessary stuff (ingredients, tools, etc.) and prepare them properly before even starting to make the dough.

## 5. Setting up Spark Clusters with AWS


## 6. Debugging and Optimization


## 7. Machine Learning with PySpark