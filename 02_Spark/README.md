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
  - [4. Data Wrangling with Spark](#4-data-wrangling-with-spark)
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


## 4. Data Wrangling with Spark


## 5. Setting up Spark Clusters with AWS


## 6. Debugging and Optimization


## 7. Machine Learning with PySpark