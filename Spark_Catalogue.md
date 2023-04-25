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
  - [Feature Engineering](#feature-engineering)
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

## Data Manipulation

## Feature Engineering

## Machine Learning



