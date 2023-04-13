# Introduction to Hadoop and MapReduce

I made these notes while following the Udacity course [Intro to Hadoop and MapReduce](https://www.udacity.com/course/intro-to-hadoop-and-mapreduce--ud617).

Lecture videos, complete list: [Introduction to Hadoop and MapReduce](https://www.youtube.com/watch?v=DEQNknALf_8&list=PLAwxTw4SYaPkXJ6LAV96gH8yxIfGaN3H-).

Mikel Sagardia, 2023.  
No guarantees.

Table of contents:

- [Introduction to Hadoop and MapReduce](#introduction-to-hadoop-and-mapreduce)
  - [Big Data](#big-data)
    - [Hadoop](#hadoop)
  - [HDFS and MapReduce](#hdfs-and-mapreduce)


## Big Data

Selected videos:

- [Big Data Intro](https://www.youtube.com/watch?v=Fyxi0qlyTaQ)
- [Three Vs](https://www.youtube.com/watch?v=KJKKFV97ntg&t=9s)
- [Data Formats](https://www.youtube.com/watch?v=EVzqlf369tI)
- [Velocity](https://www.youtube.com/watch?v=nPtRFCRMfhw)
- [Doug Cutting: The Origins of Hadoop](https://www.youtube.com/watch?v=ebgXN7VaIZA)
- [Hadoop Core](https://www.youtube.com/watch?v=alie2Kn-jRY)
- [Hadoop Ecosystem](https://www.youtube.com/watch?v=4sZ7n-Wg9Dc)

Summary points:

- Big data refers to the production of large amounts of data, which is (1) **difficult to store** and (2) **process** with *single servers*.
- Challenges of big data, the 3 Vs:
  - Volume: we have many data and most of seems to be worthless.
  - Velocity: Data is created fast, sometimes streamed; we can get data as fast as TB/day.
  - Variety: We have data from different sources and in various formats; unlike traditional data, it is unstructured and it doesn't fit that easily to relational/SQL databases. The nice thing about Hadoop is that  we can store the data in raw format for later processing. For instance: we can store in MP3 the phone calls to later extract a *mood* indicator from the conversation.
- Typical data candidate for big data:
  - Transactions
  - Logs
  - Business actions
  - User actions
  - Sensor data
  - Medical data
  - Social network data

### Hadoop

- Hadoop originated as an Open Source implementation of Google's work on distributed systems, focusing on [MapReduce](http://static.googleusercontent.com/media/research.google.com/en/us/archive/mapreduce-osdi04.pdf) and [GFS - Google File System](http://static.googleusercontent.com/media/research.google.com/en/us/archive/gfs-sosp2003.pdf). Hadoop was the first open source implementation of an OS for distributed systems.
- Hadoop consists of two main components
    1.  Hadoop File System: HDFS - to **store** data.
        - Data is split and store in different machines.
        - The machines forma cluster.
        - We can in extend the cluster adding more machines as we require.
        - The machines don't need to be high-end!
    2.  Hadoop MapReduce - to **process** data.
        - The data is processed in each machine separately, locally.
        - Then, the results are aggregated.
- Hadoop Ecosystem: many layers and modules have appeared to make it easier to use Hadoop, instead of forcing the user to write MapReduce code in Java
  - Hive (SQL) and Pig (scripting): they enable the users to interface the data in the cluster using SQL or simple scripting. Their code is translated into MapReduce.
  - Impala: It is similar to Hive, because it enables SQL queries to the data, but it doesn't use MapReduce as an intermediate step, it directly accesses the HDFS. In general:
    - Hive is used for large batch jobs.
    - Imapala is used to fast online queries.
  - Sqoop: transfers SQL database data into the cluster HDFS.
  - Flume: ingests data from external systems and transfers it into the cluster HDFS.
  - ...
- Cloudera is the company which created Hadoop and they have a CDH, Cloudera Distribution Hadoop.

## HDFS and MapReduce

Selected videos:

