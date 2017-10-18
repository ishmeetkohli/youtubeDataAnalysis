![Youtube Logo](https://servicesdirectory.withyoutube.com/static/images/logo.svg)

# Large Scale data analysis on Youtube Dataset using Spark and Hadoop

## ABSTRACT

Data Analysis and Mining are becoming indispensable part of every major organization to find recent trends and statistics and formulate business strategies, planning and marketing. However, most of the Data generated is generally in Huge Size and comes in unstructured format.  Big Data cannot be analyzed by traditional database systems and processes. To resolve this issue, many new tools that implement Parallel Processing are being deployed in these organizations.

As part of the Advanced Databases Project, we propose to perform Data Analysis of YouTube data. We extracted data of 5 Million Video records from YouTube API and performed Data Analysis on the data to insight into latest trends and user engagement in YouTube with respect to Categories and Year. Data Analysis and Visualization was done using Apache Hadoop and Apache Spark.

## INTRODUCTION

Big Data refers to large datasets that could not be analyzed by traditional database systems and processes like RDBMS and existing DataWarehousing systems. Big Data is generally characterized by Huge Volume, High Velocity and High Variety. Companies like Google, YouTube, Facebook, Amazon, Alibaba, Pandora, and Wikipedia are generating and collecting Petabytes of Big data every minute in mutistructured formats likes videos, audios, images, metadata, logs etc. The data generated can be used for Recommendations, formulating business and market strategies using Data Analysis and applying machine learning algorithms. It has been estimated that 2.5 Exabyte of Data is produced every day.

The Volume and variety of Big Data makes the task of Data Analysis using existing traditional Data processing techniques extremely challenging.  To solve this issue, organizations are shifting towards using multiple servers and using parallel processing to save time and memory. There are different technologies like Hadoop, Spark, HBASE that have been developed and are rapidly evolving to deal with Big Data.

As part of Advanced Databases project, we have extracted and analyzed dataset of 5 Million records from YouTube API. The dataset size was 603 MB and consists of key attributes like Video Id, views, likes, Comments and Categories. We performed Data Analysis using Apache Hadoop and Apache Spark open source software framework.

YouTube has 1.3 Billion users and 300 hours of video are being uploaded in YouTube every minute. YouTube gets 30 million users every day and nearly 5 Billion views are watched every day. Hence, YouTube has now become very important marketing tool for major companies and entertainment channels. The primary purpose if this project is to find how real YouTube time data can be analyzed to get latest analysis and trends.  Along with videos with highest view count, most watched categories, we will find how the user base is increasing and how their interests are changing with every year


## DATASET DESCRIPTION

We used two sources, YouTube-8M Video Understanding Challenge and the YouTube API to extract latest YouTube data.

1. Initially, we scraped 8 Million Video Ids from Google Cloud &amp; YouTube-8M Video Understanding Challenge Dataset.
2. Then, we used YouTube API to fetch the following attributes corresponding to these Ids

| Column 1 | Video ID |
| --- | --- |
| Column 2 | View Count |
| Column 3 | Like Count |
| Column 4 | Dislike Count |
| Column 5 | Comment Count |
| Column 6 | Category |
| Column 7 | Published on (Date) |

Out of the 8 Million Video Ids, we were able to fetch 5 Million records since we exhausted the API limit quota.
The total size of data we extracted was around 600 MB and spanned across 18 files and data collection took approximately 5 hours.

## ALGORITHMS USED

We performed analysis on 5 queries using MapReduce algorithm using Apache Hadoop and Apache Spark software framework.  Following is the flow of project to implement MapReduce algorithm.

 
### MapReduce Algorithm

MapReduce Algorithm consists of Map() procedure that performs filtering and sorting of input data and Reduce() performs summary\aggregate function per (key, value) pair.

 
### Hadoop

Hadoop is a distributed computing Framework developed and maintained by The Apache Software Foundation written in Java. Hadoop consists of HDFS and MapReduce and is genrally deployed in a group of machines called cluster. Initially, GFS and MapReduce were built to empower Google Search. HDFS stands for Hadoop Distributed File System and is used to store data across multiple disks.MapReduce is a way to parallelize Data processing tasks.

### Spark

Apache Spark was built on top of Hadoop MapReduce framework. It extends MapReduce model to efficiently use it for more type of computations using interactive queries and stream processing. The main feature of Spark is in-memory cluster computing that increases the processing speed of application. Resilient Distribute Datasets are the fundamental data structures of Spark. Spark&#39;s API is written in Scala, Java and Python. Spark automatically distributes the data contained in RDDs across cluster and parallelizes the data.

## Queries

Following are the list of queries we tried to answer using both Hadoop MapReduce and Spark:

1. Top 20 videos with largest number of views.
2. Top 20 videos with largest number of likes.
3. Top 20 videos with largest number of dislikes.
4. Top 10 viewed categories.
5. Most watched categories per year
