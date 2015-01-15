## Installation
* Please clone this repo to your home directory so that /data is accessible as follows:
  * ~/spark-after-dark/data

## Running
* Please run you spark-shell command from your home directory so that data files will be accessible from within your Spark application code as follows (relative to your current/home directory):
  * sc.textFile("spark-after-dark/data/ratings.csv.gz")
* All code can be copy/pasted into the spark-shell from ~/spark-after-dark/src/scala/...

## Datasets
* http://www.occamslab.com/petricek/data/
  * ratings.csv (FromUserID,ToUserID,Rating)
  * gender.csv (UserID,Gender)

## Examples
* SQL:  Using SQL and Parquet to query descriptive summary statistics on the datasets
* Core:  RDD basics and joins
* GraphX:  Using PageRank to determine top most-desirable users
* MLlib:  Using Alternating Least Squares (ALS) to recommend new users
