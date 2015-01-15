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
  * FromUserID is user who provided rating
  * ToUserID is user who has been rated
  * FromUserIDs range between 1 and 135,359
  * ToUserIDs range between 1 and 220,970 (not every profile has been rated)
  * Ratings are on a 1-10 scale where 10 is best (integer ratings only)
  * Only users who provided at least 20 ratings were included
  * Users who provided constant ratings were excluded
* gender.csv (UserID,Gender)
  * Gender is denoted by a "M" for male and "F" for female and "U" for unknown

## Examples
* SQL:  Using SQL and Parquet to query descriptive summary statistics on the datasets
* Core:  RDD basics and joins
* GraphX:  Using PageRank to determine top most-desirable users
* MLlib:  Using Alternating Least Squares (ALS) to recommend new users
