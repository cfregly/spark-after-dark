// Databricks notebook source exported at Thu, 15 Jan 2015 03:43:20 UTC
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.sql._


// COMMAND ----------

// MAGIC %md ##Read in ratings input data (0 == reject, 1 == accept)

// COMMAND ----------

val rawRatingsRDD = sc.textFile("/mnt/databricks-cfregly/spark-after-dark/data/ratings-sm-only-0-and-1.csv")


// COMMAND ----------

// MAGIC %md ##Create UserRatings RDD from input ratings data

// COMMAND ----------

case class UserRating(fromUserId: Long, toUserId: Long, rating: Int)

// COMMAND ----------

val userRatingsRDD = rawRatingsRDD.map{ rawUserRating => 
 	val tokens = rawUserRating.split(",")
 	UserRating(tokens(0).toLong, tokens(1).toLong, tokens(2).toInt)
}

// COMMAND ----------

userRatingsRDD.take(10)

// COMMAND ----------

// MAGIC %md ##Keep only the positive ratings as this is what we'll feed to PageRank 

// COMMAND ----------

val positiveRatingsRDD = userRatingsRDD.filter(userRating => userRating.rating == 1)

// COMMAND ----------

// MAGIC %md ##Save the data into a format that PageRank needs (fromUserId, toUserId) 

// COMMAND ----------

val positiveRatingsEdgeListRDD = positiveRatingsRDD.map{userRating => s"${userRating.fromUserId} ${userRating.toUserId}"}

// COMMAND ----------

// MAGIC %md ##Save the Graph data out to a file

// COMMAND ----------

val timestamp = System.currentTimeMillis()
positiveRatingsEdgeListRDD.saveAsTextFile(s"/mnt/databricks-cfregly/spark-after-dark/tmp/positive-ratings-pagerank-$timestamp")


// COMMAND ----------

// MAGIC %md ##Load the Graph data into an RDD 

// COMMAND ----------

val graph = GraphLoader.edgeListFile(sc, s"/mnt/databricks-cfregly/spark-after-dark/tmp/positive-ratings-pagerank-$timestamp/*", false, 10).cache()

// COMMAND ----------

// MAGIC %md ##Run PageRank

// COMMAND ----------

val pageRankGraph = graph.pageRank(0.01)

// COMMAND ----------

// MAGIC %md ##Show the top 10 most-desirable userIds

// COMMAND ----------

println(pageRankGraph.vertices.top(10)(Ordering.by(pageRank => pageRank._2)).mkString("\n"))

// COMMAND ----------

// MAGIC %md ## Join the PageRank results with the Users dataset to show human-readable names

// COMMAND ----------

val rawUsersRDD = sc.textFile("/mnt/databricks-cfregly/spark-after-dark/data/users-sm.csv")

// COMMAND ----------

val usersRDD = rawUsersRDD.map{ rawUser => 
	val tokens = rawUser.split(",")
	(tokens(0).toLong, tokens(1))
}

// COMMAND ----------

val pageRankVerticesRDD = pageRankGraph.vertices.map(pageRank => (pageRank._1.toLong, pageRank._2))

// COMMAND ----------

val pageRanksWithUsersRDD = pageRankVerticesRDD.join(usersRDD)

// COMMAND ----------

// MAGIC %md ##Show the top 10 most-desirable user names!

// COMMAND ----------

println(pageRanksWithUsersRDD.top(10)(Ordering.by(pageRankWithUser => pageRankWithUser._2._1)).mkString("\n"))
