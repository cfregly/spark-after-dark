import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.SparkContext.doubleRDDToDoubleRDDFunctions
import org.apache.spark.graphx._
import org.apache.spark.rdd.PairRDDFunctions._

import org.apache.spark.sql._

/////////////////////////////////////////
// Define domain classes
/////////////////////////////////////////
case class UserRating(from: Int, to: Int, rating: Int)

/////////////////////////////////////////
// Load and cleanse the ratings data
// Data Format:
// 	 from,to,rating
/////////////////////////////////////////

// Create RDD from input text file
// (transformation, lazy)
val ratingLinesRDD = sc.textFile("data/ratings-sm-only-0-and-1.dat")

// Split the lines into tokens by "," 
// Create UserRating from the line token splits (arrays are 0-based)
// (transformation, lazy)
val ratings = ratingLinesRDD.map{ ratingLine => 
 	val tokens = ratingLine.split(",")
 	UserRating(tokens(0).toInt, tokens(1).toInt, tokens(2).toInt)
}

// Filter to include only ratings of value 1 (accept, swipe-right rating)
// (transformation, lazy)
val acceptRatings = ratings.filter{case UserRating(from, to, rating) => rating == 1}

// Get current timestamp
val timestamp = System.currentTimeMillis()

val acceptRatingsEdgeList = acceptRatings.map{case UserRating(from, to, rating) => s"$from $to"}
acceptRatingsEdgeList.saveAsTextFile(s"/tmp/accept-ratings-pagerank-$timestamp")

// Build the graph of ratings using the part-* files generated above
// Need to explicitly set the number of partitions or else 1 will be used and this will likely OOM
val graph = GraphLoader.edgeListFile(sc, s"/tmp/accept-ratings-pagerank-$timestamp/*", false, 10).cache()

// Run PageRank with a convergence threshold of 0.01
// Result is a tuple of (userId,pageRank)
val pageRanks = graph.pageRank(0.01)

// Print the top 10 (userId,pageRank) tuples ordered by pageRank
println(pageRanks.vertices.top(10)(Ordering.by(pageRank => pageRank._2)).mkString("\n"))






// Expected Output:
// 	(90006,0.31452387499999995)
// 	(10004,0.292375)
// 	(90001,0.250773875)
// 	(10005,0.2245875)
// 	(10002,0.2177875)
// 	(10003,0.2177875)
// 	(90003,0.21375)
// 	(90005,0.187023875)
// 	(90004,0.187023875)
// 	(90002,0.187023875)
