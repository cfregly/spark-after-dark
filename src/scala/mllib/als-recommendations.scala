import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.SparkContext.doubleRDDToDoubleRDDFunctions
import org.apache.spark.graphx._
import org.apache.spark.rdd.PairRDDFunctions._
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating

/////////////////////////////////////////
// Load and cleanse the ratings data
// Data Format:
// 	 from,to,rating
/////////////////////////////////////////

// Create RDD from input text file
// (transformation, lazy)
val ratingLinesRDD = sc.textFile("data/ratings-sm-only-01.dat")

// Split the lines into tokens by "," 
// Create ALS Rating from the line token splits (arrays are 0-based)
// (transformation, lazy)
val ratings = ratingLinesRDD.map{ ratingLine => 
 	val tokens = ratingLine.split(",")
 	Rating(tokens(0).toInt, tokens(1).toInt, tokens(2).toInt)
}.cache()

// TODO:  Sample the original dataset to train on 80% and test/verify on 20%  

// Train the ALS model
val model = ALS.train(ratings, 1, 5, 0.01, 100)

// Evaluate the model on rating data
val userUserRatings = ratings.map { case Rating(from, to, rating) =>
  (from, to)
}

val predictions = model.predict(userUserRatings)
predictions.top(10)(Ordering.by(rating => rating.rating))

// Expected Output
//  Array(Rating(90001,10004,1.1447724735952383),
 // Rating(10002,90006,1.1439689651765355), 
 // Rating(10002,90001,1.0022159515095745), 
 // Rating(90001,10002,0.9933700903369725), 
 // Rating(10002,90004,0.9843697120270881), 
 // Rating(10001,90001,0.972300646783167), 
 // Rating(90004,10005,0.9693423009302452), 
 // Rating(90004,10004,0.7460939865281379), 
 // Rating(10006,90006,0.7179777846053692), 
 // Rating(10002,90005,0.7014506596640229))