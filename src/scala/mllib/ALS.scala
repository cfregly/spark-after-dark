import org.apache.spark.SparkContext._
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating

//Read in the ratings file (fromUserId, toUserId, rating).  These ratings are 0-9.
val rawRatingsRDD = sc.textFile("spark-after-dark/data/ratings.csv.gz")

//Create mllib.recommendation.Rating RDD from raw ratings input data
val ratingsRDD = rawRatingsRDD.map{ rating => 
 	val tokens = rating.split(",")
 	Rating(tokens(0).toInt, tokens(1).toInt, tokens(2).toInt)
}

//Separate ratings data into training data (80%) and test data (20%)
val splitRatingsRDD = ratingsRDD.randomSplit(Array(0.80,0.20))	
val trainingRatingsRDD = splitRatingsRDD(0)
val knownTestRatingsRDD = splitRatingsRDD(1)

//Train the ALS model using the training data and various model hyperparameters
val model = ALS.train(trainingRatingsRDD, 1, 5, 0.01, 100)

//Compare predictions against the known test data
val knownTestFromToRDD = knownTestRatingsRDD.map { 
  case Rating(fromUserId, toUserId, rating) => (fromUserId, toUserId)
}

//Test the model by predicting the ratings for the known test data
val actualPredictionsRDD = model.predict(knownTestFromToRDD)

actualPredictionsRDD.take(10).mkString("\n")

//Show lineage of complex RDD
actualPredictionsRDD.toDebugString

//Prepare the known test predictions and actual predictions for comparison
val actualPredictionsKeyedByFromToRDD = actualPredictionsRDD.map{ 
  case Rating(from, to, rating) => ((from, to), rating)
}

val testPredictionsKeyedByFromToRDD = knownTestRatingsRDD.map{ 
  case Rating(from, to, rating) => ((from, to), rating) 
}

//Join the known test predictions with the actual predictions
val testRatingsAndActualPredictionsJoinedRDD = testPredictionsKeyedByFromToRDD.join(actualPredictionsKeyedByFromToRDD)

testRatingsAndActualPredictionsJoinedRDD.take(10).mkString("\n")

//Evaluate the model using Mean Absolute Error (MAE) between the known test ratings and the actual predictions 
val meanAbsoluteError = testRatingsAndActualPredictionsJoinedRDD.map { 
  case ((from, to), (knownTestRating, actualRating)) => 
    val err = (knownTestRating - actualRating)
    Math.abs(err)
}.mean()
