import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.SparkContext.doubleRDDToDoubleRDDFunctions
import org.apache.spark.graphx._
import org.apache.spark.rdd.PairRDDFunctions._

/////////////////////////////////////////
// Define domain classes
/////////////////////////////////////////
// case class User(id: Int)
// case class Rating(rating: Int)
// case class Gender(gender: String)
// case class UserRatingWithGender(from: Int, to: Int, rating: Int, gender: String)

/////////////////////////////////////////
// Load and cleanse the ratings data
// Data Format:
// 	 from,to,rating
/////////////////////////////////////////

// Create RDD from input text file
// (transformation, lazy)
val ratingLinesRDD = sc.textFile("data/users-sm.dat")

// Split the lines into tokens by "," 
// Create UserRating from the line token splits (arrays are 0-based)
// Create RDD from input text file (transformation, lazy)
// (transformation, lazy)
// val ratings = ratingLinesRDD.map{ ratingLine => 
//  	val tokens = ratingLine.split(",")
//  	(tokens(1).toInt, tokens(2).toInt)
// }

val ratings = ratingLinesRDD.keyBy(line => line.toInt)

/////////////////////////////////////////
// Load and cleanse the gender data
// Data Format:
// 	 userId,gender
/////////////////////////////////////////

// Create RDD from input text file
// (transformation, lazy)
val genderLinesRDD = sc.textFile("data/gender-sm.dat")

// Split the lines into tokens by "," 
// Create UserGender from the line tokens splits (arrays are 0-based)
// (transformation, lazy)
val genders = genderLinesRDD.map{ genderLine => 
	val tokens = genderLine.split(",")
	(tokens(0).toInt, tokens(1))
}

val ratingsWithGender = ratings.join(genders)
ratingsWithGender.take(10)
