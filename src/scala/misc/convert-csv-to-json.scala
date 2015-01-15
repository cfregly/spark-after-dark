import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.SparkContext.doubleRDDToDoubleRDDFunctions
import org.apache.spark.graphx._
import org.apache.spark.rdd.PairRDDFunctions._

// Read csv (transformation)
val ratingsCSV = sc.textFile("data/ratings.dat")

// Convert csv to JSON
val ratingsJSON = ratingsCSV.map(ratingsLine => {
	val tokens = ratingsLine.split(",")
	s"{/"from/":s(tokens(0),/"to/":s(tokens(1),/"rating/":s(tokens(2)}"}

// Read the Parquet (transformation)
val ratingsParquetSchemaRDD = sqlContext.parquetFile(s"/tmp/ratings-parquet-$timestamp")

// Register Parquet file as a temp table (temp because it only applies to this context)
ratingsParquetSchemaRDD.registerTempTable("ratingsParquetTable")

// Query and aggregate (avg) the ratings using SQL
val ratingsStats = sqlContext.sql("SELECT avg(rating), min(rating), max(rating) FROM ratingsParquetTable")
val ratingsStatsRowArray = ratingsStats.take(1)
println(ratingsStatsRowArray(0))
