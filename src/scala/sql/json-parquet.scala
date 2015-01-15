import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.SparkContext.doubleRDDToDoubleRDDFunctions
import org.apache.spark.graphx._
import org.apache.spark.rdd.PairRDDFunctions._

import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext.createSchemaRDD

// Create SQLContext from SparkContext
val sqlContext = new SQLContext(sc)

// Read JSON (transformation)
val ratingsJSON = sqlContext.jsonFile("data/ratings-sm.json")

// Print the inferred JSON schema
ratingsJSON.printSchema()

// Save as Parquet (action)
// Note:  The mappers that do the JSON read above are pipelined with the mappers that write the parquet file
val timestamp = System.currentTimeMillis()
ratingsJSON.saveAsParquetFile(s"/tmp/ratings-parquet-$timestamp")

// Read the Parquet (transformation)
val ratingsParquetSchemaRDD = sqlContext.parquetFile(s"/tmp/ratings-parquet-$timestamp")

// Register Parquet file as a temp table (temp because it only applies to this context)
ratingsParquetSchemaRDD.registerTempTable("ratingsParquetTable")

// Query and aggregate (avg) the ratings using SQL
val ratingsStats = sqlContext.sql("SELECT avg(rating), min(rating), max(rating) FROM ratingsParquetTable")
val ratingsStatsRowArray = ratingsStats.take(1)
println(ratingsStatsRowArray(0))
