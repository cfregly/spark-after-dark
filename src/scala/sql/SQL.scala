import org.apache.spark.SparkContext._
import org.apache.spark.sql._

val sqlContext = new SQLContext(sc)

//Read in raw ratings data (fromUserId, toUserId, rating)
val ratingsCsvRDD = sc.textFile("spark-after-dark/data/ratings.csv.gz")

//Convert raw ratings RDD to Json RDD
val ratingsJsonRDD = ratingsCsvRDD.map(rating => {
	val tokens = rating.split(",")
    s"""{"fromUserId":${tokens(0)},"toUserId":${tokens(1)},"rating":${tokens(2)}}"""
})

//Create SchemaRDD from JsonRDD
val ratingsJsonSchemaRDD = sqlContext.jsonRDD(ratingsJsonRDD)

//Cache the SchemaRDD as we'll be using this heavily moving forward
ratingsJsonSchemaRDD.cache()

//Describe the SchemaRDD inferred from the JSON
ratingsJsonSchemaRDD.printSchema
 
ratingsJsonSchemaRDD.registerTempTable("ratingsJsonTable")

sqlContext.sql("DESCRIBE ratingsJsonTable")

//Show the top 10 most-active users
val mostActiveUsersSchemaRDD = sqlContext.sql("SELECT fromUserId, count(*) as ct from ratingsJsonTable group by fromUserId order by ct desc limit 10")

println(mostActiveUsersSchemaRDD.collect().mkString("\n"))

//Show the top 10 most-rated users
val mostRatedUsersSchemaRDD = sqlContext.sql("SELECT toUserId, count(*) as ct from ratingsJsonTable group by toUserId order by ct desc limit 10")

println(mostRatedUsersSchemaRDD.collect().mkString("\n"))

//Perform the same aggregations (count, average, minimum, maximum) using Parquet

val timestamp = System.currentTimeMillis()
ratingsJsonSchemaRDD.saveAsParquetFile(s"spark-after-dark/tmp/ratings-parquet-$timestamp")

val ratingsParquetSchemaRDD = sqlContext.parquetFile(s"spark-after-dark/tmp/ratings-parquet-$timestamp")

ratingsParquetSchemaRDD.registerTempTable("ratingsParquetTable")

val ratingsParquetStatsRDD = sqlContext.sql("SELECT avg(rating) as average, min(rating) as minimum, max(rating) as maximum FROM ratingsParquetTable")

println(ratingsParquetStatsRDD.collect().mkString("\n"))
