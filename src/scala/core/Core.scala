import org.apache.spark.SparkContext._
import org.apache.spark.graphx._

//Load user data (userId, name) into a User case class
val rawUsersRDD = sc.textFile("spark-after-dark/data/users-sm.csv")

case class User(id: Int, name: String)

//Convert the raw user lines into typed User objects
val usersRDD = rawUsersRDD.map { user => 
 	val tokens = user.split(",")
    User(tokens(0).toInt, tokens(1).toString)
}

usersRDD.take(10).mkString("\n")

//Show RDD lineage
usersRDD.toDebugString

//Load the gender data (userId,gender) into a UserGender case class
val rawGendersRDD = sc.textFile("spark-after-dark/data/gender-sm.csv")

//Convert the raw user gender lines into typed UserGender objects
case class UserGender(userId: Int, gender: String)

val gendersRDD = rawGendersRDD.map{ gender => 
	val tokens = gender.split(",")
	UserGender(tokens(0).toInt, tokens(1))
}

//Specify userId as the key for both RDD's so we can join() to 2 datasets
val usersByUserIdRDD = usersRDD.keyBy(user => user.id)
val gendersByUserIdRDD = gendersRDD.keyBy(gender => gender.userId)

//Join the 2 datasets by userId
val usersWithGenderJoinedRDD = usersByUserIdRDD.join(gendersByUserIdRDD)
usersWithGenderJoinedRDD.take(10).mkString("\n")

//Cache the results so we don't have to rejoin again...
usersWithGenderJoinedRDD.cache()