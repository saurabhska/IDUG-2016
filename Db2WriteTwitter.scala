import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import java.util.Properties
import java.sql.DriverManager
import java.sql.PreparedStatement

object Db2WriteTwitter extends App{
   Class.forName("com.ibm.db2.jcc.DB2Driver")
   val con = DriverManager.getConnection("jdbc:db2://localhost:50000/sample:currentSchema=SSKA;user=SSKA;password=saurabhska;")
   val SQL="insert into SSKA.TWITTERUSERS values (?)"
   val ps = con.prepareStatement(SQL)
       
   val consumerKey = "C3WjW9fKRNRTt8RvibJkAgqv7";
   val consumerSecret = "vI6HunFqBbmL6otD2AmuhBQN50vDo1PbyaH9OQk1EtKC2hyVFM";
   val accessToken = "146839306-oF5YA7SZPIBeQ18Oz5Ufpy5awRisRMZ59xxtG4oK";
   val accessTokenSecret = "uJ3wEupypfQ8r1dnwzhTGqI2h5B7dSTdb4KYv3S6Jhzkz";
   System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
   System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
   System.setProperty("twitter4j.oauth.accessToken", accessToken)
   System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

   val sparkConf = new SparkConf().setAppName("TwitterPopularTags").setMaster("local[16]")
   val ssc = new StreamingContext(sparkConf, Seconds(2))
 
   //val filters = Array("#IDUG","#IDUGNA","#IDUGNA2015","#IDUG2015")
   val filters = Array("7 DAYS FOR FAN")
   val stream = TwitterUtils.createStream(ssc, None, filters) 
   val users = stream.map(status => status.getUser.getName)
   val recentUsers = users.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
  
   recentUsers.foreachRDD(rdd => {
      println("\nNumber of users in last 60 seconds (%s total):".format(rdd.count()))
      rdd.foreach{
        case (user, tag) => println("%s ".format(user))
        val singleUser = format(user)
        ps.setString(1, singleUser)
        ps.execute()
        println("Inserted Twitter User into DB: " + singleUser)
        }
    })
    ssc.start()
    ssc.awaitTermination()
}