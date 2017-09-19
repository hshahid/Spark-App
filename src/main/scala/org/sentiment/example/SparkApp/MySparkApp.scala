import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.flume._

/**
 * A Spark Streaming application that receives tweets on certain 
 * keywords from twitter datasource and find the popular hashtags
 * 
 * Arguments: <comsumerKey> <consumerSecret> <accessToken> <accessTokenSecret> <keyword_1> ... <keyword_n>
 * <comsumerKey>        - Twitter consumer key 
 * <consumerSecret>     - Twitter consumer secret
 * <accessToken>        - Twitter access token
 * <accessTokenSecret>  - Twitter access token secret
 * <keyword_1>          - The keyword to filter tweets
 * <keyword_n>          - Any number of keywords to filter tweets
 * 
 * More discussion at stdatalabs.blogspot.com
 * 
 * @author Sachin Thirumala
 */

object MySparkApp {
  val conf = new SparkConf().setMaster("local[4]").setAppName("Spark Streaming - PopularHashTags")
  val sc = new SparkContext(conf)

  def main(args: Array[String]) {

    sc.setLogLevel("WARN")

    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val filters = args.takeRight(args.length - 4)

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generat OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", 	"nzqklPC0Dt6Kd9ma59jGsnqL8")
    System.setProperty("twitter4j.oauth.consumerSecret", "eXU6SbaJpx7LXsJxzl7OZEoDxfd2EvNwtQ9XUpL71eIAnJx4PU")
    System.setProperty("twitter4j.oauth.accessToken", "	4807270241-z32Nst6CkC5875C8Osa4YZ1tgL3sp4mWIUNJEyf")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "hiA2zDxAytMvPFHanedocVmWrN7WfRNxUbvpA3tTe9pVV")

    // Set the Spark StreamingContext to create a DStream for every 5 seconds
    val ssc = new StreamingContext(sc, Seconds(5))
    // Pass the filter keywords as arguements

    //  val stream = FlumeUtils.createStream(ssc, args(0), args(1).toInt)  
    val stream = TwitterUtils.createStream(ssc, None, filters)
    
    // Split the stream on space and extract hashtags 
    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    // Get the top hashtags over the previous 60 sec window
    val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
      .map { case (topic, count) => (count, topic) }
      .transform(_.sortByKey(false))

    // Get the top hashtags over the previous 10 sec window
    val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
      .map { case (topic, count) => (count, topic) }
      .transform(_.sortByKey(false))

    // print tweets in the currect DStream 
    stream.print()

    // Print popular hashtags  
    topCounts60.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
      topList.foreach { case (count, tag) => println("%s (%s tweets)".format(tag, count)) }
    })
    topCounts10.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
      topList.foreach { case (count, tag) => println("%s (%s tweets)".format(tag, count)) }
    })

    ssc.start()
    ssc.awaitTermination()
  }
} 