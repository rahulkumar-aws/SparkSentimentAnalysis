package io.reactiveguru.jobs

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.FilterQuery

object twitterLocations {

  def main(args: Array[String]): Unit = {
    // Get bounding boxes of locations for which to retrieve Tweets from command line
    val locationArgs = Array.empty[String]
    val boundingBoxes = if (locationArgs.length == 0) {
      System.out.println("No location bounding boxes specified, using defaults for New York City")
      val nycSouthWest = Array(-74.0, 40.0)
      val nycNorthEast = Array(-73.0, 41.0)
      Array(nycSouthWest, nycNorthEast)
    } else {
      locationArgs.map(_.toDouble).sliding(2, 2).toArray
    }

    val conf = ConfigFactory.load("twitter")
    val consumerKey = conf.getString("twitter.consumerKey")
    val consumerSecret = conf.getString("twitter.consumerSecret")
    val accessToken = conf.getString("twitter.accessToken")
    val accessTokenSecret = conf.getString("twitter.accessTokenSecret")

    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val sparkConf = new SparkConf().setAppName("TwitterLocations")

    // check Spark configuration for master URL, set it to local if not configured
    if (!sparkConf.contains("spark.master")) {
      sparkConf.setMaster("local[2]")
    }

    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val sc = ssc.sparkContext
    sc.setLogLevel("ERROR")
    val locationsQuery = new FilterQuery().locations(boundingBoxes : _*)

    // Print Tweets from the specified coordinates
    // This includes Tweets geo-tagged in the bounding box defined by the coordinates
    // As well as Tweets tagged in places inside of the bounding box
    TwitterUtils.createFilteredStream(ssc, None, Some(locationsQuery))
      .map(tweet => {
        val latitude = Option(tweet.getGeoLocation).map(l => s"${l.getLatitude},${l.getLongitude}")
        val place = Option(tweet.getPlace).map(_.getName)
        val location = latitude.getOrElse(place.getOrElse("(no location)"))
        val text = tweet.getText.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
        s"$location\t$text"
      })
      .print()

    ssc.start()
    ssc.awaitTermination()
  }
}
