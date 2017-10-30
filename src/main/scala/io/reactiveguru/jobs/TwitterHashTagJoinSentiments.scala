package io.reactiveguru.jobs

import com.typesafe.config.ConfigFactory
import io.reactiveguru.jobs.Main.getClass
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils

object TwitterHashTagJoinSentiments {

  def main(args: Array[String]): Unit = {
    if (!Logger.getRootLogger.getAllAppenders.hasMoreElements) {
      Logger.getRootLogger.setLevel(Level.WARN)
    }

    val conf = ConfigFactory.load("twitter")
    val consumerKey = conf.getString("twitter.consumerKey")
    val consumerSecret = conf.getString("twitter.consumerSecret")
    val accessToken = conf.getString("twitter.accessToken")
    val accessTokenSecret = conf.getString("twitter.accessTokenSecret")

    val filters = Array("Netflix","Amazon","Pixel2", "Google", "iPhoneX")

    val wordSentimentFilePath = getClass.getResource("/data/AFINN.txt").getPath

    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val sparkConf = new SparkConf().setAppName("TwitterHashTagJoinSentiments")
    if (!sparkConf.contains("spark.master")) {
      sparkConf.setMaster("local[2]")
    }

    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val stream = TwitterUtils.createStream(ssc, None, filters)

    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    // Read in the word-sentiment list and create a static RDD from it
    val wordSentiments = ssc.sparkContext.textFile(wordSentimentFilePath).map { line =>
      val Array(word, happinessValue) = line.split("\t")
      (word, happinessValue.toInt)
    }.cache()

    // Determine the hash tags with the highest sentiment values by joining the streaming RDD
    // with the static RDD inside the transform() method and then multiplying
    // the frequency of the hash tag by its sentiment value
    val happiest60 = hashTags.map(hashTag => (hashTag.tail, 1))
      .reduceByKeyAndWindow(_ + _, Seconds(60))
      .transform{topicCount => wordSentiments.join(topicCount)}
      .map{case (topic, tuple) => (topic, tuple._1 * tuple._2)}
      .map{case (topic, happinessValue) => (happinessValue, topic)}
      .transform(_.sortByKey(false))

    val happiest10 = hashTags.map(hashTag => (hashTag.tail, 1))
      .reduceByKeyAndWindow(_ + _, Seconds(10))
      .transform{topicCount => wordSentiments.join(topicCount)}
      .map{case (topic, tuple) => (topic, tuple._1 * tuple._2)}
      .map{case (topic, happinessValue) => (happinessValue, topic)}
      .transform(_.sortByKey(false))

    // Print hash tags with the most positive sentiment values
    happiest60.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nHappiest topics in last 60 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (happiness, tag) => println("%s (%s happiness)".format(tag, happiness))}
    })

    happiest10.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nHappiest topics in last 10 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (happiness, tag) => println("%s (%s happiness)".format(tag, happiness))}
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
