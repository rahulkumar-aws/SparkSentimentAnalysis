package io.reactiveguru.jobs

import com.typesafe.config.ConfigFactory
import com.twitter.algebird.HyperLogLog._
import com.twitter.algebird.HyperLogLogMonoid
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils

object TwitterAlgebirdHLL {
  def main(args: Array[String]): Unit = {
    if (!Logger.getRootLogger.getAllAppenders.hasMoreElements) {
      Logger.getRootLogger.setLevel(Level.WARN)
    }

    /** Bit size parameter for HyperLogLog, trades off accuracy vs size */
    val BIT_SIZE = 12

    val conf = ConfigFactory.load("twitter")
    val consumerKey = conf.getString("twitter.consumerKey")
    val consumerSecret = conf.getString("twitter.consumerSecret")
    val accessToken = conf.getString("twitter.accessToken")
    val accessTokenSecret = conf.getString("twitter.accessTokenSecret")

    val filters = Array("Netflix","Amazon","Pixel2", "Google", "iPhoneX")

    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val sparkConf = new SparkConf().setAppName("TwitterHashTagJoinSentiments")
    if (!sparkConf.contains("spark.master")) {
      sparkConf.setMaster("local[2]")
    }

    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val sc = ssc.sparkContext
    sc.setLogLevel("ERROR")
    val stream = TwitterUtils.createStream(ssc, None, filters, StorageLevel.MEMORY_ONLY_SER)


    val users = stream.map(status => status.getUser.getId)

    val hll = new HyperLogLogMonoid(BIT_SIZE)
    var globalHll = hll.zero
    var userSet: Set[Long] = Set()

    val approxUsers = users.mapPartitions(ids => {
      ids.map(id => hll.create(id))
    }).reduce(_ + _)

    val exactUsers = users.map(id => Set(id)).reduce(_ ++ _)

    approxUsers.foreachRDD(rdd => {
      if (rdd.count() != 0) {
        val partial = rdd.first()
        globalHll += partial
        println("Approx distinct users this batch: %d".format(partial.estimatedSize.toInt))
        println("Approx distinct users overall: %d".format(globalHll.estimatedSize.toInt))
      }
    })

    exactUsers.foreachRDD(rdd => {
      if (rdd.count() != 0) {
        val partial = rdd.first()
        userSet ++= partial
        println("Exact distinct users this batch: %d".format(partial.size))
        println("Exact distinct users overall: %d".format(userSet.size))
        println("Error rate: %2.5f%%".format(((globalHll.estimatedSize / userSet.size.toDouble) - 1
          ) * 100))
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
