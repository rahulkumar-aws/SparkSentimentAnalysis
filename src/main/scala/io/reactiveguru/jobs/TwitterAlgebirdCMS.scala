package io.reactiveguru.jobs

import com.twitter.algebird.{MapMonoid, TopPctCMS}
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TwitterAlgebirdCMS {

  def main(args: Array[String]): Unit = {

    // Set logging level if log4j not configured (override by adding log4j.properties to classpath)
    if (!Logger.getRootLogger.getAllAppenders.hasMoreElements) {
      Logger.getRootLogger.setLevel(Level.WARN)
    }

    // CMS parameters
    val DELTA = 1E-3
    val EPS = 0.01
    val SEED = 1
    val PERC = 0.001
    // K highest frequency elements to take
    val TOPK = 10

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
    val sparkConf = new SparkConf().setAppName("TwitterAlgebirdCMS")

    // check Spark configuration for master URL, set it to local if not configured
    if (!sparkConf.contains("spark.master")) {
      sparkConf.setMaster("local[2]")
    }

    val ssc = new StreamingContext(sparkConf, Seconds(10))
    val stream = TwitterUtils.createStream(ssc, None, filters, StorageLevel.MEMORY_ONLY_SER_2)

    val users = stream.map(status => status.getUser.getId)

    // val cms = new CountMinSketchMonoid(EPS, DELTA, SEED, PERC)
    val cms = TopPctCMS.monoid[Long](EPS, DELTA, SEED, PERC)
    var globalCMS = cms.zero
    val mm = new MapMonoid[Long, Int]()
    var globalExact = Map[Long, Int]()

    val approxTopUsers = users.mapPartitions(ids => {
      ids.map(id => cms.create(id))
    }).reduce(_ ++ _)

    val exactTopUsers = users.map(id => (id, 1))
      .reduceByKey((a, b) => a + b)

    approxTopUsers.foreachRDD(rdd => {
      if (rdd.count() != 0) {
        val partial = rdd.first()
        val partialTopK = partial.heavyHitters.map(id =>
          (id, partial.frequency(id).estimate)).toSeq.sortBy(_._2).reverse.slice(0, TOPK)
        globalCMS ++= partial
        val globalTopK = globalCMS.heavyHitters.map(id =>
          (id, globalCMS.frequency(id).estimate)).toSeq.sortBy(_._2).reverse.slice(0, TOPK)
        println("Approx heavy hitters at %2.2f%% threshold this batch: %s".format(PERC,
          partialTopK.mkString("[", ",", "]")))
        println("Approx heavy hitters at %2.2f%% threshold overall: %s".format(PERC,
          globalTopK.mkString("[", ",", "]")))
      }
    })

    exactTopUsers.foreachRDD(rdd => {
      if (rdd.count() != 0) {
        val partialMap = rdd.collect().toMap
        val partialTopK = rdd.map(
          {case (id, count) => (count, id)})
          .sortByKey(ascending = false).take(TOPK)
        globalExact = mm.plus(globalExact.toMap, partialMap)
        val globalTopK = globalExact.toSeq.sortBy(_._2).reverse.slice(0, TOPK)
        println("Exact heavy hitters this batch: %s".format(partialTopK.mkString("[", ",", "]")))
        println("Exact heavy hitters overall: %s".format(globalTopK.mkString("[", ",", "]")))
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
