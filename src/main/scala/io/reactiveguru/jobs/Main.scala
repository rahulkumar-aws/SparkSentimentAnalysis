package io.reactiveguru.jobs
import org.apache.spark.sql.SparkSession

import scala.io.Source
object Main {

  case class TweetData(id: String, text: String)
  def main(args: Array[String]): Unit = {
    val appName = "sparksentiment"
    val master = "local[*]"
    val spark = SparkSession
      .builder()
      .appName(appName)
      .master(master)
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val filepath = getClass.getResource("/data/small.txt").getPath
    val datafile = sc.textFile(filepath)
    val afinnFilePath = getClass.getResource("/data/AFINN.txt").getPath
    val AFINN = sc.textFile(afinnFilePath).map(x=> x.split("\t")).map(x=>(x(0).toString,x(1).toInt))
    val linewithuiqeIndex = datafile.zipWithUniqueId().map(x=>{
      TweetData(x._2.toString,x._1)
    })
    val tweetDF = spark.createDataFrame(linewithuiqeIndex)

    tweetDF.createTempView("tweet")
    val extracted_tweets = spark.sql("select id,text from tweet").collect

    val tweetsSenti = extracted_tweets.map(tweetText => {
      val tweetWordsSentiment = tweetText(1).toString.split(" ").map(word => {
        var senti: Int = 0
        if (AFINN.lookup(word.toLowerCase()).length > 0) {
          senti = AFINN.lookup(word.toLowerCase())(0)
        }
        senti
      })
      val tweetSentiment = tweetWordsSentiment.sum
      (tweetSentiment ,tweetText.toString)
    })
    val tweetsSentiRDD: org.apache.spark.rdd.RDD[(Int, String)] = sc.parallelize(tweetsSenti.toList).sortBy(x => x._1, false)
    tweetsSentiRDD.foreach(println)
    spark.close()
  }

}
