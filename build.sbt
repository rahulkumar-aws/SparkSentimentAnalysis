name := "SparkSentimentAnalysis"

version := "0.1"

scalaVersion := "2.11.11"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.2.0"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.2.0"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.2.0"
libraryDependencies += "org.apache.bahir" % "spark-streaming-twitter_2.11" % "2.2.0"
libraryDependencies += "com.typesafe" % "config" % "1.3.1"

// https://mvnrepository.com/artifact/com.twitter/algebird-core_2.10
libraryDependencies += "com.twitter" % "algebird-core_2.10" % "0.13.3"








        