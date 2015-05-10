/**
 * Created by arun on 5/4/15.
 */

package com.cloudwick.spark.loganalysis
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

/**
 * The HitsPerHour finds the hits happend in a hourly basis.
 *
 * Example command to run:
 * % spark-submit
 *   --class "com.cloudwick.spark.loganalysis.HitsPerHour"
 *   --master local[4]
 *  target/scala-2.10/scala-2.10/myspark_2.10-1.0.jar
 *   /Users/arun/mylogpath/mock_apache_pool-1-thread-1.data
 */

object HitsPerHour {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("HitsPerHour")
    val sc = new SparkContext(sparkConf)

    val logFile = args(0)

    val accessLogs = sc.textFile(logFile).map(ApacheAccessLog.parseLogLine).cache()




    // Any IPAddress that has accessed the server more than 10 times.
    val ipAddresses = accessLogs
      .map(log => (log.dateTime.substring(12,14), 1))
      .reduceByKey(_ + _)
      .take(100)
    val formattedIpAddress = ipAddresses.map(t => (t._1+ " to " + (t._1.toInt + 1), t._2))
    println(s"""Hourly visit : ${formattedIpAddress.mkString(" ")}""")


    sc.stop()
  }
}
