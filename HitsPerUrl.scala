/**
 * Created by arun on 5/4/15.
 */
package com.cloudwick.spark.loganalysis
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

/**
 * The HitsPerUrl gives the number of hits per URL.
 *
 * Example command to run:
 * % spark-submit
 *   --class "com.cloudwick.spark.loganalysis.HitsPerURL"
 *   --master local[4]
 *   target/scala-2.10/scala-2.10/myspark_2.10-1.0.jar
 *   /Users/arun/mylogpath/mock_apache_pool-1-thread-1.data
 */

object HitsPerUrl {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("HitsPerUrl")
    val sc = new SparkContext(sparkConf)

    val logFile = args(0)

    val accessLogs = sc.textFile(logFile).map(ApacheAccessLog.parseLogLine).cache()




    // Any IPAddress that has accessed the server more than 10 times.
    val ipAddresses = accessLogs
      .map(log => (log.ipAddress, 1))
      .reduceByKey(_ + _)
      .take(100)
    println(s"""Hits Per Hour: ${ipAddresses.mkString("[", ",", "]")}""")


    sc.stop()
  }
}

