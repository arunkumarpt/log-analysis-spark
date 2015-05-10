/**
 * Created by arun on 5/4/15.
 */
package com.cloudwick.spark.loganalysis
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

/**
 * The TopIpaddresses takes in an apache access log file and
 * return top 10 IP Addresses.
 * Example command to run:
 * % spark-submit
 *   --class "com.cloudwick.spark.loganalysis.TopIpaddresses"
 *   --master local[4]
 *   target/scala-2.10/scala-2.10/myspark_2.10-1.0.jar
 *   /Users/arun/mylogpath/mock_apache_pool-1-thread-1.data
 */

object TopIpaddresses {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("TopIpaddresses")
    val sc = new SparkContext(sparkConf)

    val logFile = args(0)

    val accessLogs = sc.textFile(logFile).map(ApacheAccessLog.parseLogLine).cache()



    // Any IPAddress that has accessed the server more than 10 times.
    val ipAddresses = accessLogs
      .map(log => (log.ipAddress, 1))
      .reduceByKey(_ + _)
      .filter(_._2 > 10)
      .map(_._1)
      .take(100)
    println(s"""IPAddresses > 10 times: ${ipAddresses.mkString("[", ",", "]")}""")



    sc.stop()
  }
}
