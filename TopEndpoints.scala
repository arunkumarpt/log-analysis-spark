/**
 * Created by arun on 5/4/15.
 */
package com.cloudwick.spark.loganalysis
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

/**
 * The TopEndpoints finds the top 10 end points.
 *
 * Example command to run:
 * % spark-submit
 *   --class "com.cloudwick.spark.loganalysis.TopEndpoints"
 *   --master local[4]
 *   target/scala-2.10/scala-2.10/myspark_2.10-1.0.jar
 *   /Users/arun/mylogpath/mock_apache_pool-1-thread-1.data
 */

object TopEndpoints {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("TopEndpoints")
    val sc = new SparkContext(sparkConf)

    val logFile = args(0)

    val accessLogs = sc.textFile(logFile).map(ApacheAccessLog.parseLogLine).cache()


    // Top Endpoints.
    val topEndpoints = accessLogs
      .map(log => (log.endpoint, 1))
      .reduceByKey(_ + _)
      .top(10)(OrderingUtils.SecondValueOrdering)
    println(s"""Top Endpoints: ${topEndpoints.mkString("[", ",", "]")}""")

    sc.stop()
  }
}

