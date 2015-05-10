/**
 * Created by arun on 5/4/15.
 */
package com.cloudwick.spark.loganalysis
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

/**
 * The LogSizeAggregator takes in an apache access log file and
 * computes min, max and avg of content size of the log.
 *
 * Example command to run:
 * % spark-submit
 *   --class "com.cloudwick.spark.loganalysis.LogSizeAggregator"
 *   --master local[4]
 *   target/scala-2.10/scala-2.10/myspark_2.10-1.0.jar
 *   /Users/arun/mylogpath/mock_apache_pool-1-thread-1.data
 */

object LogSizeAggregator {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("LogSizeAggregator")
    val sc = new SparkContext(sparkConf)

    val logFile = args(0)

    val accessLogs = sc.textFile(logFile).map(ApacheAccessLog.parseLogLine)

    // Calculate statistics based on the content size.
    val contentSizes = accessLogs.map(log => log.contentSize).cache()
    println("Content Size Avg: %s, Min: %s, Max: %s".format(
      contentSizes.reduce(_ + _) / contentSizes.count,
      contentSizes.min,
      contentSizes.max))



    sc.stop()
  }
}
