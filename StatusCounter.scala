/**
 * Created by arun on 5/4/15.
 */

package com.cloudwick.spark.loganalysis
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

/**
 * The StatusCounter aggragate the log messages based on the status code.
 *
 * Example command to run:
 *   --class "com.cloudwick.spark.loganalysis.StatusCounter"
 *   --master local[4]
 *   target/scala-2.10/scala-2.10/myspark_2.10-1.0.jar
 *   /Users/arun/mylogpath/mock_apache_pool-1-thread-1.data
 */

object StatusCounter {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("StatusCounter")
    val sc = new SparkContext(sparkConf)

    val logFile = args(0)

    val accessLogs = sc.textFile(logFile).map(ApacheAccessLog.parseLogLine)





    val responseCount = accessLogs
      .map(log => (log.responseCode, 1))
      .reduceByKey(_ + _)
      .take(100)

    println(s"""REspose code count : ${responseCount.mkString(" ")}""")


    sc.stop()
  }
}


