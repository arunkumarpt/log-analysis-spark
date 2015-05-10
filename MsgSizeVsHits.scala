/**
 * Created by arun on 5/4/15.
 */

package com.cloudwick.spark.loganalysis
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

/**
 * The MsgSizeVsHits calculate the message size and aggregate according to that.
 *
 * Example command to run:
 *   --class "com.cloudwick.spark.loganalysis.MsgSizeVsHits"
 *   --master local[4]
 *   target/scala-2.10/scala-2.10/myspark_2.10-1.0.jar
 *   /Users/arun/mylogpath/mock_apache_pool-1-thread-1.data
 */

object MsgSizeVsHits {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("MsgSizeVsHits")
    val sc = new SparkContext(sparkConf)

    val logFile = args(0)

    val accessLogs = sc.textFile(logFile).map(ApacheAccessLog.parseLogLine).cache()





    val mssgsize = accessLogs
      .map(log => ((log.contentSize)/1024, 1))
      .reduceByKey(_ + _)
      .take(100)

    println(s"""Message size based count : ${mssgsize.mkString(" ")}""")


    sc.stop()
  }
}

