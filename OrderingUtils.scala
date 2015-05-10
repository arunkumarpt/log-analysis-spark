/**
 * Created by arun on 5/4/15.
 */
package com.cloudwick.spark.loganalysis
import scala.math.Ordering

object OrderingUtils {
  object SecondValueOrdering extends Ordering[(String, Int)] {
    def compare(a: (String, Int), b: (String, Int)) = {
      a._2 compare b._2
    }
  }

  object SecondValueLongOrdering extends Ordering[(String, Long)] {
    def compare(a: (String, Long), b: (String, Long)) = {
      a._2 compare b._2
    }
  }
}
