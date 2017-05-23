package org.apache.spark.utils

/**
 * Created by administrator on 2015/11/7.
 */
trait Logging extends org.apache.spark.internal.Logging {
  implicit def doubleToString(x: Double) = x.toString
  implicit def intToString(x: Int) = x.toString
  implicit def longToString(x: Long) = x.toString
  implicit def floatToString(x: Float) = x.toString
  implicit def mapToString(m:Map[String,Int])=m.toString()

}
