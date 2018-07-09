package util

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

object SparkUtils {
  def getSparkContext: SparkContext = {
    val sparkConf = new SparkConf()
    new SparkContext(sparkConf)
  }

  def getHiveContext(sc: SparkContext): HiveContext = {
    val hc = new HiveContext(sc)
    hc.setConf("spark.sql.hive.convertMetastoreOrc", "false")
    hc
  }
}
