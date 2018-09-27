package org.apache.spark.mllib.clustering.dbscan

import org.apache.spark.mllib.clustering.dbscan.DBSCAN
import org.apache.spark.mllib.linalg.Vectors
import org.rogach.scallop._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
//import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.functions._


class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val eps = opt[Double](required = true, descr = "半径")
  val minPoints = opt[Int](required = true, descr = "最少点数量")
  val maxPointsPerPartition = opt[Int](required = true, descr = "分区最多点数量")
  val taskName = opt[String](descr = "任务名称")
  val inputSql = opt[String](descr = "输入数据")
  val rowKey = opt[String](descr = "rowKey")
  //    val name = trailArg[String]()
  verify()
}

object Main {

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)
    val sparkConstruct = new SparkConstruct("hive", "hive", conf.taskName())
    sparkConstruct.init()
    val data = sparkConstruct.readDF(conf.inputSql())
    val row_key = data.select(conf.rowKey())
    data.drop(conf.rowKey())
    //    df.select(array(df.columns.map(col(_)): _*)).rdd.map(_.getSeq[Double](0))
    val new_rdd = data.rdd.map(f => f.toSeq.map(_.toString.toDouble)).map(f => Vectors.dense(f.toArray))

    DBSCAN.train(new_rdd, conf.eps(), conf.minPoints(), conf.maxPointsPerPartition())


  }
}
