package org.apache.spark.mllib.clustering.dbscan

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

class SparkConstruct(inputType: String, outputType: String, appName: String) extends java.io.Serializable {

  var sparkSession: SparkSession = null
  var sparkContext: SparkContext = null

  def init(): Unit = {
    val conf = new SparkConf().setAppName(appName)
    if (inputType == "local" && outputType == "local") {
      conf.setMaster("local")
    }
    if (inputType == "hive" || outputType == "hive") {
      sparkSession = SparkSession.builder.config(conf).enableHiveSupport().getOrCreate()
    } else {
      sparkSession = SparkSession.builder.config(conf).getOrCreate()
    }
    sparkContext = sparkSession.sparkContext
    sparkSession.sparkContext.setLogLevel("ERROR")
  }

  def read(inputPath: String): RDD[String] = {
    if (inputType == "hive") {
      readFromHive(inputPath)
    } else {
      readTextFile(inputPath)
    }
  }

  def readTextFile(inputPath: String): RDD[String] = {
    sparkSession.sparkContext.textFile(inputPath)
  }

  def readFromHive(sqlStr: String): RDD[String] = {
    val result = sparkSession.sql(sqlStr).rdd.map {
      x => {
        val list = new ListBuffer[String]();
        for (i <- 0 until x.length) {
          list.append(x.get(i).toString)
        }
        list.mkString("\t")
      }
    }
    result
  }

  def readDF(inputPath: String, columns: Array[String]): DataFrame = {
    if (inputType == "hive") {
      readFromHiveDF(inputPath, columns)
    } else {
      readTextFileDF(inputPath, columns)
    }
  }

  def readDF(inputPath: String): DataFrame = {
    if (inputType == "hive") {
      readFromHiveDF(inputPath, null)
    } else {
      val inputSchema = inputPath.split(";")
      if (inputSchema.length != 2) {
        System.err.println("input schema[" + inputPath + "] error")
        System.exit(-1)
      }
      val realInputPath = inputSchema(0)
      val columns = inputSchema(1).split(",")
      readTextFileDF(realInputPath, columns)
    }
  }

  def readTextFileDF(inputPath: String, columns: Array[String]): DataFrame = {
    val fields = columns.map {
      fieldName => StructField(fieldName, StringType, nullable = true)
    }
    val schema = StructType(fields)
    val inputRDD = sparkSession.sparkContext.textFile(inputPath).map {
      x => x.split("\t").toSeq
    }.filter {
      x => x.length == columns.length
    }.map {
      Row.fromSeq(_)
    }
    val inputDF = sparkSession.createDataFrame(inputRDD, schema)
    inputDF
  }

  def readFromHiveDF(sqlStr: String, columns: Array[String] = null): DataFrame = {
    var result: DataFrame = sparkSession.sql(sqlStr)
    if (columns != null && columns.length != 0) {
      result = result.toDF(columns: _*)
    }
    result
  }

  def save(outputPath: String, outputDF: DataFrame): Unit = {
    if (outputType == "hive") {
      saveHive(outputPath, outputDF)
    } else {
      outputDF.rdd.map {
        x => {
          val list = new ListBuffer[String]();
          for (i <- 0 until x.length) {
            list.append(x.get(i).toString)
          }
          list.mkString("\t")
        }

      }.saveAsTextFile(outputPath)
    }
  }

  def saveHive(outputPath: String, outputDF: DataFrame): Unit = {
    val table_schema = outputPath.split(";")
    if (table_schema.length != 3 && table_schema.length != 4) {
      System.err.println("output table schema[" + outputPath + "] error")
      System.exit(-1)
    }
    val table_name = table_schema(0)
    var partition = table_schema(1).replaceAll("/", ",")
    if (partition != "") {
      val partitionTemplate = "PARTITION (%s)"
      partition = partitionTemplate.format(partition)
    }
    val columns = if (table_schema(2) == "") null else table_schema(2).split(",")
    val is_create = table_schema.length == 4 && table_schema(3) == "true" && partition == ""
    var result = outputDF
    var columnNames = table_schema(2)
    if (columns != null) {
      result = outputDF.toDF(columns: _*)
    } else {
      columnNames = "*"
    }
    result.createOrReplaceTempView("temp")
    val sqlList = new ListBuffer[String]()
    var sqlTemplate = "INSERT OVERWRITE TABLE %s %s SELECT %s FROM temp"
    var sqlStr = sqlTemplate.format(table_name, partition, columnNames)
    if (is_create) {
      sqlTemplate = "CREATE TABLE %s AS SELECT %s FROM temp"
      sqlStr = sqlTemplate.format(table_name, columnNames)
      val sqlDropTable = "DROP TABLE IF EXISTS %s".format(table_name)
      sqlList.append(sqlDropTable)
    }
    sqlList.append(sqlStr)
    for (sqlItem <- sqlList) {
      sparkSession.sql(sqlItem)
    }
  }

  def save(outputPath: String, outputRdd: RDD[String]): Unit = {
    if (outputType == "hive") {
      saveHive(outputPath, outputRdd)
    } else {
      outputRdd.saveAsTextFile(outputPath)
    }
  }

  def saveHive(outputPath: String, outputRdd: RDD[String]): Unit = {
    val table_schema = outputPath.split(";")
    if (table_schema.length != 3 && table_schema.length != 4) {
      System.err.println("output table schema[" + outputPath + "] error")
      System.exit(-1)
    }
    val table_name = table_schema(0)
    var partition = table_schema(1).replaceAll("/", ",")
    if (partition != "") {
      val partitionTemplate = "PARTITION (%s)"
      partition = partitionTemplate.format(partition)
    }
    val columns = table_schema(2).split(",")
    val is_create = table_schema.length == 4 && table_schema(3) == "true" && partition == ""
    val outputRddRow = outputRdd.map {
      x => x.split("\t").toSeq
    }.filter {
      x => x.length == columns.length
    }.map {
      Row.fromSeq(_)
    }

    val fields = columns.map {
      fieldName => StructField(fieldName, StringType, nullable = true)
    }
    val schema = StructType(fields)
    val outputDataFrame = sparkSession.createDataFrame(outputRddRow, schema)
    outputDataFrame.createOrReplaceTempView("temp")
    val sqlList = new ListBuffer[String]()
    var sqlTemplate = "INSERT OVERWRITE TABLE %s %s SELECT %s FROM temp"
    var sqlStr = sqlTemplate.format(table_name, partition, table_schema(2))
    if (is_create) {
      sqlTemplate = "CREATE TABLE %s AS SELECT %s FROM temp"
      sqlStr = sqlTemplate.format(table_name, table_schema(2))
      val sqlDropTable = "DROP TABLE IF EXISTS %s".format(table_name)
      sqlList.append(sqlDropTable)
    }
    sqlList.append(sqlStr)
    for (sqlItem <- sqlList) {
      sparkSession.sql(sqlItem)
    }
  }

  def stop(): Unit = {
    sparkSession.stop()
  }

}
