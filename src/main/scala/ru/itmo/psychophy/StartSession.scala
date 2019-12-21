package ru.itmo.psychophy

import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object StartSession {

  val sparkSession = SparkSession
    .builder()
    .master("local[*]")
    .config("spark.sql.warehouse.dir", "./target")
    .config("spark.broadcast.compress", value = false)
    .config("spark.shuffle.compress", value = false)
    .config("spark.shuffle.spill.compress",value = false)
    .appName("Testing Spark Session")
    .getOrCreate()

  sparkSession.sparkContext.setLogLevel("ERROR")

  import sparkSession.implicits._

  val getFilePath = (fileName: String) => getClass.getClassLoader.getResource(fileName).getPath

  def loadFromJson(fileName: String): DataFrame = {
    sparkSession.read
      .option("multiline", value = true)
      .schema(experimentsSchema)
      .json(getFilePath(fileName))
  }

  def loadFromCsv(fileName: String): DataFrame = {
    sparkSession.read
      .option("header", "true")
      .csv(getFilePath(fileName))
      .withColumn("heartRate", 'heartRate.cast(IntegerType))
      .withColumn("levelFs", 'levelFs.cast(IntegerType))
      .withColumn("markFs", 'markFs.cast(DoubleType))
      .withColumn("rrnn", 'rrnn.cast(IntegerType))
      .withColumn("sdnn", 'sdnn.cast(IntegerType))
  }

}
