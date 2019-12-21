package ru.itmo.psychophy

import org.apache.spark.sql.SparkSession

object BatchJob {

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

}
