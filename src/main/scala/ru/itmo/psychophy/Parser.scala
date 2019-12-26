package ru.itmo.psychophy

import ru.itmo.psychophy.StartSession._

object Parser {

  def main(args: Array[String]): Unit = {
    val rom = sparkSession.read
      .csv(getFilePath("PEN_EKG.csv"))

    rom.printSchema()
    import sparkSession.implicits._
    val table = rom.collect.map { row =>
      (0 to 5).map { i =>
        val cell = row(i).toString
        val startIndex = cell.indexOf(" = ")
        val key = cell.substring(0, startIndex - 1)
        val value = cell.substring(startIndex + 3, cell.length)
        println(key + "|" + value)
        key -> value
      }
    }.flatMap(_.groupBy(_._1).mapValues(_.map(_._2).mkString(" "))).toSeq.toDF

    table.printSchema()

    table.coalesce(1)
      .write
      .option("header","true")
      .option("sep",",")
      .mode("overwrite")
      .csv("src/main/resources/parced")

  }
}
