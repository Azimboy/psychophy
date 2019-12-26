package ru.itmo.psychophy

import org.apache.spark.sql.functions._
import ru.itmo.psychophy.StartSession.{getFilePath, saveToCsv, sparkSession}
import ru.itmo.psychophy.Utils._

object Lab4MonotonAnalsis {

  def main(args: Array[String]): Unit = {
    import sparkSession.implicits._

//    val monotonDf = sparkSession.read
//      .option("header", "true")
//      .csv(getFilePath("lab4/data.csv"))
//
//    saveToCsv(monotonDf, "lab4/data")

    val monBeforeDf = sparkSession.read
      .option("header", "true")
      .csv(getFilePath("lab4/data_before.csv"))

    val monAfterDf = sparkSession.read
      .option("header", "true")
      .csv(getFilePath("lab4/data_after.csv"))

    monBeforeDf.show()
    monAfterDf.show()
  }

}
