package ru.itmo.psychophy

import org.apache.spark.sql.functions._
import ru.itmo.psychophy.StartSession._
import ru.itmo.psychophy.Utils._
import vegas.sparkExt._
import vegas.{Bar, Nom, Quant, Vegas}

object Lab3PekDataAnalysis {

  def main(args: Array[String]): Unit = {

    import sparkSession.implicits._

    val pekBeforeDf = sparkSession.read
      .option("header", "true")
      .csv(getFilePath("lab3/data_before.csv"))

    val pekAfterDf = sparkSession.read
      .option("header", "true")
      .csv(getFilePath("lab3/data_after.csv"))

    pekBeforeDf.show()
    pekAfterDf.show()

  }

}
