package ru.itmo.psychophy

import org.apache.spark.sql.functions._
import ru.itmo.psychophy.StartSession._

object Lab3ProgramDataAnalysis {

  def main(args: Array[String]): Unit = {

    import sparkSession.implicits._

    val pekDf = loadFromJson("lab3/pek.json", pekExperimentsSchema)
      .select(explode($"experiments").as("exp"))
      .withColumn("name", $"exp.name")
      .withColumn("results", $"exp.results")
      .drop("exp")

    pekDf.printSchema()
    pekDf.show()

    val avgMinMaxErrorTimesDf = pekDf.select(
      $"name",
      size($"results").as("level"),
      array_min($"results.avgErrorTime1").as("avgErrorTimeMin1"),
      array_min($"results.avgErrorTime2").as("avgErrorTimeMin2"),
      array_min($"results.avgErrorTime3").as("avgErrorTimeMin3"),
      array_max($"results.avgErrorTime1").as("avgErrorTimeMax1"),
      array_max($"results.avgErrorTime2").as("avgErrorTimeMax2"),
      array_max($"results.avgErrorTime3").as("avgErrorTimeMax3")
    )

    avgMinMaxErrorTimesDf.show()

    saveToCsv(avgMinMaxErrorTimesDf, "pek")
  }

}
