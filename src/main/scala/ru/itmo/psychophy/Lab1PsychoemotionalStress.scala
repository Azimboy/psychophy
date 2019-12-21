package ru.itmo.psychophy

import org.apache.spark.sql.functions._
import ru.itmo.psychophy.StartSession.{loadFromCsv, sparkSession}
import ru.itmo.psychophy.Utils._

object Lab1PsychoemotionalStress {

  def main(args: Array[String]): Unit = {
    import sparkSession.implicits._

    val psyStressBeforeDs = loadFromCsv("psy_stress_after.csv")
      .groupBy($"levelFs").agg(
      count("levelFs").as("countLevelFs"),
      collect_list($"fullName").as("names"),
      avg($"heartRate").as("avgHeartRate"),
      avg($"markFs").as("avgMarkFs"),
      stddev($"markFs").as("avgStdDev"),
      avg($"rrnn").as("avgRrnn"),
      avg($"sdnn").as("avgSdnn"),
      collect_list($"markFs").as("markFsList")
    )
      .withColumn("modeMarkFs", mode($"markFsList"))
      .withColumn("medianMarkFs", median($"markFsList"))
      .sort($"levelFs")

    val psyStressAfterDs = loadFromCsv("psy_stress_before.csv")
      .groupBy($"levelFs").agg(
      count("levelFs").as("countLevelFs"),
      collect_list($"fullName").as("names"),
      avg($"heartRate").as("avgHeartRate"),
      avg($"markFs").as("avgMarkFs"),
      stddev($"markFs").as("avgStdDev"),
      avg($"rrnn").as("avgRrnn"),
      avg($"sdnn").as("avgSdnn"),
      collect_list($"markFs").as("markFsList")
    )
      .withColumn("modeMarkFs", mode($"markFsList"))
      .withColumn("medianMarkFs", median($"markFsList"))
      .sort($"levelFs")

    psyStressBeforeDs.show()
    psyStressAfterDs.show()
  }

}
