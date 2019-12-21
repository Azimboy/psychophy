package ru.itmo.psychophy

import org.apache.spark.sql.functions._
import ru.itmo.psychophy.StartSession.{loadFromCsv, sparkSession}
import ru.itmo.psychophy.Utils._

object Lab2MonotonousActivity {

  def main(args: Array[String]): Unit = {
    import sparkSession.implicits._

    val monotonBeforeDs = loadFromCsv("monoton_after.csv")
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

    val monotonAfterDs = loadFromCsv("monoton_before.csv")
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

    monotonBeforeDs.show()
    monotonAfterDs.show()
  }

}
