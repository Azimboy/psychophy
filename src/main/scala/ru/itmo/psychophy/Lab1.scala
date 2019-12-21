package ru.itmo.psychophy

import org.apache.spark.sql.functions.{avg, collect_list, count}
import org.apache.spark.sql.types.DoubleType
import ru.itmo.psychophy.BatchJob.sparkSession
import ru.itmo.psychophy.Utils._

object Lab1 {

  def main(args: Array[String]): Unit = {
    import sparkSession.implicits._

    val beforeDs = sparkSession.read
      .option("header", "true")
      .csv(getFilePath("psy_stress_before.csv"))
      .withColumn("heartRate", 'heartRate.cast(DoubleType))
      .withColumn("levelFs", 'levelFs.cast(DoubleType))
      .withColumn("markFs", 'markFs.cast(DoubleType))
      .withColumn("rrnn", 'rrnn.cast(DoubleType))
      .withColumn("sdnn", 'sdnn.cast(DoubleType))

    beforeDs.printSchema()
    beforeDs.show()

    val levelGroupDs = beforeDs.groupBy($"levelFs").agg(
      count("levelFs").as("countLevelFs"),
      collect_list($"fullName").as("names"),
      avg($"heartRate").as("avgHeartRate"),
      avg($"markFs").as("avgMarkFs"),
      avg($"rrnn").as("avgRrnn"),
      avg($"sdnn").as("avgSdnn"),
      collect_list($"markFs").as("markFsList")
    )
      .withColumn("modeMarkFs", mode($"markFsList"))
      .withColumn("medianMarkFs", median($"markFsList"))

    levelGroupDs.printSchema()
    levelGroupDs.show()
  }
}
