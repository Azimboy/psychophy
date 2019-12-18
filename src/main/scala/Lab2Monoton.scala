import BatchJob._
import Models.{mode, _}
import org.apache.spark.sql.functions._
import vegas._
import vegas.sparkExt._

object Lab2Monoton {

  def main(args: Array[String]): Unit = {

    import sparkSession.implicits._

    val path = this.getClass.getResource("monoton.json").getPath
    val monotonDF = sparkSession.read.option("multiline", value = true).schema(experimentsSchema).json(path)

    val monotonDf = monotonDF.select(explode($"experiments").as("exp"))
      .withColumn("name", $"exp.name")
      .withColumn("analysis", $"exp.analysis")
      .drop("exp")

    val avgErrorTimesDf = monotonDf.select(
      $"name",
      count1($"analysis.skippedReactions").as("skippedReactionsCount"),
      count1($"analysis.excessReactions").as("excessReactionsCount"),
      array_max($"analysis.avgErrorTime").as("avgErrorTimeMax"),
      array_min($"analysis.avgErrorTime").as("avgErrorTimeMin"),
      avgerage($"analysis.avgErrorTime").as("avgErrorTimesAvg"),
      mode($"analysis.avgErrorTime").as("avgErrorTimeMode"),
      median($"analysis.avgErrorTime").as("avgErrorTimeMedian")
    ).withColumn("avgErrorTimeRange", $"avgErrorTimeMax" - $"avgErrorTimeMin")

    val preErrorTimesDf = monotonDf.select(
      $"name",
      count1($"analysis.skippedReactions").as("skippedReactionsCount"),
      count1($"analysis.excessReactions").as("excessReactionsCount"),
      array_max($"analysis.preErrorTime").as("preErrorTimeMax"),
      array_min($"analysis.preErrorTime").as("preErrorTimeMin"),
      avgerage($"analysis.preErrorTime").as("preErrorTimeAvg"),
      mode($"analysis.preErrorTime").as("preErrorTimeMode"),
      median($"analysis.preErrorTime").as("preErrorTimeMedian")
    ).withColumn("preErrorTimeRange", $"preErrorTimeMax" - $"preErrorTimeMin")

    avgErrorTimesDf.show()
    preErrorTimesDf.show()

    val plot = Vegas("Country Pop")
      .withDataFrame(avgErrorTimesDf)
      .encodeX("name", Nom)
      .encodeY("avgErrorTimeMedian", Quant)
//      .encodeColor("name", Nom)
      .mark(Bar)

    plot.show

  }

}
