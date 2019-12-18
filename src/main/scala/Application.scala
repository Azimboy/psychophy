import Models._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Application {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "./target")
      .config("spark.broadcast.compress", value = false)
      .config("spark.shuffle.compress", value = false)
      .config("spark.shuffle.spill.compress",value = false)
      .appName("Testing Spark Session")
      .getOrCreate()

    import spark.implicits._

    val path = this.getClass.getResource("monoton.json").getPath
    val monotonDF = spark.read.option("multiline", value = true).schema(experimentsSchema).json(path)

    val monotonDf = monotonDF.select(explode($"experiments").as("exp"))
      .withColumn("name", $"exp.name")
      .withColumn("analysis", $"exp.analysis")
      .drop("exp")

    val avgerage = udf((xs: Seq[Double]) => xs.sum / xs.size)
    val modeDf = monotonDf.select(
      $"name",
      array_max($"analysis.avgErrorTime").as("avgErrorTimeMax"),
      array_min($"analysis.avgErrorTime").as("avgErrorTimeMin"),
      avgerage($"analysis.avgErrorTime").as("avgErrorTimesAvg"),
      array_max($"analysis.preErrorTime").as("preErrorTimeMax"),
      array_min($"analysis.preErrorTime").as("preErrorTimeMin"),
      avgerage($"analysis.preErrorTime").as("preErrorTimeAvg")
    )

    modeDf.printSchema()
    modeDf.show()

  }
}
