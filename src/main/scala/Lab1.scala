import BatchJob._

object Lab1 {

  def main(args: Array[String]): Unit = {
    import sparkSession._

    val beforeDf = sparkSession.read
      .format("csv")
      .option("header", "true")
      .load(getFilePath("psy_stress_before.csv"))

    beforeDf.printSchema()
    beforeDf.show()

  }
}
