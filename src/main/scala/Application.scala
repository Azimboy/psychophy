import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders

object Application {
  import Models._

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "./target")
      .config("spark.broadcast.compress", value = false)
      .config("spark.shuffle.compress", value = false)
      .config("spark.shuffle.spill.compress",value = false)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("Testing Spark Session")
      .getOrCreate()

    val monotonSchema = Encoders.product[Monoton].schema

    import spark.implicits._

    val path = this.getClass.getResource("monoton.json").getPath
    val monotonDF = spark.read.schema(monotonSchema).json(path)

    monotonDF.printSchema()
  }
}
