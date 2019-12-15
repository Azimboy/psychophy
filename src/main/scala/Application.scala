import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

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

    val monotonSchema = ScalaReflection.schemaFor[Monoton].dataType.asInstanceOf[StructType]

    import spark.implicits._

    val path = this.getClass.getResource("monoton.json").getPath
    val monotonDF = spark.read.schema(monotonSchema).json(path).as[Monoton]

    val count = monotonDF.map(a => a.analysis.size)
    count.show()
    monotonDF.printSchema()
  }
}
