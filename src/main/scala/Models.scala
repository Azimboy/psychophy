import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

object Models {

  case class Analyse(minutes: Int, skippedReactions: Int, excessReactions: Int, avgErrorTime: Double, preErrorTime: Double)

  case class Monoton(name: String, analysis: Seq[Analyse])

  case class Experiments(experiments: Seq[Monoton])

  val experimentsSchema = ScalaReflection.schemaFor[Experiments].dataType.asInstanceOf[StructType]

  //    Manual schema
  //    val analysisSchema = StructType(
  //      Seq[StructField](
  //        StructField("minutes", IntegerType),
  //        StructField("skippedReactions", IntegerType),
  //        StructField("excessReactions", IntegerType),
  //        StructField("avgErrorTime", DoubleType),
  //        StructField("preErrorTime", DoubleType)
  //      )
  //    )
  //
  //    val experimentsSchema = StructType(
  //      Seq[StructField](
  //        StructField("name", StringType),
  //        StructField("analysis", ArrayType(analysisSchema))
  //      )
  //    )
  //
  //    val mainSchema = StructType(
  //      Seq[StructField](
  //        StructField("experiments", ArrayType(experimentsSchema))
  //      )
  //    )

}
