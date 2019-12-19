import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.StructType

object Models {

  case class Analyse(minutes: Int, skippedReactions: Int, excessReactions: Int, avgErrorTime: Double, preErrorTime: Double)

  case class Monoton(name: String, analysis: Seq[Analyse])

  case class Experiments(experiments: Seq[Monoton])

  val experimentsSchema = ScalaReflection.schemaFor[Experiments].dataType.asInstanceOf[StructType]

//  Manual schema
//  val analysisSchema = StructType(
//    Seq[StructField](
//      StructField("minutes", IntegerType),
//      StructField("skippedReactions", IntegerType),
//      StructField("excessReactions", IntegerType),
//      StructField("avgErrorTime", DoubleType),
//      StructField("preErrorTime", DoubleType)
//    )
//  )
//
//  val experimentsSchema = StructType(
//    Seq[StructField](
//      StructField("name", StringType),
//      StructField("analysis", ArrayType(analysisSchema))
//    )
//  )
//
//  val mainSchema = StructType(
//    Seq[StructField](
//      StructField("experiments", ArrayType(experimentsSchema))
//    )
//  )

//  Helper functions
  val count1 = udf((list: Seq[Int]) => list.count(_ == 1))

  val avgerage = udf((list: Seq[Double]) => list.sum / list.size)

  val mode = udf((list: Seq[Double]) => list.groupBy(i => i).mapValues(_.size).maxBy(_._2)._1)

  val median = udf((list: Seq[Double]) => {
      val (lower, upper) = list.sortWith(_<_).splitAt(list.size / 2)
      if (list.size % 2 == 0) (lower.last + upper.head) / 2.0 else upper.head
    })

}
