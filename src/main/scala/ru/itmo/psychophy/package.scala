package ru.itmo

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

package object psychophy {

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

  case class Analyse(minutes: Int, skippedReactions: Int, excessReactions: Int, avgErrorTime: Double, preErrorTime: Double)

  case class Monoton(name: String, analysis: Seq[Analyse])

  case class Experiments(experiments: Seq[Monoton])

  val experimentsSchema = ScalaReflection.schemaFor[Experiments].dataType.asInstanceOf[StructType]

  //  case class Details(fullName: String, heartRate: Double, levelFs: Double, markFs: Double, rrnn: Double, sdnn: Double)
  //  val experimentsSchema = ScalaReflection.schemaFor[Details].dataType.asInstanceOf[StructType]

}
