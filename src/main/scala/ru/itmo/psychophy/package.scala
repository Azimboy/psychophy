package ru.itmo

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

package object psychophy {

  //  Manual schema
  //  val analysisSchema = StructType(
  //    Seq[StructField](
  //      StructField(minutes, IntegerType),
  //      StructField(skippedReactions, IntegerType),
  //      StructField(excessReactions, IntegerType),
  //      StructField(avgErrorTime, DoubleType),
  //      StructField(preErrorTime, DoubleType)
  //    )
  //  )
  //
  //  val experimentsSchema = StructType(
  //    Seq[StructField](
  //      StructField(name, StringType),
  //      StructField(analysis, ArrayType(analysisSchema))
  //    )
  //  )
  //
  //  val mainSchema = StructType(
  //    Seq[StructField](
  //      StructField(experiments, ArrayType(experimentsSchema))
  //    )
  //  )

  case class Analyse(minutes: Int, skippedReactions: Int, excessReactions: Int, avgErrorTime: Double, preErrorTime: Double)

  case class Monoton(name: String, analysis: Seq[Analyse])

  case class Experiments(experiments: Seq[Monoton])

  val experimentsSchema = ScalaReflection.schemaFor[Experiments].dataType.asInstanceOf[StructType]

  case class Result(
    avgPres1: Double, avgPres2: Double, avgPres3: Double,
    avgSkip1: Double, avgSkip2: Double, avgSkip3: Double,
    avgExсess1: Double, avgExсess2: Double, avgExсess3: Double,
    avgErrorTime1: Double, avgErrorTime2: Double, avgErrorTime3: Double,
    avgPredom1: Double, avgPredom2: Double, avgPredom3: Double
  )

  case class Pek(name: String, results: Seq[Result])

  case class PekExperiments(experiments: Seq[Pek])

  val pekExperimentsSchema = ScalaReflection.schemaFor[PekExperiments].dataType.asInstanceOf[StructType]
  //  case class Details(fullName: String, heartRate: Double, levelFs: Double, markFs: Double, rrnn: Double, sdnn: Double)
  //  val experimentsSchema = ScalaReflection.schemaFor[Details].dataType.asInstanceOf[StructType]

}
