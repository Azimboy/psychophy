object Models {

  case class Analyse(minutes: Int, skippedReactions: Int, excessReactions: Int, avgErrorTime: Double, prevalenceErrorTime: Double)

  case class Monoton(analyses: Seq[Analyse])

}
