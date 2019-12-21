package ru.itmo.psychophy

import org.apache.spark.sql.functions.udf

object Utils {

  val count1 = udf((list: Seq[Int]) => list.count(_ == 1))

  val avgerage = udf((list: Seq[Double]) => list.sum / list.size)

  val mode = udf((list: Seq[Double]) => list.groupBy(i => i).mapValues(_.size).maxBy(_._2)._1)

  val median = udf((list: Seq[Double]) => {
      val (lower, upper) = list.sortWith(_ < _).splitAt(list.size / 2)
      if (list.size % 2 == 0) (lower.last + upper.head) / 2.0 else upper.head
    })

}
