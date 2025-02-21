package com.github.jelmerk.spark.knn.evaluation

import java.io.File

import com.github.jelmerk.TestHelpers._
import com.github.jelmerk.spark.SharedSparkContext
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

case class Neighbor[TId, TDistance](neighbor: TId, distance: TDistance)

class KnnSimilarityEvaluatorSpec extends AnyWordSpec with SharedSparkContext {

  import spark.implicits._

  private val evaluator = new KnnSimilarityEvaluator()
    .setApproximateNeighborsCol("approximate")
    .setExactNeighborsCol("exact")

  "KnnSimilarityEvaluator" should {

    "evaluate performance" in {
      val df = Seq(
        Seq(Neighbor("1", 0.1f), Neighbor("2", 0.2f)) -> Seq(Neighbor("1", 0.1f), Neighbor("2", 0.2f)),
        Seq(Neighbor("3", 0.1f))                      -> Seq(Neighbor("3", 0.1f), Neighbor("4", 0.9f))
      ) toDF ("approximate", "exact")

      evaluator.evaluate(df) should be(0.75)
    }

    "evaluate performance for no results as 1" in {
      val df = Seq(
        Seq.empty[Neighbor[Int, Float]] -> Seq.empty[Neighbor[Int, Float]]
      ) toDF ("approximate", "exact")

      evaluator.evaluate(df) should be(1)
    }

    "save and load evaluator" in {
      withTempFolder { dir =>
        val path = new File(dir, "evaluator").toString
        evaluator.write.overwrite().save(path)

        val loadedEvaluator = KnnSimilarityEvaluator.read.load(path)

        loadedEvaluator.getApproximateNeighborsCol should be(evaluator.getApproximateNeighborsCol)
        loadedEvaluator.getExactNeighborsCol should be(evaluator.getExactNeighborsCol)
      }
    }
  }

}
