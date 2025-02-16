package com.github.jelmerk.spark.linalg

import java.io.File

import com.github.jelmerk.TestHelpers._
import com.github.jelmerk.spark.SharedSparkContext
import org.apache.spark.ml.linalg._
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

class NormalizerSpec extends AnyWordSpec with SharedSparkContext {

  import spark.implicits._

  private implicit val denseVectorEncoder: Encoder[DenseVector]   = ExpressionEncoder()
  private implicit val sparseVectorEncoder: Encoder[SparseVector] = ExpressionEncoder()

  private val normalizer = new Normalizer()
    .setInputCol("vector")
    .setOutputCol("normalized")

  "Normalizer" should {

    "normalize sparse vector" in {
      val input  = Seq(Tuple1(Vectors.sparse(3, Array(0, 1, 2), Array(0.01, 0.02, 0.03)))).toDF("vector")
      val result = normalizer.transform(input).select("normalized").as[SparseVector].head()
      result should be(
        Vectors.sparse(3, Array(0, 1, 2), Array(0.2672612419124244, 0.5345224838248488, 0.8017837257372731))
      )
    }

    "normalize dense vector" in {
      val input  = Seq(Tuple1(Vectors.dense(Array(0.01, 0.02, 0.03)))).toDF("vector")
      val result = normalizer.transform(input).select("normalized").as[DenseVector].head()
      result should be(Vectors.dense(Array(0.2672612419124244, 0.5345224838248488, 0.8017837257372731)))
    }

    "normalize double array" in {
      val input  = Seq(Tuple1(Array(0.01, 0.02, 0.03))).toDF("vector")
      val result = normalizer.transform(input).select("normalized").as[Array[Double]].head()
      result should be(Array(0.2672612419124244, 0.5345224838248488, 0.8017837257372731))
    }

    "normalize float array" in {
      val input  = Seq(Tuple1(Array(0.01f, 0.02f, 0.03f))).toDF("vector")
      val result = normalizer.transform(input).select("normalized").as[Array[Float]].head()
      result should be(Array(0.26726124f, 0.5345225f, 0.8017837f))
    }

    "save and load normalizer" in {
      withTempFolder { dir =>
        val path = new File(dir, "normalizer").toString
        normalizer.write.overwrite().save(path)

        val loadedNormalizer = Normalizer.read.load(path)

        loadedNormalizer.getInputCol should be(normalizer.getInputCol)
        loadedNormalizer.getOutputCol should be(normalizer.getOutputCol)
      }
    }
  }

}
