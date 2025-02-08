package com.github.jelmerk.spark.conversion

import com.github.jelmerk.spark.SharedSparkContext
import org.apache.spark.ml.linalg._
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

class VectorConverterSpec extends AnyWordSpec with SharedSparkContext {

  import spark.implicits._

  private implicit val denseVectorEncoder: Encoder[DenseVector] = ExpressionEncoder()

  "VectorConverter" should {

    "convert dense vector to float array" in {
      val input     = Seq(Tuple1(Vectors.dense(Array(1d, 2d, 3d)))).toDF("in")
      val converter = new VectorConverter().setInputCol("in").setOutputCol("out").setOutputType("array<float>")
      val result    = converter.transform(input).select("out").as[Array[Float]].head()

      result should be(Array(1f, 2f, 3f))
    }

    "convert double array to float array" in {
      val input     = Seq(Tuple1(Array(1d, 2d, 3d))).toDF("in")
      val converter = new VectorConverter().setInputCol("in").setOutputCol("out").setOutputType("array<float>")
      val result    = converter.transform(input).select("out").as[Array[Float]].head()

      result should be(Array(1f, 2f, 3f))
    }

    "convert dense vector to double array" in {
      val input     = Seq(Tuple1(Vectors.dense(Array(1d, 2d, 3d)))).toDF("in")
      val converter = new VectorConverter().setInputCol("in").setOutputCol("out").setOutputType("array<double>")
      val result    = converter.transform(input).select("out").as[Array[Double]].head()

      result should be(Array(1d, 2d, 3d))
    }

    "convert float array to double array" in {
      val input     = Seq(Tuple1(Array(1f, 2f, 3f))).toDF("in")
      val converter = new VectorConverter().setInputCol("in").setOutputCol("out").setOutputType("array<double>")
      val result    = converter.transform(input).select("out").as[Array[Double]].head()

      result should be(Array(1d, 2d, 3d))
    }

    "convert float array to dense vector" in {
      val input     = Seq(Tuple1(Array(1f, 2f, 3f))).toDF("in")
      val converter = new VectorConverter().setInputCol("in").setOutputCol("out").setOutputType("vector")
      val result    = converter.transform(input).select("out").as[DenseVector].head()

      result should be(Vectors.dense(Array(1d, 2d, 3d)))
    }

    "convert double array to dense vector" in {
      val input     = Seq(Tuple1(Array(1d, 2d, 3d))).toDF("in")
      val converter = new VectorConverter().setInputCol("in").setOutputCol("out").setOutputType("vector")
      val result    = converter.transform(input).select("out").as[DenseVector].head()

      result should be(Vectors.dense(Array(1d, 2d, 3d)))
    }

  }

}
