package com.github.jelmerk.spark.knn.hnsw

import java.io.File

import com.github.jelmerk.TestHelpers._
import com.github.jelmerk.spark.SharedSparkContext
import org.apache.spark.SparkConf
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

class HnswSimilaritySpec extends AnyWordSpec with SharedSparkContext {

  import spark.implicits._

  override def conf: SparkConf = super.conf
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryo.registrator", "com.github.jelmerk.spark.HnswLibKryoRegistrator")
    .set("spark.speculation", "false")
    .set("spark.ui.enabled", "true")

  private val hnsw = new HnswSimilarity()
    .setIdentifierCol("id")
    .setFeaturesCol("vector")
    .setNumPartitions(2)
    .setNumReplicas(0)
    .setNumThreads(1)
    .setK(10)
    .setPredictionCol("neighbors")

  "HnswSimilarity" should {

    "query pre-partitioned data" in {

      val similarity = hnsw
        .copy(ParamMap.empty)
        .setPartitionCol("partition")
        .setQueryPartitionsCol("partitions")

      val indexItems = Seq(
        PrePartitionedInputRow(partition = 0, id = 1000000, vector = Vectors.dense(0.0110, 0.2341)),
        PrePartitionedInputRow(partition = 0, id = 2000000, vector = Vectors.dense(0.2300, 0.3891)),
        PrePartitionedInputRow(partition = 1, id = 3000000, vector = Vectors.dense(0.4300, 0.9891))
      ).toDF()

      withDisposableResource(similarity.fit(indexItems)) { model =>
        val queries = Seq(QueryRow(partitions = Seq(0), id = 123, vector = Vectors.dense(0.2400, 0.3891))).toDF()

        val result = model
          .transform(queries)
          .as[OutputRow[Int, DenseVector, Double]]
          .head()

        result.neighbors.size should be(2) // it couldn't see 3000000 because we only query partition 0

      }
    }

    "index and query dense vectors" in {

      val items = Seq(
        InputRow(1000000L, Vectors.dense(0.0110, 0.2341)),
        InputRow(2000000L, Vectors.dense(0.2300, 0.3891)),
        InputRow(3000000L, Vectors.dense(0.4300, 0.9891))
      ).toDF()

      withDisposableResource(hnsw.fit(items)) { model =>
        val results = model
          .transform(items)
          .as[OutputRow[Long, DenseVector, Double]]
          .collect()
          .groupBy(_.id)
          .mapValues(_.head)

        results(1000000L).neighbors.map(_.neighbor) should be(Seq(1000000L, 3000000L, 2000000L))
        results(2000000L).neighbors.map(_.neighbor) should be(Seq(2000000L, 3000000L, 1000000L))
        results(3000000L).neighbors.map(_.neighbor) should be(Seq(3000000L, 2000000L, 1000000L))

      }
    }

    "index and query sparse vectors" in {
      val items = Seq(
        InputRow("1000000", Vectors.sparse(2, Array(0, 1), Array(0.0110, 0.2341))),
        InputRow("2000000", Vectors.sparse(2, Array(0, 1), Array(0.2300, 0.3891))),
        InputRow("3000000", Vectors.sparse(2, Array(0, 1), Array(0.4300, 0.9891)))
      ).toDF()

      withDisposableResource(hnsw.fit(items)) { model =>
        val results = model
          .transform(items)
          .as[OutputRow[String, SparseVector, Double]]
          .collect()
          .groupBy(_.id)
          .mapValues(_.head)

        results("1000000").neighbors.map(_.neighbor) should be(Seq("1000000", "3000000", "2000000"))
        results("2000000").neighbors.map(_.neighbor) should be(Seq("2000000", "3000000", "1000000"))
        results("3000000").neighbors.map(_.neighbor) should be(Seq("3000000", "2000000", "1000000"))

      }
    }

    "index and query float array vectors" in {

      val items = Seq(
        InputRow(1000000, Array(0.0110f, 0.2341f)),
        InputRow(2000000, Array(0.2300f, 0.3891f)),
        InputRow(3000000, Array(0.4300f, 0.9891f))
      ).toDF()

      withDisposableResource(hnsw.fit(items)) { model =>
        val results = model
          .transform(items)
          .as[OutputRow[Int, Array[Float], Float]]
          .collect()
          .groupBy(_.id)
          .mapValues(_.head)

        results(1000000).neighbors.map(_.neighbor) should be(Seq(1000000, 3000000, 2000000))
        results(2000000).neighbors.map(_.neighbor) should be(Seq(2000000, 3000000, 1000000))
        results(3000000).neighbors.map(_.neighbor) should be(Seq(3000000, 2000000, 1000000))

      }
    }

    "index and query double array vectors" in {
      val items = Seq(
        InputRow(1000000, Array(0.0110, 0.2341)),
        InputRow(2000000, Array(0.2300, 0.3891)),
        InputRow(3000000, Array(0.4300, 0.9891))
      ).toDF()

      withDisposableResource(hnsw.fit(items)) { model =>
        val results = model
          .transform(items)
          .as[OutputRow[Int, Array[Double], Double]]
          .collect()
          .groupBy(_.id)
          .mapValues(_.head)

        results(1000000).neighbors.map(_.neighbor) should be(Seq(1000000, 3000000, 2000000))
        results(2000000).neighbors.map(_.neighbor) should be(Seq(2000000, 3000000, 1000000))
        results(3000000).neighbors.map(_.neighbor) should be(Seq(3000000, 2000000, 1000000))

      }

    }

    "save and load hnsw" in {
      withTempFolder { folder =>
        val path = new File(folder, "hnsw")
        hnsw.write.overwrite().save(path.toString)

        val loadedHnsw = HnswSimilarity.read.load(path.toString)

        loadedHnsw.getIdentifierCol should be(hnsw.getIdentifierCol)
        loadedHnsw.getFeaturesCol should be(hnsw.getFeaturesCol)
      }
    }

    "save and load hnsw model" in {
      withTempFolder { folder =>
        val items = Seq(
          InputRow(1000000, Array(0.0110f, 0.2341f)),
          InputRow(2000000, Array(0.2300f, 0.3891f)),
          InputRow(3000000, Array(0.4300f, 0.9891f))
        ).toDF()

        val path = new File(folder, "model").getCanonicalPath

        withDisposableResource(hnsw.fit(items)) { model =>
          model.write.overwrite.save(path)
        }

        withDisposableResource(HnswSimilarityModel.load(path)) { model =>
          val query = InputRow(1000000, Array(0.0110f, 0.2341f))

          val queryItems = Seq(query).toDF()

          val result = model.transform(queryItems).as[OutputRow[Int, Array[Float], Float]].head()

          result.id should be(query.id)
          result.vector should be(query.vector)
          result.neighbors should be(
            Seq(Neighbor(1000000, 0.0f), Neighbor(3000000, 0.06521261f), Neighbor(2000000, 0.11621308f))
          )

        }
      }

    }
  }

}

case class PrePartitionedInputRow[TId, TVector](partition: Int, id: TId, vector: TVector)

case class QueryRow[TId, TVector](partitions: Seq[Int], id: TId, vector: TVector)

case class InputRow[TId, TVector](id: TId, vector: TVector)

case class Neighbor[TId, TDistance](neighbor: TId, distance: TDistance)

case class OutputRow[TId, TVector, TDistance](id: TId, vector: TVector, neighbors: Seq[Neighbor[TId, TDistance]])
