package com.github.jelmerk.spark.knn.bruteforce

import java.io.InputStream
import java.net.InetSocketAddress

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

import com.github.jelmerk.knn.ObjectSerializer
import com.github.jelmerk.knn.scalalike.{DistanceFunction, Item}
import com.github.jelmerk.knn.scalalike.bruteforce.BruteForceIndex
import com.github.jelmerk.registration.server.PartitionAndReplica
import com.github.jelmerk.serving.client.IndexClientFactory
import com.github.jelmerk.spark.knn._
import org.apache.spark.SparkContext
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.{Identifiable, MLReadable, MLReader, MLWritable, MLWriter}

private[bruteforce] trait BruteForceIndexType extends IndexType {
  protected override type TIndex[TId, TVector, TItem <: Item[TId, TVector], TDistance] =
    BruteForceIndex[TId, TVector, TItem, TDistance]

  protected override implicit def indexClassTag[TId: ClassTag, TVector: ClassTag, TItem <: Item[
    TId,
    TVector
  ]: ClassTag, TDistance: ClassTag]: ClassTag[TIndex[TId, TVector, TItem, TDistance]] =
    ClassTag(classOf[TIndex[TId, TVector, TItem, TDistance]])
}

private[bruteforce] trait BruteForceIndexLoader extends IndexLoader with BruteForceIndexType {
  protected def loadIndex[TId, TVector, TItem <: Item[TId, TVector] with Product, TDistance](
      inputStream: InputStream,
      minCapacity: Int
  ): BruteForceIndex[TId, TVector, TItem, TDistance] = BruteForceIndex.loadFromInputStream(inputStream)
}

private[bruteforce] trait BruteForceModelCreator extends ModelCreator[BruteForceSimilarityModel] {
  protected def createModel[
      TId: TypeTag,
      TVector: TypeTag,
      TItem <: Item[TId, TVector] with Product: TypeTag,
      TDistance: TypeTag
  ](
      uid: String,
      numPartitions: Int,
      numReplicas: Int,
      numThreads: Int,
      sparkContext: SparkContext,
      indices: Map[PartitionAndReplica, InetSocketAddress],
      clientFactory: IndexClientFactory[TId, TVector, TDistance],
      taskGroup: String
  ): BruteForceSimilarityModel =
    new BruteForceSimilarityModelImpl[TId, TVector, TItem, TDistance](
      uid,
      numPartitions,
      numReplicas,
      numThreads,
      sparkContext,
      indices,
      clientFactory,
      taskGroup
    )
}

/** Companion class for BruteForceSimilarityModel. */
object BruteForceSimilarityModel extends MLReadable[BruteForceSimilarityModel] {

  private[knn] class BruteForceModelReader
      extends KnnModelReader[BruteForceSimilarityModel]
      with BruteForceModelCreator
      with BruteForceIndexLoader

  override def read: MLReader[BruteForceSimilarityModel] = new BruteForceModelReader
}

/** Model produced by `BruteForceSimilarity`. */
abstract class BruteForceSimilarityModel
    extends KnnModelBase[BruteForceSimilarityModel]
    with KnnModelParams
    with MLWritable

private[knn] class BruteForceSimilarityModelImpl[
    TId,
    TVector,
    TItem <: Item[TId, TVector] with Product,
    TDistance
](
    override val uid: String,
    val numPartitions: Int,
    val numReplicas: Int,
    val numThreads: Int,
    val sparkContext: SparkContext,
    val indexAddresses: Map[PartitionAndReplica, InetSocketAddress],
    val clientFactory: IndexClientFactory[TId, TVector, TDistance],
    val taskGroup: String
)(implicit val idTypeTag: TypeTag[TId], val vectorTypeTag: TypeTag[TVector])
    extends BruteForceSimilarityModel
    with KnnModelOps[
      BruteForceSimilarityModel,
      TId,
      TVector,
      TItem,
      TDistance,
      BruteForceIndex[TId, TVector, TItem, TDistance]
    ] {

//  override implicit protected def idTypeTag: TypeTag[TId] = typeTag[TId]

  override def copy(extra: ParamMap): BruteForceSimilarityModel = {
    val copied = new BruteForceSimilarityModelImpl[TId, TVector, TItem, TDistance](
      uid,
      numPartitions,
      numReplicas,
      numThreads,
      sparkContext,
      indexAddresses,
      clientFactory,
      taskGroup
    )
    copyValues(copied, extra).setParent(parent)
  }

  override def write: MLWriter = new KnnModelWriter[
    BruteForceSimilarityModel,
    TId,
    TVector,
    TItem,
    TDistance,
    BruteForceIndex[TId, TVector, TItem, TDistance]
  ](this)

  override protected def loadIndex(in: InputStream): BruteForceIndex[TId, TVector, TItem, TDistance] =
    BruteForceIndex.loadFromInputStream[TId, TVector, TItem, TDistance](in)

}

/** Nearest neighbor search using a brute force approach. This will be very slow. It is in most cases not recommended
  * for production use. But can be used to determine the accuracy of an approximative index.
  *
  * @param uid
  *   identifier
  */
class BruteForceSimilarity(override val uid: String)
    extends KnnAlgorithm[BruteForceSimilarityModel](uid)
    with BruteForceModelCreator
    with BruteForceIndexLoader {

  def this() = this(Identifiable.randomUID("brute_force"))

  override protected def createIndex[TId, TVector, TItem <: Item[TId, TVector] with Product, TDistance](
      dimensions: Int,
      maxItemCount: Int,
      distanceFunction: DistanceFunction[TVector, TDistance]
  )(implicit
      distanceOrdering: Ordering[TDistance],
      idSerializer: ObjectSerializer[TId],
      itemSerializer: ObjectSerializer[TItem]
  ): BruteForceIndex[TId, TVector, TItem, TDistance] =
    BruteForceIndex[TId, TVector, TItem, TDistance](dimensions, distanceFunction)

  override protected def emptyIndex[TId, TVector, TItem <: Item[TId, TVector] with Product, TDistance]
      : BruteForceIndex[TId, TVector, TItem, TDistance] =
    BruteForceIndex.empty[TId, TVector, TItem, TDistance]

  override def copy(extra: ParamMap): BruteForceSimilarity = defaultCopy(extra)
}
