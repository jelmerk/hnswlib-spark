package com.github.jelmerk.spark.knn.hnsw

import java.io.InputStream
import java.net.InetSocketAddress

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

import com.github.jelmerk.knn
import com.github.jelmerk.knn.scalalike.{DistanceFunction, Item}
import com.github.jelmerk.knn.scalalike.hnsw._
import com.github.jelmerk.registration.server.PartitionAndReplica
import com.github.jelmerk.serving.client.IndexClientFactory
import com.github.jelmerk.spark.knn._
import org.apache.spark.SparkContext
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._

private[hnsw] trait HnswIndexType extends IndexType {
  protected override type TIndex[TId, TVector, TItem <: Item[TId, TVector], TDistance] =
    HnswIndex[TId, TVector, TItem, TDistance]

  protected override implicit def indexClassTag[TId: ClassTag, TVector: ClassTag, TItem <: Item[
    TId,
    TVector
  ]: ClassTag, TDistance: ClassTag]: ClassTag[TIndex[TId, TVector, TItem, TDistance]] =
    ClassTag(classOf[TIndex[TId, TVector, TItem, TDistance]])

}

private[hnsw] trait HnswIndexLoader extends IndexLoader with HnswIndexType {
  protected override def loadIndex[TId, TVector, TItem <: Item[TId, TVector] with Product, TDistance](
      inputStream: InputStream,
      minCapacity: Int
  ): HnswIndex[TId, TVector, TItem, TDistance] = {
    val index = HnswIndex.loadFromInputStream[TId, TVector, TItem, TDistance](inputStream)
    index.resize(index.maxItemCount + minCapacity)
    index
  }
}

private[hnsw] trait HnswModelCreator extends ModelCreator[HnswSimilarityModel] {
  @SuppressWarnings(Array("MaxParameters"))
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
      jobGroup: String
  ): HnswSimilarityModel =
    new HnswSimilarityModelImpl[TId, TVector, TItem, TDistance](
      uid,
      numPartitions,
      numReplicas,
      numThreads,
      sparkContext,
      indices,
      clientFactory,
      jobGroup
    )
}

private[hnsw] trait HnswParams extends KnnAlgorithmParams with HnswModelParams {

  /** Size of the dynamic list for the nearest neighbors (used during the search). Default: 10
    *
    * @group param
    */
  final val ef = new IntParam(
    this,
    "ef",
    "size of the dynamic list for the nearest neighbors (used during the search)",
    ParamValidators.gt(0)
  )

  /** @group getParam */
  final def getEf: Int = $(ef)

  /** The number of bi-directional links created for every new element during construction.
    *
    * Default: 16
    *
    * @group param
    */
  final val m = new IntParam(
    this,
    "m",
    "number of bi-directional links created for every new element during construction",
    ParamValidators.gt(0)
  )

  /** @group getParam */
  final def getM: Int = $(m)

  /** Has the same meaning as ef, but controls the index time / index precision. Default: 200
    *
    * @group param
    */
  final val efConstruction = new IntParam(
    this,
    "efConstruction",
    "has the same meaning as ef, but controls the index time / index precision",
    ParamValidators.gt(0)
  )

  /** @group getParam */
  final def getEfConstruction: Int = $(efConstruction)

  setDefault(m -> 16, efConstruction -> 200, ef -> 10)
}

/** Common params for Hnsw and HnswModel.
  */
private[hnsw] trait HnswModelParams extends KnnModelParams

/** Companion class for HnswSimilarityModel.
  */
object HnswSimilarityModel extends MLReadable[HnswSimilarityModel] {

  private[hnsw] class HnswModelReader
      extends KnnModelReader[HnswSimilarityModel]
      with HnswIndexLoader
      with HnswModelCreator

  override def read: MLReader[HnswSimilarityModel] = new HnswModelReader

}

/** Model produced by `HnswSimilarity`.
  */
abstract class HnswSimilarityModel extends KnnModelBase[HnswSimilarityModel] with HnswModelParams with MLWritable

private[knn] class HnswSimilarityModelImpl[
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
    val jobGroup: String
)(implicit val idTypeTag: TypeTag[TId], val vectorTypeTag: TypeTag[TVector])
    extends HnswSimilarityModel
    with KnnModelOps[HnswSimilarityModel, TId, TVector, TItem, TDistance, HnswIndex[TId, TVector, TItem, TDistance]] {

  override def copy(extra: ParamMap): HnswSimilarityModel = {
    val copied = new HnswSimilarityModelImpl[TId, TVector, TItem, TDistance](
      uid,
      numPartitions,
      numReplicas,
      numThreads,
      sparkContext,
      indexAddresses,
      clientFactory,
      jobGroup
    )
    copyValues(copied, extra).setParent(parent)
  }

  override def write: MLWriter =
    new KnnModelWriter[HnswSimilarityModel, TId, TVector, TItem, TDistance, HnswIndex[TId, TVector, TItem, TDistance]](
      this
    )

  override protected def loadIndex(in: InputStream): HnswIndex[TId, TVector, TItem, TDistance] =
    HnswIndex.loadFromInputStream[TId, TVector, TItem, TDistance](in)

}

object HnswSimilarity extends DefaultParamsReadable[HnswSimilarity]

/** Nearest neighbor search using the approximative hnsw algorithm.
  *
  * @param uid
  *   identifier
  */
class HnswSimilarity(override val uid: String)
    extends KnnAlgorithm[HnswSimilarityModel](uid)
    with HnswParams
    with HnswIndexLoader
    with HnswModelCreator {

  def this() = this(Identifiable.randomUID("hnsw"))

  /** @group setParam */
  def setM(value: Int): this.type = set(m, value)

  /** @group setParam */
  def setEf(value: Int): this.type = set(ef, value)

  /** @group setParam */
  def setEfConstruction(value: Int): this.type = set(efConstruction, value)

  override protected def createIndex[TId, TVector, TItem <: Item[TId, TVector] with Product, TDistance](
      dimensions: Int,
      maxItemCount: Int,
      distanceFunction: DistanceFunction[TVector, TDistance]
  )(implicit
      distanceOrdering: Ordering[TDistance],
      idSerializer: knn.ObjectSerializer[TId],
      itemSerializer: knn.ObjectSerializer[TItem]
  ): HnswIndex[TId, TVector, TItem, TDistance] =
    HnswIndex[TId, TVector, TItem, TDistance](
      dimensions,
      distanceFunction,
      maxItemCount,
      getM,
      getEf,
      getEfConstruction,
      removeEnabled = false,
      idSerializer,
      itemSerializer
    )

  override protected def emptyIndex[TId, TVector, TItem <: Item[TId, TVector] with Product, TDistance]
      : HnswIndex[TId, TVector, TItem, TDistance] =
    HnswIndex.empty[TId, TVector, TItem, TDistance]

  override def copy(extra: ParamMap): HnswSimilarity = defaultCopy(extra)
}
