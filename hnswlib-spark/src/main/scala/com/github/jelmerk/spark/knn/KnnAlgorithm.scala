package com.github.jelmerk.spark.knn

import java.io.InputStream
import java.net.{InetAddress, InetSocketAddress}
import java.nio.charset.StandardCharsets

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.language.higherKinds
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import scala.util.Failure

import com.github.jelmerk.hnswlib.scala._
import com.github.jelmerk.index.IndexClientFactory
import com.github.jelmerk.index.IndexServerFactory
import com.github.jelmerk.registration.{PartitionAndReplica, RegistrationServerFactory}
import com.github.jelmerk.registration.RegistrationClient
import com.github.jelmerk.spark.Disposable
import com.github.jelmerk.spark.util.SerializableConfiguration
import org.apache.hadoop.fs.Path
import org.apache.spark.{Partitioner, SparkContext, SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasFeaturesCol, HasPredictionCol}
import org.apache.spark.ml.util.{DefaultParamsWritable, MLReader, MLWriter}
import org.apache.spark.rdd.RDD
import org.apache.spark.resource.{ResourceProfileBuilder, TaskResourceRequests}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.json4s._
import org.json4s.jackson.Serialization.{read, write}

private[knn] final case class ModelMetaData(
    `class`: String,
    timestamp: Long,
    sparkVersion: String,
    uid: String,
    identifierType: String,
    vectorType: String,
    numPartitions: Int,
    numReplicas: Int,
    numThreads: Int,
    paramMap: ModelParameters
)

final private case class ModelParameters(
    featuresCol: String,
    predictionCol: String,
    k: Int,
    queryPartitionsCol: Option[String]
)

private[knn] trait IndexType {

  /** Type of index. */
  protected type TIndex[TId, TVector, TItem <: Item[TId, TVector], TDistance] <: Index[TId, TVector, TItem, TDistance]

  /** ClassTag of index type. */
  protected implicit def indexClassTag[TId: ClassTag, TVector: ClassTag, TItem <: Item[
    TId,
    TVector
  ]: ClassTag, TDistance: ClassTag]: ClassTag[TIndex[TId, TVector, TItem, TDistance]]
}

private[knn] trait IndexCreator extends IndexType {

  /** Create the index used to do the nearest neighbor search.
    *
    * @param dimensions
    *   dimensionality of the items stored in the index
    * @param maxItemCount
    *   maximum number of items the index can hold
    * @param distanceFunction
    *   the distance function
    * @param distanceOrdering
    *   the distance ordering
    * @param idSerializer
    *   invoked for serializing ids when saving the index
    * @param itemSerializer
    *   invoked for serializing items when saving items
    *
    * @tparam TId
    *   type of the index item identifier
    * @tparam TVector
    *   type of the index item vector
    * @tparam TItem
    *   type of the index item
    * @tparam TDistance
    *   type of distance between items
    * @return
    *   the created index
    */
  protected def createIndex[
      TId,
      TVector,
      TItem <: Item[TId, TVector] with Product,
      TDistance
  ](dimensions: Int, maxItemCount: Int, distanceFunction: DistanceFunction[TVector, TDistance])(implicit
      distanceOrdering: Ordering[TDistance],
      idSerializer: ObjectSerializer[TId],
      itemSerializer: ObjectSerializer[TItem]
  ): TIndex[TId, TVector, TItem, TDistance]

  /** Create an immutable empty index.
    *
    * @tparam TId
    *   type of the index item identifier
    * @tparam TVector
    *   type of the index item vector
    * @tparam TItem
    *   type of the index item
    * @tparam TDistance
    *   type of distance between items
    * @return
    *   the created index
    */
  protected def emptyIndex[
      TId,
      TVector,
      TItem <: Item[TId, TVector] with Product,
      TDistance
  ]: TIndex[TId, TVector, TItem, TDistance]
}

private[knn] trait IndexLoader extends IndexType {

  /** Load an index
    *
    * @param inputStream
    *   InputStream to restore the index from
    * @param minCapacity
    *   loaded index needs to have space for at least this man additional items
    *
    * @tparam TId
    *   type of the index item identifier
    * @tparam TVector
    *   type of the index item vector
    * @tparam TItem
    *   type of the index item
    * @tparam TDistance
    *   type of distance between items
    * @return
    *   create an index
    */
  protected def loadIndex[TId, TVector, TItem <: Item[TId, TVector] with Product, TDistance](
      inputStream: InputStream,
      minCapacity: Int
  ): TIndex[TId, TVector, TItem, TDistance]
}

private[knn] trait ModelCreator[TModel <: KnnModelBase[TModel]] {

  /** Creates the model to be returned from fitting the data.
    *
    * @param uid
    *   identifier
    * @param numPartitions
    *   number of partitions
    * @param numReplicas
    *   number of replicas
    * @param numThreads
    *   number of threads
    * @param sparkContext
    *   the spark context
    * @param indices
    *   map of index servers
    * @param clientFactory
    *   builds index clients
    * @param jobGroup
    *   jobGroup that holds the indices
    * @tparam TId
    *   type of the index item identifier
    * @tparam TVector
    *   type of the index item vector
    * @tparam TItem
    *   type of the index item
    * @tparam TDistance
    *   type of distance between items
    * @return
    *   model
    */
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
  ): TModel
}

/** Common params for KnnAlgorithm and KnnModel. */
@SuppressWarnings(Array("CollectionIndexOnNonIndexedSeq"))
private[knn] trait KnnModelParams extends Params with HasFeaturesCol with HasPredictionCol {

  /** Param for the column name for the query partitions.
    *
    * @group param
    */
  final val queryPartitionsCol = new Param[String](this, "queryPartitionsCol", "column name for the query partitions")

  /** @group getParam */
  final def getQueryPartitionsCol: String = $(queryPartitionsCol)

  /** Param for number of neighbors to find (> 0). Default: 5
    *
    * @group param
    */
  final val k = new IntParam(this, "k", "number of neighbors to find", ParamValidators.gt(0))

  /** @group getParam */
  final def getK: Int = $(k)

  setDefault(
    k             -> 5,
    predictionCol -> "prediction",
    featuresCol   -> "features"
  )

  /** Validates the input schema and appends a prediction column to it.
    *
    * @param schema
    *   The input schema of the dataset.
    * @param identifierDataType
    *   The type of the identifier column
    * @return
    *   The transformed schema with an added column for predictions.
    * @throws java.lang.IllegalArgumentException
    *   If the output column (prediction column) already exists in the input schema.
    */
  protected def validateAndTransformSchema(schema: StructType, identifierDataType: DataType): StructType = {

    val distanceType = schema(getFeaturesCol).dataType match {
      case ArrayType(FloatType, _) => FloatType
      case _                       => DoubleType
    }

    val predictionStruct = new StructType()
      .add("neighbor", identifierDataType, nullable = false)
      .add("distance", distanceType, nullable = false)

    val neighborsField = StructField(getPredictionCol, new ArrayType(predictionStruct, containsNull = false))

    if (schema.fieldNames.contains(getPredictionCol)) {
      throw new IllegalArgumentException(s"Output column $getPredictionCol already exists.")
    }
    schema
      .add(neighborsField)
  }
}

/** Params for knn algorithms. */
private[knn] trait KnnAlgorithmParams extends KnnModelParams {

  /** Param for the column name for the row identifier. Default: "id"
    *
    * @group param
    */
  final val identifierCol = new Param[String](this, "identifierCol", "column name for the row identifier")

  /** @group getParam */
  final def getIdentifierCol: String = $(identifierCol)

  /** Number of partitions
    *
    * @group param
    */
  final val numPartitions = new IntParam(this, "numPartitions", "number of partitions", ParamValidators.gt(0))

  /** @group getParam */
  final def getNumPartitions: Int = $(numPartitions)

  /** Param that specifies the number of index replicas to create when querying the index. More replicas means you can
    * execute more queries in parallel at the expense of increased resource usage. Default: 0
    *
    * @group param
    */
  final val numReplicas =
    new IntParam(this, "numReplicas", "number of index replicas to create when querying", ParamValidators.gtEq(0))

  /** @group getParam */
  final def getNumReplicas: Int = $(numReplicas)

  /** Param that specifies the number of threads to use.
    *
    * @group param
    */
  final val numThreads = new IntParam(this, "numThreads", "number of threads to use per index", ParamValidators.gt(0))

  /** @group getParam */
  final def getNumThreads: Int = $(numThreads)

  /** Param for the distance function to use. One of "bray-curtis", "canberra", "cosine", "correlation", "euclidean",
    * "inner-product", "manhattan" or the fully qualified classname of a distance function Default: "cosine"
    *
    * @group param
    */
  final val distanceFunction = new Param[String](this, "distanceFunction", "distance function to use")

  /** @group getParam */
  final def getDistanceFunction: String = $(distanceFunction)

  /** Param for the partition identifier
    *
    * @group param
    */
  final val partitionCol = new Param[String](this, "partitionCol", "column name for the partition identifier")

  /** @group getParam */
  final def getPartitionCol: String = $(partitionCol)

  /** Param to the initial model. All the vectors from the initial model will be included in the final output model.
    *
    * @group param
    */
  final val initialModelPath = new Param[String](this, "initialModelPath", "path to the initial model")

  /** @group getParam */
  final def getInitialModelPath: String = $(initialModelPath)

  setDefault(identifierCol -> "id", distanceFunction -> "cosine", numReplicas -> 0)
}

private[knn] object KnnModelWriter {
  private implicit val format: Formats = DefaultFormats.withLong
}

/** Persists a knn model.
  *
  * @param instance
  *   the instance to persist
  *
  * @tparam TModel
  *   type of the model
  * @tparam TId
  *   type of the index item identifier
  * @tparam TVector
  *   type of the index item vector
  * @tparam TItem
  *   type of the index item
  * @tparam TDistance
  *   type of distance
  * @tparam TIndex
  *   type of the index
  */
private[knn] class KnnModelWriter[
    TModel <: KnnModelBase[TModel],
    TId: TypeTag,
    TVector: TypeTag,
    TItem <: Item[TId, TVector] with Product,
    TDistance,
    TIndex <: Index[TId, TVector, TItem, TDistance]
](instance: TModel with KnnModelOps[TModel, TId, TVector, TItem, TDistance, TIndex])
    extends MLWriter {

  import KnnModelWriter._

  override protected def saveImpl(path: String): Unit = {

    val indicesPath = new Path(path, "indices").toString

    val client = instance.clientFactory.create(instance.indexAddresses)
    try {
      client.saveIndex(indicesPath)
    } finally {
      client.shutdown()
    }

    val metadata = ModelMetaData(
      `class` = instance.getClass.getName,
      timestamp = System.currentTimeMillis(),
      sparkVersion = sc.version,
      uid = instance.uid,
      identifierType = typeDescription[TId],
      vectorType = typeDescription[TVector],
      numPartitions = instance.numPartitions,
      numReplicas = instance.numReplicas,
      numThreads = instance.numThreads,
      paramMap = ModelParameters(
        featuresCol = instance.getFeaturesCol,
        predictionCol = instance.getPredictionCol,
        k = instance.getK,
        queryPartitionsCol = Option(instance.queryPartitionsCol).filter(instance.isSet).map(instance.getOrDefault)
      )
    )

    // Avoid saveAsTextFile since it runs on an executor, and we don’t want to allocate one core just for saving the model.
    val metadataPath = new Path(path, "metadata")

    val fs = metadataPath.getFileSystem(sc.hadoopConfiguration)

    val jsonPath = new Path(metadataPath, "part-00000")
    val out      = fs.create(jsonPath)
    try {
      out.write(write(metadata).getBytes(StandardCharsets.UTF_8))
    } finally {
      out.close()
    }

    fs.create(new Path(metadataPath, "_SUCCESS")).close()

  }
}

private[knn] object KnnModelReader {
  private implicit val format: Formats = DefaultFormats.withLong
}

/** Reads a knn model from persistent storage.
  *
  * @tparam TModel
  *   type of model
  */
private[knn] abstract class KnnModelReader[TModel <: KnnModelBase[TModel]]
    extends MLReader[TModel]
    with IndexLoader
    with IndexServing
    with ModelCreator[TModel]
    with Serializable {

  import KnnModelReader._

  override def load(path: String): TModel = {

    val metadataPath = new Path(path, "metadata").toString

    val metadataStr = sc.textFile(metadataPath, 1).first()

    val metadata = read[ModelMetaData](metadataStr)

    (metadata.identifierType, metadata.vectorType) match {
      case ("int", "float_array") =>
        typedLoad[Int, Array[Float], IntFloatArrayIndexItem, Float](path, metadata)
      case ("int", "double_array") =>
        typedLoad[Int, Array[Double], IntDoubleArrayIndexItem, Double](path, metadata)
      case ("int", "vector") =>
        typedLoad[Int, Vector, IntVectorIndexItem, Double](path, metadata)

      case ("long", "float_array") =>
        typedLoad[Long, Array[Float], LongFloatArrayIndexItem, Float](path, metadata)
      case ("long", "double_array") =>
        typedLoad[Long, Array[Double], LongDoubleArrayIndexItem, Double](path, metadata)
      case ("long", "vector") =>
        typedLoad[Long, Vector, LongVectorIndexItem, Double](path, metadata)

      case ("string", "float_array") =>
        typedLoad[String, Array[Float], StringFloatArrayIndexItem, Float](path, metadata)
      case ("string", "double_array") =>
        typedLoad[String, Array[Double], StringDoubleArrayIndexItem, Double](path, metadata)
      case ("string", "vector") =>
        typedLoad[String, Vector, StringVectorIndexItem, Double](path, metadata)
      case (identifierType, vectorType) =>
        throw new IllegalStateException(
          s"Cannot create model for identifier type $identifierType and vector type $vectorType."
        )
    }
  }

  @SuppressWarnings(Array("MaxParameters"))
  private def typedLoad[
      TId: TypeTag: ClassTag,
      TVector: TypeTag: ClassTag,
      TItem <: Item[TId, TVector] with Product: TypeTag: ClassTag,
      TDistance: TypeTag: ClassTag
  ](path: String, metadata: ModelMetaData)(implicit
      indexServerFactory: IndexServerFactory[TId, TVector, TItem, TDistance],
      clientFactory: IndexClientFactory[TId, TVector, TDistance]
  ): TModel = {

    val indicesPath = new Path(path, "indices")

    val taskReqs = new TaskResourceRequests().cpus(metadata.numThreads)
    val profile  = new ResourceProfileBuilder().require(taskReqs).build()

    val serializableConfiguration = new SerializableConfiguration(sc.hadoopConfiguration)

    val partitionPaths = (0 until metadata.numPartitions).map { partitionId =>
      partitionId -> new Path(indicesPath, partitionId.toString)
    }

    val indexRdd = sc
      .makeRDD(partitionPaths)
      .partitionBy(new PartitionIdPartitioner(metadata.numPartitions))
      .mapPartitions { it =>
        val (partitionId, indexPath) = it.next()
        val fs                       = indexPath.getFileSystem(serializableConfiguration.value)

        logInfo(partitionId, s"Loading index from $indexPath")
        val inputStream = fs.open(indexPath)
        val index       = loadIndex[TId, TVector, TItem, TDistance](inputStream, 0)
        logInfo(partitionId, s"Finished loading index from $indexPath")
        Iterator(index)
      }
      .withResources(profile)

    val jobGroup = metadata.uid + "_" + System.nanoTime()
    val servers  = serve(jobGroup, indexRdd, metadata.numReplicas)

    val model = createModel(
      metadata.uid,
      metadata.numPartitions,
      metadata.numReplicas,
      metadata.numThreads,
      sc,
      servers,
      clientFactory,
      jobGroup
    )

    val params = metadata.paramMap

    params.queryPartitionsCol
      .fold(model)(model.setQueryPartitionsCol)
      .setFeaturesCol(params.featuresCol)
      .setPredictionCol(params.predictionCol)
      .setK(params.k)
  }

}

/** Base class for nearest neighbor search models.
  *
  * @tparam TModel
  *   type of the model
  */
private[knn] abstract class KnnModelBase[TModel <: KnnModelBase[TModel]]
    extends Model[TModel]
    with KnnModelParams
    with Disposable {

  private[knn] def sparkContext: SparkContext

  private[knn] def jobGroup: String

  @volatile private[knn] var disposed: Boolean = false

  /** @group setParam */
  def setQueryPartitionsCol(value: String): this.type = set(queryPartitionsCol, value)

  /** @group setParam */
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  def setK(value: Int): this.type = set(k, value)

  def isDisposed: Boolean = disposed

  /** Disposes of the spark resources associated with this model. Afterward it can no longer be used */
  override def dispose(): Unit = {
    sparkContext.cancelJobGroup(jobGroup)

    disposed = true
  }

}

/** Contains the core knn search logic
  *
  * @tparam TModel
  *   type of the model
  * @tparam TId
  *   type of the index item identifier
  * @tparam TVector
  *   type of the index item vector
  * @tparam TItem
  *   type of the index item
  * @tparam TDistance
  *   type of distance between items
  * @tparam TIndex
  *   type of the index
  */
private[knn] trait KnnModelOps[
    TModel <: KnnModelBase[TModel],
    TId,
    TVector,
    TItem <: Item[TId, TVector] with Product,
    TDistance,
    TIndex <: Index[TId, TVector, TItem, TDistance]
] {
  this: TModel with KnnModelParams =>

  implicit protected def idTypeTag: TypeTag[TId]

  implicit protected def vectorTypeTag: TypeTag[TVector]

  private[knn] def numPartitions: Int

  private[knn] def numReplicas: Int

  private[knn] def numThreads: Int

  private[knn] def indexAddresses: Map[PartitionAndReplica, InetSocketAddress]

  private[knn] def clientFactory: IndexClientFactory[TId, TVector, TDistance]

  protected def loadIndex(in: InputStream): TIndex

  override def transform(dataset: Dataset[_]): DataFrame = {
    if (isDisposed) {
      throw new IllegalStateException("Model is disposed.")
    }

    val spark              = dataset.sparkSession
    val localIndexAddr     = indexAddresses
    val localClientFactory = clientFactory
    val k                  = getK
    val featuresCol        = getFeaturesCol
    val partitionsColOpt   = if (isDefined(queryPartitionsCol)) Some(getQueryPartitionsCol) else None

    val outputSchema = transformSchema(dataset.schema)

    val rowRdd = dataset
      .toDF()
      .rdd
      .mapPartitions { it =>
        new QueryIterator(
          localIndexAddr,
          localClientFactory,
          it,
          batchSize = 1000,
          k,
          featuresCol,
          partitionsColOpt
        )
      }
    spark.createDataFrame(rowRdd, outputSchema)
  }

  override def transformSchema(schema: StructType): StructType = {
    val idDataType = ScalaReflection.encoderFor[TId].dataType
    validateAndTransformSchema(schema, idDataType)
  }

}

private[knn] object KnnAlgorithm {
  private implicit val format: Formats = DefaultFormats.withLong
}

@SuppressWarnings(Array("MaxParameters", "CollectionIndexOnNonIndexedSeq"))
private[knn] abstract class KnnAlgorithm[TModel <: KnnModelBase[TModel]](override val uid: String)
    extends Estimator[TModel]
    with ModelLogging
    with KnnAlgorithmParams
    with IndexCreator
    with IndexLoader
    with IndexServing
    with DefaultParamsWritable
    with ModelCreator[TModel] {

  import KnnAlgorithm._

  /** @group setParam */
  def setIdentifierCol(value: String): this.type = set(identifierCol, value)

  /** @group setParam */
  def setPartitionCol(value: String): this.type = set(partitionCol, value)

  /** @group setParam */
  def setQueryPartitionsCol(value: String): this.type = set(queryPartitionsCol, value)

  /** @group setParam */
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  def setK(value: Int): this.type = set(k, value)

  /** @group setParam */
  def setNumPartitions(value: Int): this.type = set(numPartitions, value)

  /** @group setParam */
  def setDistanceFunction(value: String): this.type = set(distanceFunction, value)

  /** @group setParam */
  def setNumReplicas(value: Int): this.type = set(numReplicas, value)

  /** @group setParam */
  def setNumThreads(value: Int): this.type = set(numThreads, value)

  /** @group setParam */
  def setInitialModelPath(value: String): this.type = set(initialModelPath, value)

  override def fit(dataset: Dataset[_]): TModel = {

    if (isSet(partitionCol) && dataset.schema(getPartitionCol).dataType != IntegerType) {
      throw new IllegalArgumentException(s"partition column $getPartitionCol is not an integer.")
    }

    val identifierType = dataset.schema(getIdentifierCol).dataType
    val vectorType     = dataset.schema(getFeaturesCol).dataType

    val model = (identifierType, vectorType) match {
      case (IntegerType, ArrayType(FloatType, _)) => typedFit[Int, Array[Float], IntFloatArrayIndexItem, Float](dataset)
      case (IntegerType, ArrayType(DoubleType, _)) =>
        typedFit[Int, Array[Double], IntDoubleArrayIndexItem, Double](dataset)
      case (IntegerType, VectorType)           => typedFit[Int, Vector, IntVectorIndexItem, Double](dataset)
      case (LongType, ArrayType(FloatType, _)) => typedFit[Long, Array[Float], LongFloatArrayIndexItem, Float](dataset)
      case (LongType, ArrayType(DoubleType, _)) =>
        typedFit[Long, Array[Double], LongDoubleArrayIndexItem, Double](dataset)
      case (LongType, VectorType) => typedFit[Long, Vector, LongVectorIndexItem, Double](dataset)
      case (StringType, ArrayType(FloatType, _)) =>
        typedFit[String, Array[Float], StringFloatArrayIndexItem, Float](dataset)
      case (StringType, ArrayType(DoubleType, _)) =>
        typedFit[String, Array[Double], StringDoubleArrayIndexItem, Double](dataset)
      case (StringType, VectorType) => typedFit[String, Vector, StringVectorIndexItem, Double](dataset)
      case _ =>
        throw new IllegalArgumentException(
          "Cannot create index for items with identifier of type " +
            s"${identifierType.simpleString} and vector of type ${vectorType.simpleString}. " +
            "Supported identifiers are string, int, long and string. Supported vectors are array<float>, array<double> and vector "
        )
    }

    copyValues(model)
  }

  override def transformSchema(schema: StructType): StructType =
    validateAndTransformSchema(schema, schema(getIdentifierCol).dataType)

  @SuppressWarnings(Array("MaxParameters"))
  private def typedFit[
      TId: TypeTag: ClassTag,
      TVector: TypeTag: ClassTag,
      TItem <: Item[TId, TVector] with Product: TypeTag: ClassTag,
      TDistance: TypeTag: ClassTag
  ](dataset: Dataset[_])(implicit
      distanceNumeric: Numeric[TDistance],
      distanceFunctionFactory: String => DistanceFunction[TVector, TDistance],
      idSerializer: ObjectSerializer[TId],
      itemSerializer: ObjectSerializer[TItem],
      indexServerFactory: IndexServerFactory[TId, TVector, TItem, TDistance],
      indexClientFactory: IndexClientFactory[TId, TVector, TDistance]
  ): TModel = {

    val sc           = dataset.sparkSession
    val sparkContext = sc.sparkContext

    val partitionedIndexItems = partitionIndexDataset[TItem](dataset)

    // On each partition collect all the items into memory and construct the HNSW indices.
    // Save these indices to the hadoop filesystem

    val initialModelPathOption = Option(initialModelPath).filter(isSet).map(getOrDefault)

    val initialModelMetadataOption = initialModelPathOption.map { path =>
      logInfo(s"Reading initial model index metadata from $path")
      val metadataPath = new Path(path, "metadata").toString
      val metadataStr  = sparkContext.textFile(metadataPath, 1).first()
      read[ModelMetaData](metadataStr)
    }

    initialModelMetadataOption.foreach { metadata =>
      assert(metadata.numPartitions == getNumPartitions, "Number of partitions of initial model does not match")
      assert(metadata.identifierType == typeDescription[TId], "Identifier type of initial model does not match")
      assert(metadata.vectorType == typeDescription[TVector], "Vector type of initial model does not match")
    }

    val serializableConfiguration = new SerializableConfiguration(sparkContext.hadoopConfiguration)

    val taskReqs = new TaskResourceRequests().cpus(getNumThreads)
    val profile  = new ResourceProfileBuilder().require(taskReqs).build()

    val indexRdd = partitionedIndexItems
      .mapPartitions(
        (it: Iterator[TItem]) => {

          val items = it.toSeq

          val partitionId = TaskContext.getPartitionId()

          val existingIndexOption = initialModelPathOption
            .map { path =>
              val indicesDir = new Path(path, "indices")
              val indexPath  = new Path(indicesDir, partitionId.toString)
              val fs         = indexPath.getFileSystem(serializableConfiguration.value)

              logInfo(partitionId, s"Loading existing index from $indexPath")
              val inputStream = fs.open(indexPath)
              loadIndex[TId, TVector, TItem, TDistance](inputStream, items.size)
            }

          logInfo(partitionId, s"started indexing ${items.size} items on host ${InetAddress.getLocalHost.getHostName}")

          val index = existingIndexOption
            .filter(_.nonEmpty)
            .getOrElse(
              items.headOption.fold(emptyIndex[TId, TVector, TItem, TDistance]) { item =>
                createIndex[TId, TVector, TItem, TDistance](
                  item.dimensions,
                  items.size,
                  distanceFunctionFactory(getDistanceFunction)
                )
              }
            )

          val numThreads = TaskContext.get().cpus()

          index.addAll(
            items,
            progressUpdateInterval = 5000,
            listener = (workDone, max) => logDebug(f"partition $partitionId%04d: Indexed $workDone of $max items"),
            numThreads = numThreads
          )

          logInfo(partitionId, s"finished indexing ${items.size} items")

          Iterator(index)
        },
        preservesPartitioning = true
      )
      .withResources(profile)

    val jobGroup = uid + "_" + System.nanoTime()

    val registrations = serve[TId, TVector, TItem, TDistance](jobGroup, indexRdd, getNumReplicas)

    logInfo("All index replicas have successfully registered.")

    registrations.toList
      .sortBy { case (pnr, _) => (pnr.partitionNum, pnr.replicaNum) }
      .foreach { case (pnr, a) =>
        logInfo(pnr.partitionNum, pnr.replicaNum, s"Index registered as ${a.getHostName}:${a.getPort}")
      }

    createModel[TId, TVector, TItem, TDistance](
      jobGroup,
      getNumPartitions,
      getNumReplicas,
      getNumThreads,
      sparkContext,
      registrations,
      indexClientFactory,
      jobGroup
    )
  }

  private def partitionIndexDataset[TItem <: Product: ClassTag: TypeTag](dataset: Dataset[_]): RDD[TItem] = {
    import dataset.sparkSession.implicits._
    // read the id and vector from the input dataset, repartition them over numPartitions amount of partitions.
    // if the data is pre-partitioned by the user repartition the input data by the user defined partition key, use a
    // hash of the item id otherwise.

    if (isDefined(partitionCol))
      dataset
        .select(
          col(getPartitionCol).as("partition"),
          struct(col(getIdentifierCol).as("id"), col(getFeaturesCol).as("vector"))
        )
        .as[(Int, TItem)]
        .rdd
        .partitionBy(new PartitionIdPartitioner(getNumPartitions))
        .values
    else
      dataset
        .select(col(getIdentifierCol).as("id"), col(getFeaturesCol).as("vector"))
        .as[TItem]
        .rdd
        .repartition(getNumPartitions)
  }

}

/** Partitioner that uses precomputed partitions. Each partition id is its own partition
  *
  * @param numPartitions
  *   number of partitions
  */
private[knn] class PartitionIdPartitioner(override val numPartitions: Int) extends Partitioner {
  override def getPartition(key: Any): Int = key match {
    case k: Int => k
    case _      => throw new IllegalArgumentException("Partition key is not an integer.")
  }
}

/** Partitioner that uses precomputed partitions. Each unique partition and replica combination its own partition
  *
  * @param partitions
  *   the total number of partitions
  * @param replicas
  *   the total number of replicas
  */
private[knn] class PartitionReplicaIdPartitioner(partitions: Int, replicas: Int) extends Partitioner {

  override def numPartitions: Int = partitions + (replicas * partitions)

  override def getPartition(key: Any): Int = key match {
    case (partition: Int, replica: Int) => partition + (replica * partitions)
    case _ => throw new IllegalArgumentException("Partition key is not z pair of integers.")
  }
}

private[knn] trait ModelLogging extends Logging {
  protected def logInfo(partition: Int, message: String): Unit = logInfo(f"partition $partition%04d: $message")

  protected def logInfo(partition: Int, replica: Int, message: String): Unit = logInfo(
    f"partition $replica%04d replica $partition%04d: $message"
  )
}

private[knn] trait IndexServing extends ModelLogging with IndexType {

  /** Replicates indices and serves them by starting index servers on each partition and registering them with a central
    * registration server.
    *
    * @tparam TId
    *   type of the index item identifier
    * @tparam TVector
    *   type of the index item vector
    * @tparam TItem
    *   type of the index item
    * @tparam TDistance
    *   type of distance between items
    *
    * @param jobGroup
    *   Job group of the job that holds on the indices being served
    * @param indexRdd
    *   The indices
    * @param numReplicas
    *   The number of replicas to create for each partition of the index.
    * @param indexServerFactory
    *   A factory for creating index servers, provided implicitly.
    * @return
    *   A map of partition and replica identifiers to their corresponding server addresses.
    */
  protected def serve[
      TId: ClassTag,
      TVector: ClassTag,
      TItem <: Item[TId, TVector] with Product: ClassTag,
      TDistance: ClassTag
  ](
      jobGroup: String,
      indexRdd: RDD[TIndex[TId, TVector, TItem, TDistance]],
      numReplicas: Int
  )(implicit
      indexServerFactory: IndexServerFactory[TId, TVector, TItem, TDistance]
  ): Map[PartitionAndReplica, InetSocketAddress] = {

    val numPartitions = indexRdd.partitions.length

    val sparkContext              = indexRdd.sparkContext
    val serializableConfiguration = new SerializableConfiguration(sparkContext.hadoopConfiguration)

    val keyedIndexRdd = indexRdd.flatMap { index =>
      Range.inclusive(0, numReplicas).map { replica => (TaskContext.getPartitionId(), replica) -> index }
    }

    val replicaRdd =
      if (numReplicas > 0) keyedIndexRdd.partitionBy(new PartitionReplicaIdPartitioner(numPartitions, numReplicas))
      else keyedIndexRdd

    val driverHost    = sparkContext.getConf.get("spark.driver.host")
    val server        = RegistrationServerFactory.create(driverHost, numPartitions, numReplicas)
    val serverAddress = server.start()
    try {
      val driverPort = serverAddress.getPort

      logInfo(s"Started registration server on $driverHost:$driverPort")

      val serverRdd = replicaRdd
        .map { case ((partitionNum, replicaNum), index) =>
          // serializing an InetSocketAddress does not always seem to give the correct hostname in docker
          val registrationAddress = new InetSocketAddress(driverHost, driverPort)
          val executorHost        = SparkEnv.get.blockManager.blockManagerId.host
          val numThreads          = TaskContext.get().cpus()
          val server = indexServerFactory.create(executorHost, index, serializableConfiguration.value, numThreads)

          val serverAddress = server.start()

          logInfo(
            partitionNum,
            replicaNum,
            s"started index server on host ${serverAddress.getHostName}:${serverAddress.getPort}"
          )

          logInfo(
            partitionNum,
            replicaNum,
            s"registering replica at ${registrationAddress.getHostName}:${registrationAddress.getPort}"
          )
          try {
            RegistrationClient.register(registrationAddress, partitionNum, replicaNum, serverAddress)

            logInfo(partitionNum, replicaNum, "awaiting requests")

            while (!TaskContext.get().isInterrupted() && !server.isTerminated) {
              Thread.sleep(500)
            }

            logInfo(
              partitionNum,
              replicaNum,
              "Task canceled"
            )
          } finally {
            server.shutdown()
          }

          true
        }
        .withResources(indexRdd.getResourceProfile())

      implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.global

      val indexFuture = Future {
        sparkContext.setJobGroup(jobGroup, "job group that holds the indices")
        try {
          serverRdd.count(): Unit
        } finally {
          sparkContext.clearJobGroup()
        }
      }

      val registrationFuture = Future {
        server.awaitRegistrations()
      }

      // await all indices registering themselves or an error.
      Await.ready(Future.firstCompletedOf(Seq(indexFuture, registrationFuture)), Duration.Inf)

      val maybeException = indexFuture.value.collect { case Failure(e) => e }

      maybeException.foreach { throw _ }

      Await.result(registrationFuture, Duration.Inf)

    } finally {
      server.shutdown()
    }

  }
}
