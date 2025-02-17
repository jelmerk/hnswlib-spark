from typing import Any, Dict

from pyspark.ml.wrapper import JavaEstimator, JavaModel
from pyspark.ml.param.shared import (
    Params,
    Param,
    HasFeaturesCol,
    HasPredictionCol,
    TypeConverters,
)
from pyspark.mllib.common import inherit_doc

# noinspection PyProtectedMember
from pyspark import keyword_only

# noinspection PyProtectedMember
from pyspark.ml.util import JavaMLReadable, JavaMLWritable


__all__ = [
    "HnswSimilarity",
    "HnswSimilarityModel",
    "BruteForceSimilarity",
    "BruteForceSimilarityModel",
]


class KnnModel(JavaModel):
    def dispose(self) -> None:
        assert self._java_obj is not None
        return self._java_obj.dispose()


# noinspection PyPep8Naming
@inherit_doc
class _KnnModelParams(HasFeaturesCol, HasPredictionCol):
    """
    Params for knn models.
    """

    queryPartitionsCol: Param[str] = Param(
        Params._dummy(),
        "queryPartitionsCol",
        "the column name for the query partitions",
        typeConverter=TypeConverters.toString,
    )

    k: Param[int] = Param(
        Params._dummy(),
        "k",
        "number of neighbors to find",
        typeConverter=TypeConverters.toInt,
    )

    def getQueryPartitionsCol(self) -> str:
        """
        Gets the value of queryPartitionsCol or its default value.
        """
        return self.getOrDefault(self.queryPartitionsCol)

    def getK(self) -> int:
        """
        Gets the value of k or its default value.
        """
        return self.getOrDefault(self.k)


# noinspection PyPep8Naming
@inherit_doc
class _KnnParams(_KnnModelParams):
    """
    Params for knn algorithms.
    """

    numThreads: Param[int] = Param(
        Params._dummy(),
        "numThreads",
        "number of threads to use",
        typeConverter=TypeConverters.toInt,
    )

    numReplicas: Param[int] = Param(
        Params._dummy(),
        "numReplicas",
        "number of index replicas to create when querying",
        typeConverter=TypeConverters.toInt,
    )

    identifierCol: Param[str] = Param(
        Params._dummy(),
        "identifierCol",
        "the column name for the row identifier",
        typeConverter=TypeConverters.toString,
    )

    partitionCol: Param[str] = Param(
        Params._dummy(),
        "partitionCol",
        "the column name for the partition",
        typeConverter=TypeConverters.toString,
    )

    initialModelPath: Param[str] = Param(
        Params._dummy(),
        "initialModelPath",
        "path to the initial model",
        typeConverter=TypeConverters.toString,
    )

    numPartitions: Param[int] = Param(
        Params._dummy(),
        "numPartitions",
        "number of partitions",
        typeConverter=TypeConverters.toInt,
    )

    distanceFunction: Param[str] = Param(
        Params._dummy(),
        "distanceFunction",
        "distance function, one of bray-curtis, canberra, cosine, correlation, "
        + "euclidean, inner-product, manhattan or the fully qualified classname "
        + "of a distance function",
        typeConverter=TypeConverters.toString,
    )

    def getNumThreads(self) -> int:
        """
        Gets the value of numThreads.
        """
        return self.getOrDefault(self.numThreads)

    def getNumReplicas(self) -> int:
        """
        Gets the value of numReplicas or its default value.
        """
        return self.getOrDefault(self.numReplicas)

    def getIdentifierCol(self) -> str:
        """
        Gets the value of identifierCol or its default value.
        """
        return self.getOrDefault(self.identifierCol)

    def getPartitionCol(self) -> str:
        """
        Gets the value of partitionCol or its default value.
        """
        return self.getOrDefault(self.partitionCol)

    def getInitialModelPath(self) -> str:
        """
        Gets the value of initialModelPath or its default value.
        """
        return self.getOrDefault(self.initialModelPath)

    def getNumPartitions(self) -> int:
        """
        Gets the value of numPartitions or its default value.
        """
        return self.getOrDefault(self.numPartitions)

    def getDistanceFunction(self) -> str:
        """
        Gets the value of distanceFunction or its default value.
        """
        return self.getOrDefault(self.distanceFunction)


# noinspection PyPep8Naming
@inherit_doc
class _HnswModelParams(_KnnModelParams):
    """
    Params for :py:class:`Hnsw` and :py:class:`HnswModel`.
    """


# noinspection PyPep8Naming
@inherit_doc
class _HnswParams(_HnswModelParams, _KnnParams):
    """
    Params for :py:class:`Hnsw`.
    """

    ef: Param[int] = Param(
        Params._dummy(),
        "ef",
        "size of the dynamic list for the nearest neighbors (used during the search)",
        typeConverter=TypeConverters.toInt,
    )

    m: Param[int] = Param(
        Params._dummy(),
        "m",
        "number of bi-directional links created for every new element during construction",
        typeConverter=TypeConverters.toInt,
    )

    efConstruction: Param[int] = Param(
        Params._dummy(),
        "efConstruction",
        "has the same meaning as ef, but controls the index time / index precision",
        typeConverter=TypeConverters.toInt,
    )

    def getEf(self) -> int:
        """
        Gets the value of ef or its default value.
        """
        return self.getOrDefault(self.ef)

    def getM(self) -> int:
        """
        Gets the value of m or its default value.
        """
        return self.getOrDefault(self.m)

    def getEfConstruction(self) -> int:
        """
        Gets the value of efConstruction or its default value.
        """
        return self.getOrDefault(self.efConstruction)


# noinspection PyPep8Naming
class BruteForceSimilarityModel(
    KnnModel,
    _KnnModelParams,
    JavaMLReadable["BruteForceSimilarityModel"],
    JavaMLWritable,
):
    """
    Model fitted by BruteForce.
    """

    _classpath_model = (
        "com.github.jelmerk.spark.knn.bruteforce.BruteForceSimilarityModel"
    )

    def setQueryPartitionsCol(self, value: str) -> "BruteForceSimilarityModel":
        """
        Sets the value of :py:attr:`queryPartitionsCol`.
        """
        return self._set(queryPartitionsCol=value)

    def setK(self, value: int) -> "BruteForceSimilarityModel":
        """
        Sets the value of :py:attr:`k`.
        """
        return self._set(k=value)

    def setPredictionCol(self, value: str) -> "BruteForceSimilarityModel":
        """
        Sets the value of :py:attr:`predictionCol`.
        """
        return self._set(predictionCol=value)

    def setFeaturesCol(self, value: str) -> "BruteForceSimilarityModel":
        """
        Sets the value of :py:attr:`featuresCol`.
        """
        return self._set(featuresCol=value)


# noinspection PyPep8Naming
@inherit_doc
class BruteForceSimilarity(
    JavaEstimator[BruteForceSimilarityModel],
    _KnnParams,
    JavaMLReadable["BruteForceSimilarity"],
    JavaMLWritable,
):
    """
    Exact nearest neighbour search.
    """

    _input_kwargs: Dict[str, Any]

    # noinspection PyUnusedLocal
    @keyword_only
    def __init__(
        self,
        identifierCol="id",
        partitionCol=None,
        queryPartitionsCol=None,
        numThreads=None,
        featuresCol="features",
        predictionCol="prediction",
        numPartitions=None,
        numReplicas=0,
        k=5,
        distanceFunction="cosine",
        initialModelPath=None,
    ):
        super(BruteForceSimilarity, self).__init__()
        self._java_obj = self._new_java_obj(
            "com.github.jelmerk.spark.knn.bruteforce.BruteForceSimilarity", self.uid
        )

        self._setDefault(
            identifierCol="id",
            numPartitions=1,
            numReplicas=0,
            k=5,
            distanceFunction="cosine",
        )

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def setIdentifierCol(self, value: str) -> "BruteForceSimilarity":
        """
        Sets the value of :py:attr:`identifierCol`.
        """
        return self._set(identifierCol=value)

    def setPartitionCol(self, value: str) -> "BruteForceSimilarity":
        """
        Sets the value of :py:attr:`partitionCol`.
        """
        return self._set(partitionCol=value)

    def setQueryPartitionsCol(self, value: str) -> "BruteForceSimilarity":
        """
        Sets the value of :py:attr:`queryPartitionsCol`.
        """
        return self._set(queryPartitionsCol=value)

    def setNumThreads(self, value: int) -> "BruteForceSimilarity":
        """
        Sets the value of :py:attr:`numThreads`.
        """
        return self._set(numThreads=value)

    def setNumPartitions(self, value: int) -> "BruteForceSimilarity":
        """
        Sets the value of :py:attr:`numPartitions`.
        """
        return self._set(numPartitions=value)

    def setNumReplicas(self, value: int) -> "BruteForceSimilarity":
        """
        Sets the value of :py:attr:`numReplicas`.
        """
        return self._set(numReplicas=value)

    def setK(self, value: int) -> "BruteForceSimilarity":
        """
        Sets the value of :py:attr:`k`.
        """
        return self._set(k=value)

    def setDistanceFunction(self, value: str) -> "BruteForceSimilarity":
        """
        Sets the value of :py:attr:`distanceFunction`.
        """
        return self._set(distanceFunction=value)

    def setInitialModelPath(self, value: str) -> "BruteForceSimilarity":
        """
        Sets the value of :py:attr:`initialModelPath`.
        """
        return self._set(initialModelPath=value)

    def setPredictionCol(self, value: str) -> "BruteForceSimilarity":
        """
        Sets the value of :py:attr:`predictionCol`.
        """
        return self._set(predictionCol=value)

    def setFeaturesCol(self, value: str) -> "BruteForceSimilarity":
        """
        Sets the value of :py:attr:`featuresCol`.
        """
        return self._set(featuresCol=value)

    # noinspection PyUnusedLocal
    @keyword_only
    def setParams(
        self,
        identifierCol="id",
        queryPartitionsCol=None,
        numThreads=None,
        featuresCol="features",
        predictionCol="prediction",
        numPartitions=1,
        numReplicas=0,
        k=5,
        distanceFunction="cosine",
        initialModelPath=None,
    ) -> "BruteForceSimilarity":
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _create_model(self, java_model) -> BruteForceSimilarityModel:
        return BruteForceSimilarityModel(java_model)


# noinspection PyPep8Naming
class HnswSimilarityModel(
    KnnModel, _HnswModelParams, JavaMLReadable["HnswSimilarityModel"], JavaMLWritable
):
    """
    Model fitted by Hnsw.
    """

    _classpath_model = "com.github.jelmerk.spark.knn.hnsw.HnswSimilarityModel"

    def setQueryPartitionsCol(self, value: str) -> "HnswSimilarityModel":
        """
        Sets the value of :py:attr:`queryPartitionsCol`.
        """
        return self._set(queryPartitionsCol=value)

    def setK(self, value: int) -> "HnswSimilarityModel":
        """
        Sets the value of :py:attr:`k`.
        """
        return self._set(k=value)

    def setPredictionCol(self, value: str) -> "HnswSimilarityModel":
        """
        Sets the value of :py:attr:`predictionCol`.
        """
        return self._set(predictionCol=value)

    def setFeaturesCol(self, value: str) -> "HnswSimilarityModel":
        """
        Sets the value of :py:attr:`featuresCol`.
        """
        return self._set(featuresCol=value)


# noinspection PyPep8Naming
@inherit_doc
class HnswSimilarity(
    JavaEstimator[HnswSimilarityModel],
    _HnswParams,
    JavaMLReadable["HnswSimilarity"],
    JavaMLWritable,
):
    """
    Approximate nearest neighbour search.
    """

    # noinspection PyUnusedLocal
    @keyword_only
    def __init__(
        self,
        identifierCol="id",
        partitionCol=None,
        queryPartitionsCol=None,
        numThreads=None,
        featuresCol="features",
        predictionCol="prediction",
        m=16,
        ef=10,
        efConstruction=200,
        numPartitions=1,
        numReplicas=0,
        k=5,
        distanceFunction="cosine",
        initialModelPath=None,
    ):
        super(HnswSimilarity, self).__init__()
        self._java_obj = self._new_java_obj(
            "com.github.jelmerk.spark.knn.hnsw.HnswSimilarity", self.uid
        )

        self._setDefault(
            identifierCol="id",
            m=16,
            ef=10,
            efConstruction=200,
            numPartitions=1,
            numReplicas=0,
            k=5,
            distanceFunction="cosine",
            initialModelPath=None,
        )

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def setIdentifierCol(self, value: str) -> "HnswSimilarity":
        """
        Sets the value of :py:attr:`identifierCol`.
        """
        return self._set(identifierCol=value)

    def setPartitionCol(self, value: str) -> "HnswSimilarity":
        """
        Sets the value of :py:attr:`partitionCol`.
        """
        return self._set(partitionCol=value)

    def setQueryPartitionsCol(self, value: str) -> "HnswSimilarity":
        """
        Sets the value of :py:attr:`queryPartitionsCol`.
        """
        return self._set(queryPartitionsCol=value)

    def setNumThreads(self, value: int) -> "HnswSimilarity":
        """
        Sets the value of :py:attr:`numThreads`.
        """
        return self._set(numThreads=value)

    def setNumPartitions(self, value: int) -> "HnswSimilarity":
        """
        Sets the value of :py:attr:`numPartitions`.
        """
        return self._set(numPartitions=value)

    def setNumReplicas(self, value: int) -> "HnswSimilarity":
        """
        Sets the value of :py:attr:`numReplicas`.
        """
        return self._set(numReplicas=value)

    def setK(self, value: int) -> "HnswSimilarity":
        """
        Sets the value of :py:attr:`k`.
        """
        return self._set(k=value)

    def setDistanceFunction(self, value: str) -> "HnswSimilarity":
        """
        Sets the value of :py:attr:`distanceFunction`.
        """
        return self._set(distanceFunction=value)

    def setM(self, value: int) -> "HnswSimilarity":
        """
        Sets the value of :py:attr:`m`.
        """
        return self._set(m=value)

    def setEf(self, value: int) -> "HnswSimilarity":
        """
        Sets the value of :py:attr:`ef`.
        """
        return self._set(ef=value)

    def setEfConstruction(self, value: int) -> "HnswSimilarity":
        """
        Sets the value of :py:attr:`efConstruction`.
        """
        return self._set(efConstruction=value)

    def setInitialModelPath(self, value: str) -> "HnswSimilarity":
        """
        Sets the value of :py:attr:`initialModelPath`.
        """
        return self._set(initialModelPath=value)

    def setPredictionCol(self, value: str) -> "HnswSimilarity":
        """
        Sets the value of :py:attr:`predictionCol`.
        """
        return self._set(predictionCol=value)

    def setFeaturesCol(self, value: str) -> "HnswSimilarity":
        """
        Sets the value of :py:attr:`featuresCol`.
        """
        return self._set(featuresCol=value)

    # noinspection PyUnusedLocal
    @keyword_only
    def setParams(
        self,
        identifierCol="id",
        queryPartitionsCol=None,
        numThreads=None,
        featuresCol="features",
        predictionCol="prediction",
        m=16,
        ef=10,
        efConstruction=200,
        numPartitions=None,
        numReplicas=0,
        k=5,
        distanceFunction="cosine",
        initialModelPath=None,
    ) -> "HnswSimilarity":
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _create_model(self, java_model) -> HnswSimilarityModel:
        return HnswSimilarityModel(java_model)


HnswSimilarityModelImpl = HnswSimilarityModel
BruteForceSimilarityModelImpl = BruteForceSimilarityModel
