# noinspection PyProtectedMember
from pyspark.ml.evaluation import JavaEvaluator
from pyspark.ml.param.shared import Params, Param, TypeConverters
from pyspark.mllib.common import inherit_doc

# noinspection PyProtectedMember
from pyspark import keyword_only
from pyspark.ml.util import JavaMLReadable, JavaMLWritable

__all__ = ["KnnSimilarityEvaluator"]


# noinspection PyPep8Naming
@inherit_doc
class KnnSimilarityEvaluator(
    JavaEvaluator, JavaMLReadable["KnnSimilarityEvaluator"], JavaMLWritable
):
    """
    Evaluate the performance of a knn model.
    """

    approximateNeighborsCol: Param[str] = Param(
        Params._dummy(),
        "approximateNeighborsCol",
        "prediction column for the approximate results",
        typeConverter=TypeConverters.toString,
    )

    exactNeighborsCol: Param[str] = Param(
        Params._dummy(),
        "exactNeighborsCol",
        "prediction column for the exact results",
        typeConverter=TypeConverters.toString,
    )

    @keyword_only
    def __init__(
        self,
        approximateNeighborsCol="approximateNeighbors",
        exactNeighborsCol="exactNeighbors",
    ):
        super(JavaEvaluator, self).__init__()
        self._java_obj = self._new_java_obj(
            "com.github.jelmerk.spark.knn.evaluation.KnnSimilarityEvaluator", self.uid
        )

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def getApproximateNeighborsCol(self) -> str:
        """
        Gets the value of approximateNeighborsCol or its default value.
        """
        return self.getOrDefault(self.approximateNeighborsCol)

    def setApproximateNeighborsCol(self, value: str) -> "KnnSimilarityEvaluator":
        """
        Sets the value of :py:attr:`approximateNeighborsCol`.
        """
        return self._set(approximateNeighborsCol=value)

    def getExactNeighborsCol(self) -> str:
        """
        Gets the value of exactNeighborsCol or its default value.
        """
        return self.getOrDefault(self.exactNeighborsCol)

    def setExactNeighborsCol(self, value: str) -> "KnnSimilarityEvaluator":
        """
        Sets the value of :py:attr:`exactNeighborsCol`.
        """
        return self._set(exactNeighborsCol=value)

    @keyword_only
    def setParams(
        self,
        approximateNeighborsCol="approximateNeighbors",
        exactNeighborsCol="exactNeighbors",
    ) -> "KnnSimilarityEvaluator":
        kwargs = self._input_kwargs
        return self._set(**kwargs)
