from pyspark.ml.param.shared import HasInputCol, HasOutputCol
from pyspark.ml.wrapper import JavaTransformer
from pyspark.ml.util import JavaMLReadable, JavaMLWritable
from pyspark.mllib.common import inherit_doc

# noinspection PyProtectedMember
from pyspark import keyword_only

__all__ = ["Normalizer"]


# noinspection PyPep8Naming
@inherit_doc
class Normalizer(
    JavaTransformer,
    HasInputCol,
    HasOutputCol,
    JavaMLReadable["Normalizer"],
    JavaMLWritable,
):
    """
    Normalizes vectors to unit norm
    """

    @keyword_only
    def __init__(self, inputCol="input", outputCol="output"):
        """
        __init__(self, inputCol="input", outputCol="output")
        """
        super(Normalizer, self).__init__()
        self._java_obj = self._new_java_obj(
            "com.github.jelmerk.spark.linalg.Normalizer", self.uid
        )
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def setInputCol(self, value: str) -> "Normalizer":
        """
        Sets the value of :py:attr:`inputCol`.
        """
        return self._set(inputCol=value)

    def setOutputCol(self, value: str) -> "Normalizer":
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)

    @keyword_only
    def setParams(self, inputCol="input", outputCol="output") -> "Normalizer":
        """
        setParams(self, inputCol="input", outputCol="output")
        Sets params for this Normalizer.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)
