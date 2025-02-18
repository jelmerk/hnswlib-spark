from pyspark_hnsw.conversion import VectorConverter
from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession


def test_vector_converter(spark: SparkSession) -> None:

    df = spark.createDataFrame([[Vectors.dense([0.01, 0.02, 0.03])]], ['vector'])

    converter = VectorConverter(inputCol="vector", outputCol="array", outputType="array<double>")

    result = converter.transform(df)

    assert result.count() == 1
