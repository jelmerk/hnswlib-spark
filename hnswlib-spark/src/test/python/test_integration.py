from pathlib import Path
from pyspark_hnsw.knn import HnswSimilarity
from pyspark.ml.linalg import Vectors
from pyspark.sql import functions as F
from pyspark.sql import SparkSession


def test_incremental_models(spark: SparkSession, tmp_path: Path) -> None:

    df1 = spark.createDataFrame([
        [1, Vectors.dense([0.1, 0.2, 0.3])]
    ], ['id', 'features'])

    hnsw1 = HnswSimilarity(numPartitions=2, numThreads=1)

    model1 = hnsw1.fit(df1)
    try:
        model1.write().overwrite().save(tmp_path.as_posix())
    finally:
        model1.dispose()

    df2 = spark.createDataFrame([
        [2, Vectors.dense([0.9, 0.1, 0.2])]
    ], ['id', 'features'])

    hnsw2 = HnswSimilarity(numPartitions=2, numThreads=1, initialModelPath=tmp_path.as_posix())
    model2 = hnsw2.fit(df2)
    try:
        assert model2.transform(df1).select(F.explode("prediction")).count() == 2
    finally:
        model2.dispose()

