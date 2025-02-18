from pathlib import Path
from pyspark_hnsw.knn import HnswSimilarityModel, HnswSimilarity
from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession


def test_hnsw_index_and_query(spark: SparkSession) -> None:

    df = spark.createDataFrame([
        [1, Vectors.dense([0.2, 0.9])],
        [2, Vectors.dense([0.2, 1.0])],
        [3, Vectors.dense([0.2, 0.1])],
    ], ['row_id', 'features'])

    hnsw = HnswSimilarity(identifierCol='row_id', featuresCol='features', distanceFunction='cosine', m=32, ef=5, k=5,
                          efConstruction=200, numPartitions=2, numThreads=1)

    model = hnsw.fit(df)

    try:
        result = model.transform(df)

        assert result.count() == 3
    finally:
        model.dispose()

def test_hnsw_save_and_load( tmp_path: Path) -> None:

    hnsw = HnswSimilarity(identifierCol='row_id', featuresCol='features', distanceFunction='cosine', m=32, ef=5, k=5,
                          efConstruction=200, numPartitions=2, numThreads=1)

    hnsw.write().overwrite().save(tmp_path.as_posix())
    HnswSimilarity.read().load(tmp_path.as_posix())

def test_hnsw_save_and_load_model(spark: SparkSession, tmp_path: Path) -> None:

    df = spark.createDataFrame([
        [1, Vectors.dense([0.2, 0.9])],
        [2, Vectors.dense([0.2, 1.0])],
        [3, Vectors.dense([0.2, 0.1])],
    ], ['row_id', 'features'])

    hnsw = HnswSimilarity(identifierCol='row_id', featuresCol='features', distanceFunction='cosine', m=32, ef=5, k=5,
                          efConstruction=200, numPartitions=2, numThreads=1)

    model = hnsw.fit(df)

    try:
        model.write().overwrite().save(tmp_path.as_posix())
    finally:
        model.dispose()

    HnswSimilarityModel.read().load(tmp_path.as_posix()).dispose()
