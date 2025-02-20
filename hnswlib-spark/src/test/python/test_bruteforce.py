from pathlib import Path
from pyspark_hnsw.knn import BruteForceSimilarityModel, BruteForceSimilarity
from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession


def test_bruteforce_index_and_query(spark: SparkSession) -> None:

    df = spark.createDataFrame([
        [1, Vectors.dense([0.2, 0.9])],
        [2, Vectors.dense([0.2, 1.0])],
        [3, Vectors.dense([0.2, 0.1])],
    ], ['row_id', 'features'])

    bruteforce = BruteForceSimilarity(
        identifierCol='row_id',
        featuresCol='features',
        distanceFunction='cosine',
        numPartitions=1,
        numThreads=1
    )

    model = bruteforce.fit(df)

    try:
        result = model.transform(df)

        assert result.count() == 3
    finally:
        model.dispose()

def test_bruteforce_save_and_load( tmp_path: Path) -> None:

    bruteforce = BruteForceSimilarity(
        identifierCol='row_id',
        featuresCol='features',
        distanceFunction='cosine',
        numPartitions=1,
        numThreads=1
    )

    bruteforce.write().overwrite().save(tmp_path.as_posix())
    BruteForceSimilarity.read().load(tmp_path.as_posix())

# def test_bruteforce_save_and_load_model(spark: SparkSession, tmp_path: Path) -> None:
#
#     df = spark.createDataFrame([
#         [1, Vectors.dense([0.2, 0.9])],
#         [2, Vectors.dense([0.2, 1.0])],
#         [3, Vectors.dense([0.2, 0.1])],
#     ], ['row_id', 'features'])
#
#     bruteforce = BruteForceSimilarity(
#         identifierCol='row_id',
#         featuresCol='features',
#         distanceFunction='cosine',
#         numPartitions=1,
#         numThreads=1
#     )
#
#     model = bruteforce.fit(df)
#
#     try:
#         model.write().overwrite().save(tmp_path.as_posix())
#     finally:
#         model.dispose()
#
#     BruteForceSimilarityModel.read().load(tmp_path.as_posix()).dispose()