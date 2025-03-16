import pytest

from pathlib import Path
from pyspark_hnsw.knn import BruteForceSimilarityModel, BruteForceSimilarity
from pyspark.ml.linalg import Vectors
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

@pytest.fixture
def bruteforce() -> BruteForceSimilarity:
    return BruteForceSimilarity(
        identifierCol='row_id',
        featuresCol='features',
        distanceFunction='cosine',
        numPartitions=1,
        numThreads=1,
        predictionCol="prediction",
    )

def test_bruteforce_index_and_query(spark: SparkSession, bruteforce: BruteForceSimilarity) -> None:

    items = spark.createDataFrame([
        [1, Vectors.dense([0.2, 0.9])],
        [2, Vectors.dense([0.2, 1.0])],
        [3, Vectors.dense([0.2, 0.1])],
    ], ['row_id', 'features'])

    model = bruteforce.fit(items)

    try:
        result = model.transform(items)

        assert result.count() == 3
    finally:
        model.dispose()

def test_hnsw_partition_summaries(spark: SparkSession, bruteforce: BruteForceSimilarity) -> None:
    items = spark.createDataFrame([
        [1, Vectors.dense([0.2, 0.9])],
        [2, Vectors.dense([0.2, 1.0])],
        [3, Vectors.dense([0.2, 0.1])],
    ], ['row_id', 'features'])

    model = bruteforce.fit(items)

    try:
        df = model.partitionSummaries()

        assert df.count() == 1
    finally:
        model.dispose()

def test_bruteforce_save_and_load( tmp_path: Path, bruteforce: BruteForceSimilarity) -> None:
    bruteforce.write().overwrite().save(tmp_path.as_posix())
    BruteForceSimilarity.read().load(tmp_path.as_posix())

def test_bruteforce_save_and_load_model(spark: SparkSession, bruteforce: BruteForceSimilarity, tmp_path: Path) -> None:

    items = spark.createDataFrame([
        [1, Vectors.dense([0.2, 0.9])],
        [2, Vectors.dense([0.2, 1.0])],
        [3, Vectors.dense([0.2, 0.1])],
    ], ['row_id', 'features'])

    model = bruteforce.fit(items)

    try:
        model.write().overwrite().save(tmp_path.as_posix())
    finally:
        model.dispose()

    BruteForceSimilarityModel.read().load(tmp_path.as_posix()).dispose()

def test_bruteforce_append_to_existing_model(spark: SparkSession, bruteforce: BruteForceSimilarity, tmp_path: Path) -> None:

    items = spark.createDataFrame([
        [1, Vectors.dense([0.2, 0.9])],
        [2, Vectors.dense([0.2, 1.0])],
    ], ['row_id', 'features'])

    model = bruteforce.fit(items)

    try:
        model.write().overwrite().save(tmp_path.as_posix())
    finally:
        model.dispose()

    new_items = spark.createDataFrame([
        [100, Vectors.dense([0.2, 0.1])],
    ], ['row_id', 'features'])

    new_bruteforce = bruteforce.copy().setInitialModelPath(tmp_path.as_posix())

    new_model = new_bruteforce.fit(new_items)
    try:
        query = spark.createDataFrame([
            [100, Vectors.dense([0.2, 0.1])],
        ], ['row_id', 'features'])

        result = new_model.transform(query).select(F.explode(F.col("prediction").alias("result")))
        assert result.count() == 3
    finally:
        new_model.dispose()
