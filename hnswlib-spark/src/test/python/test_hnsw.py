import pytest

from pathlib import Path
from pyspark_hnsw.knn import HnswSimilarityModel, HnswSimilarity
from pyspark.ml.linalg import Vectors
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

@pytest.fixture
def hnsw() -> HnswSimilarity:
    return HnswSimilarity(
        identifierCol='row_id',
        featuresCol='features',
        distanceFunction='cosine',
        m=32,
        ef=5,
        k=5,
        efConstruction=200,
        numPartitions=1,
        numThreads=1
    )

def test_hnsw_index_and_query(spark: SparkSession, hnsw: HnswSimilarity) -> None:

    items = spark.createDataFrame([
        [1, Vectors.dense([0.2, 0.9])],
        [2, Vectors.dense([0.2, 1.0])],
        [3, Vectors.dense([0.2, 0.1])],
    ], ['row_id', 'features'])

    model = hnsw.fit(items)

    try:
        result = model.transform(items)

        assert result.count() == 3
    finally:
        model.dispose()

def test_hnsw_save_and_load(hnsw: HnswSimilarity, tmp_path: Path) -> None:

    hnsw.write().overwrite().save(tmp_path.as_posix())
    HnswSimilarity.read().load(tmp_path.as_posix())

def test_hnsw_save_and_load_model(spark: SparkSession, hnsw: HnswSimilarity, tmp_path: Path) -> None:

    items = spark.createDataFrame([
        [1, Vectors.dense([0.2, 0.9])],
        [2, Vectors.dense([0.2, 1.0])],
        [3, Vectors.dense([0.2, 0.1])],
    ], ['row_id', 'features'])

    model = hnsw.fit(items)

    try:
        model.write().overwrite().save(tmp_path.as_posix())
    finally:
        model.dispose()

    HnswSimilarityModel.read().load(tmp_path.as_posix()).dispose()

def test_hnsw_append_to_existing_model(spark: SparkSession, hnsw: HnswSimilarity, tmp_path: Path) -> None:
    items = spark.createDataFrame([
        [1, Vectors.dense([0.2, 0.9])],
        [2, Vectors.dense([0.2, 1.0])],
    ], ['row_id', 'features'])

    model = hnsw.fit(items)

    try:
        model.write().overwrite().save(tmp_path.as_posix())
    finally:
        model.dispose()

    new_items = spark.createDataFrame([
        [100, Vectors.dense([0.2, 0.1])],
    ], ['row_id', 'features'])

    new_bruteforce = hnsw.copy().setInitialModelPath(tmp_path.as_posix())

    new_model = new_bruteforce.fit(new_items)
    try:
        query = spark.createDataFrame([
            [100, Vectors.dense([0.2, 0.1])],
        ], ['row_id', 'features'])

        result = new_model.transform(query).select(F.explode(F.col("prediction").alias("result")))
        assert result.count() == 3
    finally:
        new_model.dispose()
