---
title: Get Started
nav_order: 4
---

# Get started with Hnswlib spark

This is a quick start tutorial showing snippets for you to quickly try out Hnswlib spark.


## Simple example

{% tabs example_1 %}

{% tab example_1 python %}
```python
from pyspark_hnsw.knn import HnswSimilarity
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.jars.packages", "com.github.jelmerk:hnswlib-spark_3_5_2.12:{{ site.version_jvm }}") \
    .config("spark.ui.showConsoleProgress", "false") \
    .getOrCreate()

items = spark.table('items')

hnsw = HnswSimilarity(
    identifierCol='id',
    featuresCol='features',
    numPartitions=1,
    numThreads=4,
    m=48,
    ef=5,
    efConstruction=200,
    k=10,
    distanceFunction='cosine'
)

model = hnsw.fit(items)

model.transform(items).write.saveAsTable("results")

model.dispose()
```
{% endtab %}

{% tab example_1 scala %}
```scala
import com.github.jelmerk.spark.knn.hnsw.HnswSimilarity
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder
  .config("spark.jars.packages", "com.github.jelmerk:hnswlib-spark_3_5_2.12:{{ site.version_jvm }}")
  .config("spark.ui.showConsoleProgress", "false")
  .getOrCreate()

val items = spark.table("items")

val hnsw = new HnswSimilarity()
  .setIdentifierCol("id")
  .setFeaturesCol("features")
  .setNumPartitions(1)
  .setNumThreads(4)
  .setM(48)
  .setEf(5)
  .setEfConstruction(200)
  .setK(10)
  .setDistanceFunction("cosine")

val model = hnsw.fit(items)

model.transform(items).write.saveAsTable("results")

model.dispose()
```
{% endtab %}

{% endtabs %}


## More complex example

{% tabs example_2 %}

{% tab example_2 python %}
```python
from pyspark.ml import Pipeline

from pyspark_hnsw.conversion import VectorConverter
from pyspark_hnsw.knn import BruteForceSimilarity, HnswSimilarity, KnnSimilarityEvaluator, Normalizer
from pyspark_hnsw.linalg import Normalizer

spark = SparkSession.builder \
    .config("spark.jars.packages", "com.github.jelmerk:hnswlib-spark_3_5_2.12:{{ site.version_jvm }}") \
    .config("spark.ui.showConsoleProgress", "false") \
    .getOrCreate()

# often it is acceptable to use float instead of double precision.
# this uses less memory and will be faster

converter = VectorConverter(inputCol="featuresAsMlLibVector", outputCol="features", outputType="array<float>")

# The cosine distance is obtained with the inner product after normalizing all vectors to unit norm
# this is much faster than calculating the cosine distance directly

normalizer = Normalizer(inputCol="vector", outputCol="normalized_vector")

hnsw = HnswSimilarity(
    identifierCol='id',
    featuresCol='normalizedFeatures',
    numPartitions=1,
    numThreads=4,
    k=10,
    distanceFunction='inner-product',
    predictionCol='approximate',
    m=48,
    efConstruction=200,
)

bruteForce = BruteForceSimilarity(
    identifierCol=hnsw.getIdentifierCol(),
    featuresCol=hnsw.getFeaturesCol(),
    numPartitions=1,
    numThreads=4,
    k=hnsw.getK(),
    distanceFunction=hnsw.getDistanceFunction(),
    predictionCol='exact',
)

pipeline = Pipeline(stages=[converter, normalizer, hnsw, bruteForce])

items = spark.table('items')

model = pipeline.fit(items)

# computing the exact similarity is expensive so only take a small sample
queries = items.sample(0.01)

output = model.transform(queries)

evaluator = KnnSimilarityEvaluator(approximateNeighborsCol='approximate', exactNeighborsCol='exact')

accuracy = evaluator.evaluate(output)

print(f"Accuracy: {accuracy}")


```
{% endtab %}

{% tab example_2 scala %}
```scala
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.SparkSession

import com.github.jelmerk.spark.knn.bruteforce.BruteForceSimilarity
import com.github.jelmerk.spark.knn.evaluation.KnnSimilarityEvaluator
import com.github.jelmerk.spark.knn.hnsw.HnswSimilarity
import com.github.jelmerk.spark.linalg.Normalizer
import com.github.jelmerk.spark.conversion.VectorConverter

val spark = SparkSession.builder
  .config("spark.jars.packages", "com.github.jelmerk:hnswlib-spark_3_5_2.12:{{ site.version_jvm }}")
  .config("spark.ui.showConsoleProgress", "false")
  .getOrCreate()

val items = spark.table("items")

// often it is acceptable to use float instead of double precision.
// this uses less memory and will be faster

val converter = new VectorConverter()
  .setInputCol("featuresAsMlLibVector")
  .setOutputCol("features")
  .setOutputType("array<float>")

// The cosine distance is obtained with the inner product after normalizing all vectors to unit norm
// this is much faster than calculating the cosine distance directly

val normalizer = new Normalizer()
  .setInputCol("features")
  .setOutputCol("normalizedFeatures")

val hnsw = new HnswSimilarity()
  .setIdentifierCol("id")
  .setFeaturesCol("normalizedFeatures")
  .setNumPartitions(1)
  .setNumThreads(4)
  .setK(10)
  .setDistanceFunction("inner-product")
  .setPredictionCol("approximate")
  .setM(48)
  .setEfConstruction(200)

val bruteForce = new BruteForceSimilarity()
  .setIdentifierCol(hnsw.getIdentifierCol)
  .setFeaturesCol(hnsw.getFeaturesCol)
  .setNumPartitions(1)
  .setNumThreads(4)
  .setK(hnsw.getK)
  .setDistanceFunction(hnsw.getDistanceFunction)
  .setPredictionCol("exact")

val pipeline = new Pipeline()
  .setStages(Array(converter, normalizer, hnsw, bruteForce))

val model = pipeline.fit(items)

// computing the exact similarity is expensive so only take a small sample
val queries = items.sample(0.01)

val output = model.transform(queries)

val evaluator = new KnnSimilarityEvaluator()
  .setApproximateNeighborsCol("approximate")
  .setExactNeighborsCol("exact")

val accuracy = evaluator.evaluate(output)

println(s"Accuracy: $accuracy")

```
{% endtab %}

{% endtabs %}
