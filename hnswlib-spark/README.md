[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.jelmerk/hnswlib-spark_3_3_2.12/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.jelmerk/hnswlib-spark_3.3_2.12) [![Scaladoc](https://javadoc.io/badge2/com.github.jelmerk/hnswlib-spark_3_3_2.12/javadoc.svg)](https://javadoc.io/doc/com.github.jelmerk/hnswlib-spark_2.3_2.11)


hnswlib-spark
=============

[Apache spark](https://spark.apache.org/) integration for hnswlib.

Setup
-----

Find the package appropriate for your spark setup

|             | Scala 2.12                                      | Scala 2.13                                      |
|-------------|-------------------------------------------------|-------------------------------------------------|
| Spark 3.4.x | com.github.jelmerk:hnswlib-spark_3_4_2.12:2.0.0 | com.github.jelmerk:hnswlib-spark_3_4_2.13:2.0.0 |
| Spark 3.5.x | com.github.jelmerk:hnswlib-spark_3_5_2.12:2.0.0 | com.github.jelmerk:hnswlib-spark_3_5_2.13:2.0.0 |

Pass this as an argument to spark

    --packages 'com.github.jelmerk:hnswlib-spark_3_5_2.12:2.0.0'

Example usage
-------------

Basic:

```scala
import com.github.jelmerk.spark.knn.hnsw.HnswSimilarity

val hnsw = new HnswSimilarity()
  .setIdentifierCol("id")
  .setFeaturesCol("features")
  .setNumPartitions(2)
  .setM(48)
  .setEf(5)
  .setEfConstruction(200)
  .setK(200)
  .setDistanceFunction("cosine")
  
val model = hnsw.fit(indexItems)

model.transform(indexItems).write.parquet("/path/to/output")
```

Advanced:

```scala
import org.apache.spark.ml.Pipeline

import com.github.jelmerk.spark.knn.bruteforce.BruteForceSimilarity
import com.github.jelmerk.spark.knn.evaluation.KnnSimilarityEvaluator
import com.github.jelmerk.spark.knn.hnsw.HnswSimilarity
import com.github.jelmerk.spark.linalg.Normalizer
import com.github.jelmerk.spark.conversion.VectorConverter

// often it is acceptable to use float instead of double precision. 
// this uses less memory and will be faster 

val converter = new VectorConverter()
    .setInputCol("featuresAsMlLibVector")
    .setOutputCol("features")

// The cosine distance is obtained with the inner product after normalizing all vectors to unit norm 
// this is much faster than calculating the cosine distance directly

val normalizer = new Normalizer()
  .setInputCol("features")
  .setOutputCol("normalizedFeatures")

val hnsw = new HnswSimilarity()
  .setIdentifierCol("id")
  .setFeaturesCol("normalizedFeatures")
  .setNumPartitions(2)
  .setK(200)
  .setDistanceFunction("inner-product")
  .setPredictionCol("approximate")
  .setM(48)
  .setEfConstruction(200)

val bruteForce = new BruteForceSimilarity()
  .setIdentifierCol(hnsw.getIdentifierCol)
  .setFeaturesCol(hnsw.getFeaturesCol)
  .setNumPartitions(2)
  .setK(hnsw.getK)
  .setDistanceFunction(hnsw.getDistanceFunction)
  .setPredictionCol("exact")

val pipeline = new Pipeline()
  .setStages(Array(converter, normalizer, hnsw, bruteForce))

val model = pipeline.fit(indexItems)

// computing the exact similarity is expensive so only take a small sample
val queryItems = indexItems.sample(0.01)

val output = model.transform(queryItems)

val evaluator = new KnnSimilarityEvaluator()
  .setApproximateNeighborsCol("approximate")
  .setExactNeighborsCol("exact")

val accuracy = evaluator.evaluate(output)

println(s"Accuracy: $accuracy")

// save the model
model.write.overwrite.save("/path/to/model")
```
