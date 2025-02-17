import argparse

from pyspark.sql import SparkSession
from pyspark_hnsw.knn import *


def main(spark: SparkSession):
    parser = argparse.ArgumentParser(description='Construct brute force index')
    parser.add_argument('--input', type=str)
    parser.add_argument('--model', type=str)
    parser.add_argument('--output', type=str)
    parser.add_argument('--num_partitions', type=int)
    parser.add_argument('--num_threads', type=int)

    args = parser.parse_args()

    bruteforce = BruteForceSimilarity(identifierCol='id', featuresCol='normalized_features',
                                      distanceFunction='inner-product', numPartitions=args.num_partitions,
                                      numThreads=args.num_threads)

    index_items = spark.read.parquet(args.input)

    model = bruteforce.fit(index_items)

    model.write().overwrite().save(args.output)


if __name__ == '__main__':
    main(SparkSession.builder.getOrCreate())
