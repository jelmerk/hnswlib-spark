import argparse


from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark_hnsw.knn import BruteForceSimilarityModel, HnswSimilarityModel
from pyspark_hnsw.evaluation import KnnSimilarityEvaluator


def main(spark: SparkSession):
    parser = argparse.ArgumentParser(description='Evaluate performance of the index')
    parser.add_argument('--hnsw_model', type=str)
    parser.add_argument('--bruteforce_model', type=str)
    parser.add_argument('--input', type=str)
    parser.add_argument('--output', type=str)
    parser.add_argument('--k', type=int)
    parser.add_argument('--fraction', type=float)
    parser.add_argument('--seed', type=int)

    args = parser.parse_args()

    sample_query_items = spark.read.parquet(args.input).sample(False, args.fraction, args.seed)

    hnsw_model = HnswSimilarityModel.read().load(args.hnsw_model)
    hnsw_model.setK(args.k)
    hnsw_model.setPredictionCol('approximate')

    bruteforce_model = BruteForceSimilarityModel.read().load(args.bruteforce_model)
    bruteforce_model.setK(args.k)
    bruteforce_model.setPredictionCol('exact')

    pipeline = PipelineModel(stages=[hnsw_model, bruteforce_model])

    sample_results = pipeline.transform(sample_query_items)

    evaluator = KnnSimilarityEvaluator(approximateNeighborsCol='approximate', exactNeighborsCol='exact')

    accuracy = evaluator.evaluate(sample_results)

    spark.createDataFrame([[accuracy]], ['accuracy']).repartition(1).write.mode('overwrite').csv(args.output)

    hnsw_model.dispose()
    bruteforce_model.dispose()


if __name__ == '__main__':
    main(SparkSession.builder.getOrCreate())
