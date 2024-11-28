import argparse

from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession


def main(spark):
    parser = argparse.ArgumentParser(description='Query index')
    parser.add_argument('--input', type=str)
    parser.add_argument('--model', type=str)
    parser.add_argument('--output', type=str)
    parser.add_argument('--k', type=int)

    args = parser.parse_args()

    model = PipelineModel.read().load(args.model)

    [_, hnsw_stage] = model.stages
    hnsw_stage.setK(args.k)

    query_items = spark.read.parquet(args.input)

    results = model.transform(query_items)

    results.write.mode('overwrite').json(args.output)

    # you need to destroy the model or the index tasks running in the background will prevent spark from shutting down
    hnsw_stage.dispose()


if __name__ == '__main__':
    main(SparkSession.builder.getOrCreate())
