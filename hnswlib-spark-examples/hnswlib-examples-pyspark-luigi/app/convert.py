import argparse

from pyspark.ml import PipelineModel
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession
from pyspark_hnsw.conversion import VectorConverter
from pyspark_hnsw.linalg import Normalizer


def main(spark: SparkSession):
    parser = argparse.ArgumentParser(description='Convert and normalize input data and save it as parquet')
    parser.add_argument('--input', type=str)
    parser.add_argument('--output', type=str)
    args = parser.parse_args()

    words_df = spark.read \
        .option('inferSchema', 'true') \
        .option('quote', '\u0000') \
        .option('delimiter', ' ') \
        .csv(args.input) \
        .withColumnRenamed('_c0', 'id')
    
    vector_assembler = VectorAssembler(inputCols=words_df.columns[1:], outputCol='features_as_vector')

    converter = VectorConverter(inputCol='features_as_vector', outputCol='features', outputType='array<float>')

    normalizer = Normalizer(inputCol='features', outputCol='normalized_features')

    pipeline = PipelineModel(stages=[vector_assembler, converter, normalizer])

    pipeline.transform(words_df) \
        .select('id', 'normalized_features') \
        .write \
        .parquet(args.output)


if __name__ == "__main__":
    main(SparkSession.builder.getOrCreate())
