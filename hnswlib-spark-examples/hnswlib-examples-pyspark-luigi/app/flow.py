import zipfile
import boto3
import requests
import io

import luigi
from luigi import FloatParameter, IntParameter, Parameter
from luigi.contrib.spark import SparkSubmitTask
from luigi.contrib.s3 import S3FlagTarget, S3Target


class Download(luigi.Task):
    """
    Download the input dataset.
    """

    url = Parameter(default='https://huggingface.co/stanfordnlp/glove/resolve/main/glove.42B.300d.zip')

    def output(self):
        return S3Target('s3a://spark/input/glove.42B.300d.txt')

    def run(self):
        s3 = boto3.client("s3")
        bucket_name = "spark"

        with requests.get(self.url, stream=True) as response:
            response.raise_for_status()

            with zipfile.ZipFile(io.BytesIO(response.raw.read()), "r") as zip_ref:
                for file_name in zip_ref.namelist():
                    with zip_ref.open(file_name) as file:
                        s3.upload_fileobj(file, bucket_name, f"input/{file_name}")


class BaseSparkSubmitTask(SparkSubmitTask):

    master = 'spark://spark-master:7077'

    deploy_mode = 'client'

    packages = ['com.github.jelmerk:hnswlib-spark_3_5_2.12:2.0.0-alpha.6']

    executor_memory = "12G"


class Convert(BaseSparkSubmitTask):
    """
    Convert the input dataset to parquet.
    """

    name = 'Convert'

    app = 'convert.py'

    def requires(self):
        return Download()

    def app_options(self):
        return [
            "--input", self.input().path,
            "--output", self.output().path
        ]

    def output(self):
        return S3FlagTarget('s3a://spark/vectors/')


class HnswIndex(BaseSparkSubmitTask):
    """
    Construct the hnsw index and persists it to disk.
    """

    name = 'Hnsw index'

    app = 'hnsw_index.py'

    m = IntParameter(default=16)

    ef_construction = IntParameter(default=200)

    def requires(self):
        return Convert()

    def app_options(self):
        return [
            '--input', self.input().path,
            '--output', self.output().path,
            '--m', self.m,
            '--ef_construction', self.ef_construction,
            '--num_partitions', 1,
            # Use 6 cores instead of the 8 we have available so that in the evaluate 
            #
            # this model uses 6 cores
            # The brute force model uses 1 core
            #
            # Leaving one core for querying
            '--num_threads', 6
        ]

    def output(self):
        return S3FlagTarget('s3a://spark/hnsw_index/', flag='metadata/_SUCCESS')


class Query(BaseSparkSubmitTask):
    """
    Query the constructed knn index.
    """

    name = 'Query index'

    app = 'query.py'

    k = IntParameter(default=10)

    def requires(self):
        return {'vectors': Convert(), 'index': HnswIndex()}

    def app_options(self):
        return [
            '--input', self.input()['vectors'].path,
            '--model', self.input()['index'].path,
            '--output', self.output().path,
            '--k', self.k
        ]

    def output(self):
        return S3FlagTarget('s3a://spark/query_results/')


class BruteForceIndex(BaseSparkSubmitTask):
    """
    Construct the brute force index and persists it to disk.
    """

    name = 'Brute force index'

    app = 'bruteforce_index.py'

    def requires(self):
        return Convert()

    def app_options(self):
        return [
            '--input', self.input().path,
            '--output', self.output().path,
            '--num_partitions', 1,
            '--num_threads', 1
        ]

    def output(self):
        return S3FlagTarget('s3a://spark/brute_force_index/', flag='metadata/_SUCCESS')


class Evaluate(BaseSparkSubmitTask):
    """
    Evaluate the accuracy of the approximate k-nearest neighbors model vs a bruteforce baseline.
    """

    k = IntParameter(default=5)

    fraction = FloatParameter(default=0.0001)

    seed = IntParameter(default=123)

    name = 'Evaluate performance'

    app = 'evaluate_performance.py'

    def requires(self):
        return {'vectors': Convert(),
                'hnsw_index': HnswIndex(),
                'bruteforce_index': BruteForceIndex()}

    def app_options(self):
        return [
            '--input', self.input()['vectors'].path,
            '--output', self.output().path,
            '--hnsw_model', self.input()['hnsw_index'].path,
            '--bruteforce_model', self.input()['bruteforce_index'].path,
            '--k', self.k,
            '--seed', self.seed,
            '--fraction', self.fraction,
        ]

    def output(self):
        return S3FlagTarget('s3a://spark/metrics/')
