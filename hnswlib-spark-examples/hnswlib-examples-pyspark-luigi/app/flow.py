import urllib.request
import shutil

import luigi
import multiprocessing
from luigi import FloatParameter, IntParameter, LocalTarget, Parameter
from luigi.contrib.spark import SparkSubmitTask
from luigi.format import Nop
from luigi.contrib.external_program import ExternalProgramTask
# from luigi.contrib.hdfs import HdfsFlagTarget
# from luigi.contrib.s3 import S3FlagTarget

PACKAGES='com.github.jelmerk:hnswlib-spark_3_5_2.12:2.0.0-alpha.2,io.delta:delta-spark_2.12:3.3.0'

multiprocessing.set_start_method("fork", force=True)
num_cores=multiprocessing.cpu_count()

class Download(luigi.Task):
    """
    Download the input dataset.
    """

    url = Parameter(default='https://nlp.stanford.edu/data/glove.42B.300d.zip')

    def output(self):
        return LocalTarget('/tmp/dataset.zip', format=Nop)

    def run(self):
        # noinspection PyTypeChecker
        with urllib.request.urlopen(self.url) as response:
            with self.output().open('wb') as f:
                shutil.copyfileobj(response, f)


class Unzip(ExternalProgramTask):
    """
    Unzip the input dataset.
    """

    def requires(self):
        return Download()

    def output(self):
        return LocalTarget('/tmp/dataset', format=Nop)

    def program_args(self):
        self.output().makedirs()
        return ['unzip',
                '-u',
                '-q',
                '-d', self.output().path,
                self.input().path]


class Convert(SparkSubmitTask):
    """
    Convert the input dataset to parquet.
    """

    master = 'spark://spark-master:7077'

    deploy_mode = 'client'

    name = 'Convert'

    app = 'convert.py'

    packages = [PACKAGE]

    @property
    def conf(self):
        return {'spark.sql.extensions': 'io.delta.sql.DeltaSparkSessionExtension',
                'spark.sql.catalog.spark_catalog': 'org.apache.spark.sql.delta.catalog.DeltaCatalog'}

    def requires(self):
        return Unzip()

    def app_options(self):
        return [
            "--input", self.input().path,
            "--output", self.output().path
        ]

    def output(self):
        # return HdfsFlagTarget('/tmp/vectors_parquet')
        # return S3FlagTarget('/tmp/vectors_parquet')
        return LocalTarget('/tmp/vectors_parquet', format=Nop)


class HnswIndex(SparkSubmitTask):
    """
    Construct the hnsw index and persists it to disk.
    """

    master = 'spark://spark-master:7077'

    deploy_mode = 'client'

    name = 'Hnsw index'

    app = 'hnsw_index.py'

    packages = [PACKAGE]

    m = IntParameter(default=16)

    ef_construction = IntParameter(default=200)

    @property
    def conf(self):
        return {'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
                'spark.kryo.registrator': 'com.github.jelmerk.spark.HnswLibKryoRegistrator',
                'spark.sql.extensions': 'io.delta.sql.DeltaSparkSessionExtension',
                'spark.sql.catalog.spark_catalog': 'org.apache.spark.sql.delta.catalog.DeltaCatalog'}

    def requires(self):
        return Convert()

    def app_options(self):
        return [
            '--input', self.input().path,
            '--output', self.output().path,
            '--m', self.m,
            '--ef_construction', self.ef_construction,
            '--num_partitions', 1,
            '--num_threads', num_cores
        ]

    def output(self):
        # return HdfsFlagTarget('/tmp/hnsw_index')
        # return S3FlagTarget('/tmp/hnsw_index')
        return LocalTarget('/tmp/hnsw_index', format=Nop)


class Query(SparkSubmitTask):
    """
    Query the constructed knn index.
    """

    master = 'spark://spark-master:7077'

    deploy_mode = 'client'

    packages = [PACKAGE]

    name = 'Query index'

    app = 'query.py'

    k = IntParameter(default=10)

    @property
    def conf(self):
        return {'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
                'spark.kryo.registrator': 'com.github.jelmerk.spark.HnswLibKryoRegistrator',
                'spark.sql.extensions': 'io.delta.sql.DeltaSparkSessionExtension',
                'spark.sql.catalog.spark_catalog': 'org.apache.spark.sql.delta.catalog.DeltaCatalog'}

    def requires(self):
        return {'vectors': Convert(),
                'index': HnswIndex()}

    def app_options(self):
        return [
            '--input', self.input()['vectors'].path,
            '--model', self.input()['index'].path,
            '--output', self.output().path,
            '--k', self.k
        ]

    def output(self):
        # return HdfsFlagTarget('/tmp/query_results')
        # return S3FlagTarget('/tmp/query_results')
        return LocalTarget('/tmp/query_results')


class BruteForceIndex(SparkSubmitTask):
    """
    Construct the brute force index and persists it to disk.
    """

    master = 'spark://spark-master:7077'

    deploy_mode = 'client'

    name = 'Brute force index'

    app = 'bruteforce_index.py'

    packages = [PACKAGE]

    @property
    def conf(self):
        return {'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
                'spark.kryo.registrator': 'com.github.jelmerk.spark.HnswLibKryoRegistrator',
                'spark.sql.extensions': 'io.delta.sql.DeltaSparkSessionExtension',
                'spark.sql.catalog.spark_catalog': 'org.apache.spark.sql.delta.catalog.DeltaCatalog'}

    def requires(self):
        return Convert()

    def app_options(self):
        return [
            '--input', self.input().path,
            '--output', self.output().path,
            '--num_partitions', 1,
            '--num_threads', num_cores
        ]

    def output(self):
        # return HdfsFlagTarget('/tmp/brute_force_index')
        # return S3FlagTarget('/tmp/brute_force_index')
        return LocalTarget('/tmp/brute_force_index', format=Nop)


class Evaluate(SparkSubmitTask):
    """
    Evaluate the accuracy of the approximate k-nearest neighbors model vs a bruteforce baseline.
    """

    master = 'spark://spark-master:7077'

    deploy_mode = 'client'

    k = IntParameter(default=10)

    fraction = FloatParameter(default=0.0001)

    seed = IntParameter(default=123)

    name = 'Evaluate performance'

    app = 'evaluate_performance.py'

    packages = [PACKAGE]

    @property
    def conf(self):
        return {'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
                'spark.kryo.registrator': 'com.github.jelmerk.spark.HnswLibKryoRegistrator',
                'spark.sql.extensions': 'io.delta.sql.DeltaSparkSessionExtension',
                'spark.sql.catalog.spark_catalog': 'org.apache.spark.sql.delta.catalog.DeltaCatalog'}

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
        # return HdfsFlagTarget('/tmp/metrics')
        # return S3FlagTarget('/tmp/metrics')
        return LocalTarget('/tmp/metrics', format=Nop)
