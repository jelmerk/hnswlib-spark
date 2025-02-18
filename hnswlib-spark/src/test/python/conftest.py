import os

import pytest

from pyspark.sql import SparkSession

@pytest.fixture(scope="session", autouse=True)
def spark(request):
    # unset SPARK_HOME or it will use whatever is configured on the host system instead of the pip packages
    if "SPARK_HOME" in os.environ:
        del os.environ['SPARK_HOME']

    sc = SparkSession.builder \
        .config("spark.driver.extraClassPath", os.environ["ARTIFACT_PATH"]) \
        .master("local[*]") \
        .getOrCreate()

    request.addfinalizer(lambda: sc.stop())

    return sc
