[![Build Status](https://github.com/jelmerk/hnswlib-spark/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/jelmerk/hnswlib/actions/workflows/ci.yml)

Hnswlib spark
=============

Spark / PySpark integration for the [hsnwlib](https://github.com/jelmerk/hnswlib) project that was originally part of
the core hnswlib project.

To find out more about how to use this library take a look at the [hnswlib-spark-examples](https://github.com/jelmerk/hnswlib-spark/tree/master/hnswlib-spark-examples) module or browse the documentation
in the readme files of the submodules

## Building Hnswlib spark

Install [sdkman](https://sdkman.io/) with

```shell
curl -s "https://get.sdkman.io" | bash
```

Then run this command to build the artifact for spark 3.5 and scala 2.12

```shell
sdk env install
ENV=ci sbt ++2.12.18 clean test assembly -DsparkVersion=3.5.0
```