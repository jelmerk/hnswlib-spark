---
title: Building from source
nav_order: 3
---

# Building From Source

Obtain the source code with

```shell
git clone git@github.com:jelmerk/hnswlib-spark.git
```

Install [sdkman](https://sdkman.io/) with

```shell
curl -s "https://get.sdkman.io" | bash
```

Use it to install the required software

```shell
sdk env install
```

Finally run this command to build the artifact for spark 3.5 and scala 2.12

```shell
ENV=ci sbt ++2.12.18 clean test assembly -DsparkVersion=3.5.0
```