---
title: Installation
nav_order: 2
---

# Installation

## Python

```python
pip install hnswlib-spark=={{ site.version_py }}
```

## JVM

{% tabs jvm-install %}

{% tab jvm-install sbt %}
```scala

// for spark 3.4.x
libraryDependencies += "com.github.jelmerk" %% "hnswlib-spark_3_5" % "{{ site.version_jvm }}"

// for spark 3.5.x
libraryDependencies += "com.github.jelmerk" %% "hnswlib-spark_3_5" % "{{ site.version_jvm }}"
```
{% endtab %}

{% tab jvm-install maven %}
```xml

<properties>
    <scala.binary.version>2.12</scala.binary.version>
</properties>

<dependencies>

   <!-- for spark 3.4.x -->
   <dependency>
      <groupId>com.github.jelmerk</groupId>
      <artifactId>hnswlib-spark_3_4_${scala.binary.version}</artifactId>
      <version>{{ site.version_jvm }}</version>
   </dependency>

    <!-- for spark 3.5.x -->
    <dependency>
        <groupId>com.github.jelmerk</groupId>
        <artifactId>hnswlib-spark_3_5_${scala.binary.version}</artifactId>
        <version>{{ site.version_jvm }}</version>
    </dependency>
</dependencies>
```
{% endtab %}

{% tab jvm-install gradle %}
```gradle
ext.scalaBinaryVersion = '2.12'

dependencies {
    // for spark 3.4.x 
    implementation("com.github.jelmerk:hnswlib-spark_3_4_$scalaBinaryVersion:{{ site.version_jvm }}")
    // for spark 3.5.x
    implementation("com.github.jelmerk:hnswlib-spark_3_5_$scalaBinaryVersion:{{ site.version_jvm }}")
}
```
{% endtab %}

{% endtabs %}

## Databricks

1. Create a cluster if you don’t have one already

2. In Libraries tab inside your cluster go to Install New -> Maven -> Coordinates and enter
   
   for DBR 13.3 LTS:
   ```
   com.github.jelmerk:hnswlib-spark_3_4_2.12:{{ site.version_jvm }}
   ```
   
   for DBR 14.3 LTS and above:
   ```
   com.github.jelmerk:hnswlib-spark_3_5_2.12:{{ site.version_jvm }}
   ```
   then press install

3. Optionally add the following cluster settings for faster searches

   Advanced Options -> Spark -> Environment variables:
   ```
   JNAME=zulu17-ca-amd64
   ```

   Advanced Options -> Spark -> Spark config
   ```
   spark.executor.extraJavaOptions --enable-preview --add-modules jdk.incubator.vector
   ```
   
Now you can attach your notebook to the cluster and use Hnswlib spark!

## Spark shell

```bash
# for spark 3.4.x`
spark-shell --packages 'com.github.jelmerk:hnswlib-spark_3_4_2.12:{{ site.version_jvm }}'

# for spark 3.5.x`
spark-shell --packages 'com.github.jelmerk:hnswlib-spark_3_5_2.12:{{ site.version_jvm }}'
```

## Pyspark shell

```bash
# for spark 3.4.x   
pyspark --packages 'com.github.jelmerk:hnswlib-spark_3_4_2.12:{{ site.version_jvm }}'

# for spark 3.5.x and scala 2.12,  
pyspark --packages 'com.github.jelmerk:hnswlib-spark_3_5_2.12:{{ site.version_jvm }}'
```