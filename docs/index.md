---
title: Overview
layout: home
nav_order: 1
---

# Nearest neighbour search on Spark
{: .fs-9 }

Hnswlib Spark integrates HNSW with Spark MLlib, delivering scalable nearest neighbor search for Python and Scala.
{: .fs-6 .fw-300 }

[Get started now](docs/get_started.html){: .btn .btn-primary .fs-5 .mb-4 .mb-md-0 .mr-2 }
[View it on GitHub][Hnswlib spark repo]{: .btn .fs-5 .mb-4 .mb-md-0 }


K-Nearest Neighbors (KNN) search is a fundamental algorithm in machine learning and data science used to find the k most similar data points to a given query point. It operates in high-dimensional spaces and is widely applied in recommendation systems, anomaly detection, classification, and clustering. The core idea behind KNN is based on measuring distances—typically using Euclidean distance, cosine similarity, or other metrics—to identify the closest neighbors within a dataset. While a brute-force approach (comparing all points) is computationally expensive, approximate methods like Hierarchical Navigable Small World (HNSW) graphs significantly speed up search times, making KNN feasible for large-scale datasets. In distributed environments like Apache Spark, KNN search can be parallelized for scalability, enabling efficient nearest neighbor retrieval across vast data collections.



## About the project

### License

Hnwslib spark is distributed under the [Apache V2 license](https://github.com/jelmerk/hnswlib-spark/tree/master/LICENSE.txt).

### Contributing

When contributing to this repository, please first discuss the change you wish to make via issue,
email, or any other method with the owners of this repository before making a change.

#### Thank you to the contributors of Hnswlib Spark

<ul class="list-style-none">
{% for contributor in site.github.contributors %}
  <li class="d-inline-block mr-1">
     <a href="{{ contributor.html_url }}"><img src="{{ contributor.avatar_url }}" width="32" height="32" alt="{{ contributor.login }}"></a>
  </li>
{% endfor %}
</ul>

### Code of Conduct

Hnswlib spark adheres to No Code of Conduct. We are all adults. We accept anyone's contributions. Nothing else matters.

[View our Code of Conduct](https://github.com/jelmerk/hnswlib-spark/blob/v2/CODE_OF_CONDUCT.md) on our GitHub repository.


[Hnswlib spark repo]: https://github.com/jelmerk/hnswlib-spark
