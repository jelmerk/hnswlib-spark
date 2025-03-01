hnswlib-examples-pyspark-luigi
==============================

## Description

Demonstrates how to orchestrate building and querying a knn index with [Luigi](https://github.com/spotify/luigi)

## Setting up the project

Make sure you have the following software installed

- [docker](https://www.docker.com/products/docker-desktop/)
- [vscode](https://code.visualstudio.com/) with the [devcontainer](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers) plugin

Make sure docker has at least 24 gb of memory allocated to it

Open this folder in vs code and choose reopen in container. This will start up a fully functional spark environment

## Running the pipeline

Open a new terminal inside of vscode and execute the following command from the `app` folder


    python -m luigi --module flow Query --local-scheduler

## Accessing cluster resources

To access cluster resources open firefox, then go to settings > Network settings > Connection settings and use:
SOCKS Host: localhost Port 1080

After that you can navigate to

- http://spark-master:8080 to access the spark ui
- http://minio:9001 to access the minio ui with username: accesskey and password: secretkey