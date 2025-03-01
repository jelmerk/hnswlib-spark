hnswlib-examples-pyspark-jupyter-notebook
=========================================

## Description

Demonstrates how to create and query hnsw indices from a jupyter notebook running inside vs code

## Setting up the project

Make sure you have the following software installed

- [docker](https://www.docker.com/products/docker-desktop/)
- [vscode](https://code.visualstudio.com/) with the [devcontainer](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers) plugin

Make sure docker has at least 24 gb of memory allocated to it

Open this folder in vs code and choose reopen in container. This will start up a fully functional spark environment 

## Running the project

From within vs code open any of the notebooks in the notebooks folder and start executing the cells within 

## Accessing cluster resources

To access cluster resources open firefox, then go to settings > Network settings > Connection settings and use:
SOCKS Host: localhost Port 1080

After that you can navigate to

- http://spark-master:8080 to access the spark ui
- http://minio:9001 to access the minio ui with username: accesskey and password: secretkey

