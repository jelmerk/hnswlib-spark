hnswlib-examples-pyspark-luigi
==============================

## Description


## How to run

Make sure you have the following software installed

- [docker](https://www.docker.com/products/docker-desktop/)
- [vscode](https://code.visualstudio.com/) with the [devcontainer](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers) plugin


1. Create the spark environment with


    docker-compose up

2. Open this folder in vs code and choose reopen in container

3. Open a new terminal inside vscode and execute the following command from the app folder


    python -m luigi --module flow Query --local-scheduler

