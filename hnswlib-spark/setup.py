import os
from setuptools import setup, find_packages


setup(
    name='hnswlib-spark',
    version=os.getenv('VERSION'),
    description='Pyspark module for hnswlib',
    url="https://github.com/jelmerk/hnswlib/tree/master/hnswlib-pyspark",
    packages=find_packages(where="src/main/python"),
    package_dir={'': 'src/main/python'},
    python_requires='>=3.7',
    license="Apache License 2.0",
    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Operating System :: OS Independent',
    ],
)