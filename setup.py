#!/usr/bin/env python
"""The missing PySpark utils steup file."""

import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pyspark_utils",
    version="1.4.0",
    license="Apache License Version 2.0",
    author="Xiangquan Xiao",
    author_email="xiaoxiangquan@gmail.com",
    description="The missing PySpark utils",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/xiaoxq/pyspark-utils",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        "pyspark",
    ],
)
