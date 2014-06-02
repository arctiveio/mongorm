#! /usr/bin/python

import sys
import os
from setuptools import setup, Extension

version = "0.1"

setup(
    name="mongorm",
    version=version,
    packages=["mongorm"],
    author="Simversity Inc.",
    author_email="dev@simversity.com",
    url="http://simversity.github.io/mongorm",
    license="http://www.apache.org/licenses/LICENSE-2.0",
    description='''Python based ORM for MongoDB'''
)