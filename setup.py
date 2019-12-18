#! /usr/bin/env python3
# -*- coding: utf-8 -*-

# Learn more: https://github.com/kennethreitz/setup.py

from setuptools import setup, find_packages


with open('README.rst') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='magi-utils',
    version='1.0.0',
    description='Utilities for running Magi HDFS/HBase/Spark containers in environments like AWS',
    long_description=readme,
    author='Michael Reynolds',
    author_email='reynoldsm88@gmail.com',
    url='https://github.com/magi-platform/magi-utils',
    license=license,
    packages=find_packages(exclude=('test', 'docs') )
)
