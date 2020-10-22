#!/usr/bin/env python
from os.path import abspath, join, dirname
from platform import system
from shutil import copyfile

from setuptools import setup

ccm_script = "ccm"
if system() == "Windows":
    copyfile("ccm", "ccm.py")
    ccm_script = "ccm.py"

with open("requirements.txt") as requirements_file:
    requires = [package for package in requirements_file.readlines()]

with open(abspath(join(dirname(__file__), "README.md"))) as readme_file:
    long_description = readme_file.read()

setup(
    name="ccm",
    version="2.0.5",
    description="Cassandra Cluster Manager",
    long_description_content_type="text/markdown",
    long_description=long_description,
    author="Balalis Shlomo, Bykov Aleksandr, Efraimov Oren, Fruchter Israel, Gelcer Fabio, Janković Aleksandar and"
           "Kaikov Yaron",
    url="https://github.com/scylladb/scylla-ccm",
    packages=["ccmlib", "ccmlib.cmds"],
    scripts=[ccm_script],
    install_requires=requires,
    test_require=["pytest"],
    classifiers=[
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.3",
        "Programming Language :: Python :: 3.4"
    ],
    include_package_data=True,
)
