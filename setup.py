from setuptools import setup, find_packages
import os

current_dir = os.getcwd()

with open("README.md") as f:
    readme = f.read()

setup(
    name="page-count",
    version="1.0.0",
    packages=find_packages(exclude="tests"),
    description="This application counts the page count and user-page-count.",
    long_decription=readme
)