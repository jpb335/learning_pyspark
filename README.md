# Learning Spark

## Description
This repository documents my learning of the Spark tool/ecosystem

## Getting setup
### Required tools
- [Python](python.org/downloads) (`3.10` used)
- [Spark](https://spark.apache.org/downloads.html) (`3.5.1` used)
- [Java](https://www.java.com/en/download) (`11.0.22` used)
- [Pipenv](https://pipenv.pypa.io/en/latest/) (`2023.12.1` used)

Once all dependencies are installed, setup your `pipenv` environment

```bash
pipenv --python=3.10
pipenv install --dev
pipenv shell
```

## Housekeeping
### Formatting
[Black](https://black.readthedocs.io/en/stable/#) is used for formatting.  To format:

```
black .
```

## Dataset
Much of the work in this repository is based on the Movie Lens 100k dataset.  At time of writing, this dataset can be downloaded in the following manner (for Linux)

```bash
wget https://files.grouplens.org/datasets/movielens/ml-100k.zip
unzip ml-100k.zip
rm ml-100k.zip
```