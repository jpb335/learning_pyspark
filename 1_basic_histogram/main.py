"""
First Chapter

Goal:
Read in delimited file and show simple histogram

This will read in a set of movie data and show, using both RDD and Dataframes, a basic
histogram of movie reviews by "stars" (1-5)
"""

import os
import collections

from pyspark import SparkConf, SparkContext
from pyspark.sql import DataFrameReader, SparkSession

FILE_LOCATION = os.path.abspath(
    os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "../resources/ml-100k/u.data"
    )
)


def main():
    _check_resources_exists()

    conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
    sc = SparkContext(conf=conf)

    _run_rdd(spark_context=sc)
    _run_dataframe(spark_context=sc)


def _run_rdd(spark_context: SparkContext):
    print("### RDD ###")

    lines = spark_context.textFile(FILE_LOCATION)

    ratings = lines.map(lambda line: line.split()[2])
    result = ratings.countByValue()

    sorted_results = collections.OrderedDict(sorted(result.items(), reverse=True))
    for key, value in sorted_results.items():
        print(f"{key} stars: {value}")

    print("### END RDD ###\n\n")


def _run_dataframe(spark_context: SparkContext):
    print("### DATAFRAME ###")

    session = SparkSession(spark_context)

    lines_df = DataFrameReader(spark=session).csv(FILE_LOCATION, sep="\t")
    ratings_df = (
        lines_df.groupBy(lines_df.columns[2])
        .count()
        .withColumnRenamed(lines_df.columns[2], "Rating")
    )

    ratings_df.sort("Rating", ascending=False).show()

    print("### END DATAFRAME ###")


def _check_resources_exists():
    if not os.path.exists(FILE_LOCATION):
        raise FileNotFoundError(
            f"Expected ml-100k dataset in {FILE_LOCATION}.  See README for download instructions"
        )


if __name__ == "__main__":
    main()
