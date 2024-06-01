"""
Chapter 8

Goal:
Show the use of broadcast variables

Show the most popular movie (by review count, not rating) using the broadcast variable feature to retrieve
the movie names
"""

import codecs
import csv
import os
from typing import Dict

from pyspark.sql import SparkSession, functions
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType

RATING_FILE_LOCATION = os.path.abspath(
    os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "../resources/ml-100k/u.data"
    )
)

MOVIE_FILE_LOCATION = os.path.abspath(
    os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "../resources/ml-100k/u.item"
    )
)


def main():
    def lookup_movie_name(movie_id: int) -> str:
        return movies.value[movie_id]

    movie_dictionary = _get_movie_name_dictionary()
    session = SparkSession.builder.appName("MostPopularMovie").getOrCreate()

    movies = session.sparkContext.broadcast(movie_dictionary)

    # Define UDF for lookup
    lookup_udf = functions.udf(lookup_movie_name)

    schema = StructType(
        [
            StructField("user_id", dataType=IntegerType()),
            StructField("movie_id", dataType=IntegerType()),
            StructField("rating", dataType=IntegerType()),
            StructField("timestamp", dataType=LongType()),
        ]
    )

    rating_df = session.read.csv(RATING_FILE_LOCATION, sep="\t", schema=schema)

    rating_df = rating_df.withColumn(
        "movie_name", lookup_udf(functions.col("movie_id"))
    )

    top_movies = (
        rating_df.groupBy("movie_id", "movie_name")
        .count()
        .orderBy(functions.desc("count"))
    )

    top_movies.show(n=10, truncate=False)

    session.stop()


def _get_movie_name_dictionary() -> Dict[int, str]:
    movie_dict = {}
    with codecs.open(
        MOVIE_FILE_LOCATION, "r", encoding="ISO-8859-1", errors="ignore"
    ) as fh:
        reader = csv.reader(fh, delimiter="|")

        for each in reader:
            movie_dict[int(each[0])] = each[1]

    return movie_dict


if __name__ == "__main__":
    main()
