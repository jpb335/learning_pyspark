"""
Seventh Chapter

Goal:
Read in delimited file and show the most popular (i.e. most rated rather than most highly rated) movies
"""

import os
import collections

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
    session = SparkSession.builder.appName("MostPopularMovie").getOrCreate()

    schema = StructType(
        [
            StructField("user_id", dataType=IntegerType()),
            StructField("movie_id", dataType=IntegerType()),
            StructField("rating", dataType=IntegerType()),
            StructField("timestamp", dataType=LongType()),
        ]
    )

    # For purposes of this exercise, we only need the first two columns.  By defining only the first two, the rest of
    # the file will be ignored
    movie_schema = StructType(
        [
            StructField("movie_id", dataType=IntegerType()),
            StructField("movie_name", dataType=StringType()),
        ]
    )

    movie_df = session.read.csv(MOVIE_FILE_LOCATION, sep="|", schema=movie_schema)
    rating_df = session.read.csv(RATING_FILE_LOCATION, sep="\t", schema=schema)

    rating_df = rating_df.join(movie_df, on="movie_id")

    top_movies = (
        rating_df.groupBy("movie_id", "movie_name")
        .count()
        .orderBy(functions.desc("count"))
    )

    top_movies.show(n=10)

    session.stop()


if __name__ == "__main__":
    main()
