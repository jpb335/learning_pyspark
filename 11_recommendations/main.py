"""
Chapter 11

Goal:
Use item-based collaborative filtering to show similar movies.  Employ filtering and caching
"""

import os
import sys

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as func
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

SCORE_THRESHOLD = 0.97
CO_OCCURRENCE_THRESHOLD = 50.0


def main(movie_id: int):
    # Use all cores via local[*]
    session = (
        SparkSession.builder.appName("MostPopularMovie")
        .master("local[*]")
        .getOrCreate()
    )

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

    rating_df = rating_df.select("user_id", "movie_id", "rating")

    # Get every movie rated by the same user
    movie_pairs = (
        rating_df.alias("ratings_1")
        .join(
            rating_df.alias("ratings_2"),
            (func.col("ratings_1.user_id") == func.col("ratings_2.user_id"))
            & (func.col("ratings_1.movie_id") < func.col("ratings_2.movie_id")),
        )
        .select(
            func.col("ratings_1.movie_id").alias("movie_id_1"),
            func.col("ratings_2.movie_id").alias("movie_id_2"),
            func.col("ratings_1.rating").alias("rating_1"),
            func.col("ratings_2.rating").alias("rating_2"),
        )
    )

    computed = _compute_cosine_similarity(data=movie_pairs).cache()

    filtered = computed.filter(
        ((func.col("movie_id_1") == movie_id) | (func.col("movie_id_2") == movie_id))
        & (func.col("score") > SCORE_THRESHOLD)
        & (func.col("number_of_pairs") > CO_OCCURRENCE_THRESHOLD)
    )

    results = filtered.sort(func.col("score").desc()).take(10)

    print(f"Top 10 similar movies for {_get_movie_name(movie_df, movie_id)}")

    for each in results:
        similar_movie_id = each.movie_id_1
        if similar_movie_id == movie_id:
            similar_movie_id = each.movie_id_2

        print(
            f"{_get_movie_name(movie_df, similar_movie_id)}\tscore: {each.score}\tstrength: {each.number_of_pairs}"
        )


def _compute_cosine_similarity(data: DataFrame) -> DataFrame:
    # Compute the xx, yy, and xy columns
    pair_scores = (
        data.withColumn("xx", func.col("rating_1") * func.col("rating_1"))
        .withColumn("xy", func.col("rating_1") * func.col("rating_2"))
        .withColumn("yy", func.col("rating_2") * func.col("rating_2"))
    )

    # Compute the numerator, denominator, and number of pairs columns
    calculate_similarity = pair_scores.groupBy("movie_id_1", "movie_id_2").agg(
        func.sum(func.col("xy")).alias("numerator"),
        (
            func.sqrt(func.sum(func.col("xx"))) * func.sqrt(func.sum(func.col("yy")))
        ).alias("denominator"),
        func.count(func.col("xy")).alias("number_of_pairs"),
    )

    results = calculate_similarity.withColumn(
        "score",
        func.when(
            func.col("denominator") != 0,
            func.col("numerator") / func.col("denominator"),
        ).otherwise(0),
    ).select("movie_id_1", "movie_id_2", "score", "number_of_pairs")

    return results


def _get_movie_name(movie_names: DataFrame, movie_id: int) -> str:
    return (
        movie_names.filter(func.col("movie_id") == movie_id)
        .select("movie_name")
        .collect()[0][0]
    )


if __name__ == "__main__":
    movie_id = 50  # Default of Star Wars
    try:
        if len(sys.argv) > 1:
            movie_id = int(sys.argv[1])
        else:
            print(f"Using default movie id of {movie_id}")
    except ValueError:
        raise Exception("Movie Id argument must be an integer")

    main(movie_id=movie_id)
