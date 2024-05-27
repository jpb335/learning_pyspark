"""
Chapter 5

Goal:
Count the words in a text file using dataframes
"""

import os.path

from pyspark.sql import SparkSession
from pyspark.sql import functions


FILE_LOCATION = os.path.abspath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "../resources/words.txt")
)


def main():
    _check_file_exists()

    spark_session = SparkSession.builder.appName("WordCountDF").getOrCreate()

    input_df = spark_session.read.text(FILE_LOCATION)

    words = input_df.select(
        functions.explode(functions.split(input_df.value, "\\W+")).alias("word")
    )

    lowered = words.select(functions.lower(words.word).alias("word"))

    counts = lowered.groupBy("word").count().sort(functions.desc("count"))

    counts.show(counts.count())


def _check_file_exists():
    if not os.path.exists(FILE_LOCATION):
        raise FileNotFoundError(f"Could not find expected file at {FILE_LOCATION}")


if __name__ == "__main__":
    main()
