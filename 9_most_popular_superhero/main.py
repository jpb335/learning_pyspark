"""
Chapter 9

Goal:
Advanced usage of processing text files

Show the most popular superhero (by number of connections)
"""

import codecs
import csv
import os
from typing import Dict

from pyspark.sql import SparkSession, functions
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType

SUPERHERO_GRAPH_FILE_LOCATION = os.path.abspath(
    os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "../resources/marvel_graph"
    )
)

SUPERHERO_NAME_FILE_LOCATION = os.path.abspath(
    os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "../resources/marvel_names"
    )
)


def main():
    session = SparkSession.builder.appName("MostPopularMovie").getOrCreate()

    superhero_names_schema = StructType(
        [
            StructField("superhero_id", dataType=IntegerType()),
            StructField("superhero_name", dataType=StringType()),
        ]
    )

    superhero_graph = session.read.text(SUPERHERO_GRAPH_FILE_LOCATION)
    superhero_names = session.read.csv(
        SUPERHERO_NAME_FILE_LOCATION, sep=" ", schema=superhero_names_schema
    )

    connections = superhero_graph.withColumn(
        "superhero_id", functions.split(functions.col("value"), " ")[0]
    ).withColumn(
        "connections_count",
        functions.size(functions.split(functions.col("value"), " ")) - 1,
    )

    grouped = connections.groupBy("superhero_id").agg(
        functions.sum("connections_count").alias("connections_count")
    )

    most_popular = grouped.sort(functions.desc("connections_count")).first()

    most_popular_name = (
        superhero_names.filter(
            functions.col("superhero_id") == most_popular["superhero_id"]
        )
        .select("superhero_name")
        .first()
    )

    print(
        f"{most_popular_name[0]} is the most popular with {most_popular['connections_count']} connections"
    )


if __name__ == "__main__":
    main()
