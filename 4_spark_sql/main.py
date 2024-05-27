"""
Chapter 4

Goal:
Shows the use of Spark SQL

This will read in a file to a Spark SQL view and show the average friends per age group using Spark SQL
"""

import os.path

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

FILE_LOCATION = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), "../resources/fakefriends.csv"
)


def main():
    conf = SparkConf().setMaster("local").setAppName("Spark_SQL")
    sc = SparkContext(conf=conf)

    session = SparkSession(sparkContext=sc)

    friends_schema = StructType(
        [
            StructField(name="idx", dataType=IntegerType()),
            StructField(name="name", dataType=StringType()),
            StructField(name="age", dataType=IntegerType()),
            StructField(name="friend_count", dataType=IntegerType()),
        ]
    )

    friends_df = session.read.csv(FILE_LOCATION, schema=friends_schema)

    friends_df.createOrReplaceTempView("friends")

    resulting_df = session.sql(
        """
        SELECT *
        FROM friends
    """
    )

    resulting_df.show()

    average_friends_by_age = session.sql(
        """
        SELECT age,
            SUM(friend_count) as total_friends,
            CAST(AVG(friend_count) AS INT) as average_friends
        FROM friends
        GROUP BY age
        ORDER BY age ASC;
    """
    )

    average_friends_by_age.show(n=average_friends_by_age.count())


if __name__ == "__main__":
    main()
