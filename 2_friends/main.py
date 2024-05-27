import csv
import random
import os
from typing import Tuple
import uuid

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, DataFrameReader
from pyspark.sql.types import IntegerType
from pyspark.sql import functions


FRIENDS_FILE = os.path.join(os.path.dirname(os.path.realpath(__file__)), "friends.csv")


def main():
    try:
        _generate_file()

        conf = SparkConf().setMaster("local").setAppName("Friends")
        sc = SparkContext(conf=conf)

        _run_rdd(spark_context=sc)

        _run_df(spark_context=sc)
    finally:
        _remove_file()


def _run_rdd(spark_context: SparkContext):
    print("### RDD ###")

    lines = spark_context.textFile(FRIENDS_FILE)

    rdd = lines.map(_parse_line)

    totals_by_age = rdd.mapValues(lambda x: (x, 1)).reduceByKey(
        lambda x, y: (x[0] + y[0], x[1] + y[1])
    )

    averages_by_age = totals_by_age.mapValues(lambda x: int(round(x[0] / x[1])))

    results = averages_by_age.collect()

    print(f"Age    | Friend Count")
    for each in sorted(results, key=lambda x: x[0]):
        print(f"{each[0]}     | {each[1]}")

    print("### Finish RDD ###")


def _run_df(spark_context: SparkContext):
    print("### Dataframe ###")

    session = SparkSession(sparkContext=spark_context)

    lines_df = DataFrameReader(spark=session).csv(FRIENDS_FILE, sep=",")

    lines_df = lines_df.withColumn(
        "Age", lines_df[lines_df.columns[2]].cast(IntegerType())
    )
    lines_df = lines_df.withColumn(
        "Friends", lines_df[lines_df.columns[3]].cast(IntegerType())
    )

    total_friends = (
        lines_df.groupBy("Age")
        .sum("Friends")
        .withColumnRenamed("sum(Friends)", "Friends_Count")
    )
    total_age = lines_df.groupBy("Age").count().withColumnRenamed("count", "Age_Count")

    merged = total_friends.join(total_age, on="Age")

    merged = merged.withColumn(
        "Average Friends",
        functions.round(merged["Friends_Count"] / merged["Age_Count"]).cast(
            dataType=IntegerType()
        ),
    )

    merged = merged[["Age", "Average Friends"]]

    merged.sort("Age").show(merged.count())

    print("### End Dataframe ###")


def _parse_line(line: str) -> Tuple[int, int]:
    fields = line.split(",")

    age = int(fields[2])
    friend_count = int(fields[3])

    return age, friend_count


def _generate_file():
    with open(FRIENDS_FILE, "w") as fh:
        writer = csv.writer(fh, delimiter=",", quoting=csv.QUOTE_MINIMAL)
        for idx in range(500):
            # Rows of [row number, unique id, age, friend count]
            writer.writerow(
                [idx, str(uuid.uuid4()), random.randint(18, 99), random.randint(1, 200)]
            )


def _remove_file():
    if os.path.exists(FRIENDS_FILE):
        os.remove(FRIENDS_FILE)


if __name__ == "__main__":
    main()
