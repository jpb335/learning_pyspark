import os
import collections

from pyspark import SparkConf, SparkContext
from pyspark.sql import DataFrameReader, SparkSession

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf=conf)

file_location = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "../ml-100k/u.data"
)

print("### RDD ###")

lines = sc.textFile(file_location)

ratings = lines.map(lambda line: line.split()[2])
result = ratings.countByValue()

sorted_results = collections.OrderedDict(sorted(result.items(), reverse=True))
for key, value in sorted_results.items():
    print(f"{key} stars: {value}")

print("### END RDD ###\n\n")

print("### DATAFRAME ###")

session = SparkSession(sc)

lines_df = DataFrameReader(spark=session).csv(file_location, sep="\t")
ratings_df = (
    lines_df.groupBy(lines_df.columns[2])
    .count()
    .withColumnRenamed(lines_df.columns[2], "Rating")
)

ratings_df.sort("Rating", ascending=False).show()

print("### END DATAFRAME ###")
