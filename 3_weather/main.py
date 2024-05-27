import os
from typing import Tuple
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, DataFrameReader
from pyspark.sql.types import FloatType

FILE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "1800.csv")


MIN_WEATHER_TAG = "TMIN"
MAX_WEATHER_TAG = "TMAX"

def main():
    conf = SparkConf().setMaster("local").setAppName("Min_Weather")
    sc = SparkContext(conf=conf)

    print("Running min weather")
    _run_rdd(spark_context=sc, weather_tag=MIN_WEATHER_TAG)
    _run_dataframe(spark_context=sc, weather_tag=MIN_WEATHER_TAG)

    print("Running max weather")
    _run_rdd(spark_context=sc, weather_tag=MAX_WEATHER_TAG)
    _run_dataframe(spark_context=sc, weather_tag=MAX_WEATHER_TAG)


def _run_rdd(spark_context: SparkContext, weather_tag: str):
    print("### RDD ###")

    lines = spark_context.textFile(FILE_PATH)

    parsed = lines.map(_parse_line)

    min_temps = parsed.filter(lambda x: weather_tag in x[1])
    station_temps = min_temps.map(lambda x: (x[0], x[2]))

    if weather_tag == MIN_WEATHER_TAG:
        min_temps = station_temps.reduceByKey(lambda x, y: min(x, y))

        results = min_temps.collect()
    elif weather_tag == MAX_WEATHER_TAG:
        max_temps = station_temps.reduceByKey(lambda x, y: max(x, y))

        results = max_temps.collect()
    else:
        raise ValueError(f"Unexpected weather tag: {weather_tag}")

    for each in results:
        print(f"{each[0]}:  {each[1]:.2f}")

    print("### End RDD ###")


def _run_dataframe(spark_context: SparkContext, weather_tag: str):
    print("### Dataframe ###")

    session = SparkSession(sparkContext=spark_context)

    dataframe = DataFrameReader(spark=session).csv(FILE_PATH, sep=",")

    dataframe = dataframe.withColumnRenamed(dataframe.columns[0], "Station Name")
    dataframe = dataframe.withColumnRenamed(dataframe.columns[3], "Temp")
    dataframe = dataframe.withColumn(
        "Temp", dataframe["Temp"].cast(dataType=FloatType())
    )

    dataframe = dataframe.withColumn(
        dataframe.columns[3], dataframe["Temp"] * 0.1 * (9.0 / 5.0) + 32.0
    )

    filtered = dataframe.filter(dataframe[dataframe.columns[2]] == weather_tag)

    if weather_tag == MIN_WEATHER_TAG:
        minimum = (
            filtered.groupBy(dataframe.columns[0])
            .min(dataframe.columns[3])
            .withColumnRenamed("min(Temp)", "Minimum Temp")
        )

        minimum.show()
    elif weather_tag == MAX_WEATHER_TAG:
        minimum = (
            filtered.groupBy(dataframe.columns[0])
            .max(dataframe.columns[3])
            .withColumnRenamed("max(Temp)", "Maximum Temp")
        )

        minimum.show()
    else:
        raise ValueError(f"Unexpected weather tag: {weather_tag}")

    print("### End Dataframe ###")


def _parse_line(line: str) -> Tuple[str, str, float]:
    fields = line.split(",")

    station_id = fields[0]
    entry_type = fields[2]
    temperature_celcius = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0

    return station_id, entry_type, temperature_celcius


if __name__ == "__main__":
    main()
