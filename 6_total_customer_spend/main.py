"""
Chapter 6

Goal:
Use dataframes to calculate total spent by each customer
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import round, asc
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

FILE_LOCATION = os.path.abspath(
    os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "../resources/customer-orders.csv"
    )
)


def main():
    _check_file_exists()

    session = SparkSession.builder.appName("CustomerSpend").getOrCreate()

    schema = StructType(
        [
            StructField(name="customer_id", dataType=IntegerType()),
            StructField(name="order_id", dataType=IntegerType()),
            StructField(name="spent", dataType=FloatType()),
        ]
    )

    orders = session.read.csv(FILE_LOCATION, schema=schema)

    grouped_by_customer = (
        orders.groupBy("customer_id")
        .sum("spent")
        .withColumnRenamed("sum(spent)", "total_spent")
    )

    formatted = grouped_by_customer.select(
        grouped_by_customer.customer_id,
        round(grouped_by_customer.total_spent, 2).alias("total_spent"),
    )

    formatted.sort(asc(grouped_by_customer.customer_id)).show(formatted.count())


def _check_file_exists():
    if not os.path.exists(FILE_LOCATION):
        raise FileNotFoundError(f"Could not find expected file at {FILE_LOCATION}")


if __name__ == "__main__":
    main()
