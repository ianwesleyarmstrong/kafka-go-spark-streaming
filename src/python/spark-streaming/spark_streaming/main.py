from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Column
from pyspark.sql.window import Window
from pydantic_settings import BaseSettings
import sys


class SparkSettings(BaseSettings):
    KAFKA_BROKERS: str = "redpanda-0:9092"
    KAFKA_TOPIC: str = "kraken-trades"
    SPARK_VERSION: str = "3.4.1"
    SCALA_VERSION: str = "2.12"


spark_settings = SparkSettings()

SPARK_PACKAGES = [
    f"org.apache.spark:spark-sql-kafka-0-10_{spark_settings.SCALA_VERSION}:{spark_settings.SPARK_VERSION}",
    "org.apache.kafka:kafka-clients:3.2.1",
]


def windowed_z_score(window: Window, col: Column) -> Column:
    avg = F.avg(col).over(window)
    avg_squared = F.avg(col * col).over(window)
    standard_deviation = F.sqrt(avg_squared - (avg * avg))
    return (col - avg) / standard_deviation


spark: SparkSession = (
    SparkSession.builder.master("local")
    .appName("Testing")
    .config("spark.jars.packages", ",".join(SPARK_PACKAGES))
    .getOrCreate()
)

stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", spark_settings.KAFKA_BROKERS)
    .option("subscribe", spark_settings.KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

df = stream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

trade_message_schema = T.StructType(
    fields=[
        T.StructField("Ticker", T.StringType()),
        T.StructField("Price", T.DoubleType()),
        T.StructField("Volume", T.DoubleType()),
        T.StructField("Time", T.DoubleType()),
        T.StructField("Side", T.StringType()),
        T.StructField("OrderType", T.StringType()),
        T.StructField("Misc", T.StringType()),
    ]
)

extracted = df.withColumn(
    "jsonData", F.from_json(F.col("value"), trade_message_schema)
).select("jsonData.*")

date_format = "yyyy-MM-dd HH:mm:ss"
cleaned = (
    extracted.withColumn(
        "timestamp",
        F.to_timestamp(
            F.from_unixtime(F.col("time"), format=date_format), format=date_format
        ),
    )
    .withColumnRenamed("Ticker", "ticker")
    .withColumnRenamed("Price", "price")
    .withColumnRenamed("Volume", "volume")
    .withColumnRenamed("Side", "side")
    .withColumnRenamed("OrderType", "order_type")
    .withColumnRenamed("Misc", "misc")
)

watermark = 24

price_window = Window().partitionBy(["ticker", "side"]).rowsBetween(-sys.maxsize, sys.maxsize)

summary = (
    cleaned.withWatermark("timestamp", f"{watermark} hours")
    .withColumn("exchange", F.lit("kraken"))
    .groupBy(
        F.window(F.col("timestamp"), f"{watermark} hours"),
        F.col("ticker"),
        F.col("side"),
    )
    .agg(
        F.sum("volume").alias("total_volume"),
        F.sum(F.col("price") * F.col("volume")).alias("vwap"),
        F.avg("price").alias(f"avg_price_{watermark}"),
    )
    .withColumn("price_z_score", windowed_z_score(price_window, F.col("price")))
)

summary.writeStream.format("console").outputMode("complete").start().awaitTermination()
