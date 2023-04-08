from typing import List

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType
from settings import (
    FHV_SCHEMA,
    GREEN_SCHEMA,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_FHV_TOPIC,
    KAFKA_GREEN_TOPIC,
)

spark: SparkSession


def read_from_kafka(consume_topic: str):
    df_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", consume_topic)
        .option("startingOffsets", "earliest")
        .load()
    )
    return df_stream


def parse_df(df: DataFrame, schema: StructType):
    assert df.isStreaming, "df is not a streaming DataFrame"
    df = df.selectExpr("CAST(value AS STRING)")
    col = F.split(df["value"], ", ")

    for idx, field in enumerate(schema):
        df = df.withColumn(field.name, col.getItem(idx).cast(field.dataType))
    return df.select([field.name for field in schema])


def sink_console(
    df: DataFrame, output_mode: str = "complete", processing_time: str = "5 seconds"
):
    write_query = (
        df.writeStream.outputMode(output_mode)
        .trigger(processingTime=processing_time)
        .format("console")
        .option("truncate", False)
        .start()
    )
    return write_query


def sink_kafka(
    topic: str,
    df: DataFrame,
    output_mode: str = "complete",
    processing_time: str = "5 seconds",
):
    write_query = (
        df.writeStream.outputMode(output_mode)
        .trigger(processingTime=processing_time)
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", topic)
        .option("checkpointLocation", "/tmp/checkpoint")
        .start()
    )
    return write_query


def main():
    global spark
    spark = SparkSession.builder.appName("tripdata").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    fhv_df_stream = read_from_kafka(KAFKA_FHV_TOPIC)
    green_df_stream = read_from_kafka(KAFKA_GREEN_TOPIC)

    fhv_df = parse_df(fhv_df_stream, FHV_SCHEMA)
    green_df = parse_df(green_df_stream, GREEN_SCHEMA)

    fhv_df.printSchema()
    green_df.printSchema()

    sink_console(fhv_df, output_mode="append")
    sink_console(green_df, output_mode="append")

    fhv_pu_du = fhv_df.select("PULocationID", "DOLocationID")
    green_pu_du = green_df.select("PULocationID", "DOLocationID")

    sink_kafka("rides", fhv_pu_du, output_mode="append")
    sink_kafka("rides", green_pu_du, output_mode="append")

    rides_df = read_from_kafka("rides")

    rides_df.printSchema()

    rides_df = parse_df(rides_df, StructType([FHV_SCHEMA[3], FHV_SCHEMA[4]]))

    rides_gb = rides_df.groupBy("PULocationID").count()

    sink_console(rides_gb, output_mode="complete")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
