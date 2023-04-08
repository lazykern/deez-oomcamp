from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_FHV_TOPIC = "fhv_tripdata"
KAFKA_GREEN_TOPIC = "green_tripdata"


FHV_SCHEMA = StructType(
    [
        StructField("dispatching_base_num", StringType(), True),
        StructField("pickup_datetime", StringType(), True),
        StructField("dropOff_datetime", StringType(), True),
        StructField("PULocationID", StringType(), True),
        StructField("DOLocationID", StringType(), True),
        StructField("SR_Flag", StringType(), True),
        StructField("Affiliated_base_number", StringType(), True),
    ]
)

GREEN_SCHEMA = StructType(
    [
        StructField("VendorID", StringType(), True),
        StructField("lpep_pickup_datetime", StringType(), True),
        StructField("lpep_dropoff_datetime", StringType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("RatecodeID", StringType(), True),
        StructField("PULocationID", StringType(), True),
        StructField("DOLocationID", StringType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("trip_distance", FloatType(), True),
        StructField("fare_amount", FloatType(), True),
        StructField("extra", FloatType(), True),
        StructField("mta_tax", FloatType(), True),
        StructField("tip_amount", FloatType(), True),
        StructField("tolls_amount", FloatType(), True),
        StructField("ehail_fee", StringType(), True),
        StructField("improvement_surcharge", FloatType(), True),
        StructField("total_amount", FloatType(), True),
        StructField("payment_type", IntegerType(), True),
        StructField("trip_type", IntegerType(), True),
        StructField("congestion_surcharge", StringType(), True),
    ]
)
