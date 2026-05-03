from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window
from pyspark.sql.functions import sum as _sum, count, round as _round
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0-preview2"

spark = (
    SparkSession.builder
    .appName("Lab4-Kafka")
    .config("spark.jars.packages", KAFKA_PACKAGE)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

tx_schema = StructType([
    StructField("tx_id",     StringType()),
    StructField("user_id",   StringType()),
    StructField("amount",    DoubleType()),
    StructField("store",     StringType()),
    StructField("category",  StringType()),
    StructField("timestamp", StringType()),
])

kafka_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("subscribe", "transactions")
    .load()
)

df = (
    kafka_raw
    .select(from_json(col("value").cast("string"), tx_schema).alias("tx"))
    .select("tx.*")
    .withColumn("timestamp", to_timestamp("timestamp"))
)

windowed = (
    df
    .withWatermark("timestamp", "2 minutes")
    .groupBy(
        window("timestamp", "2 minutes", "1 minute"),
        "store"
    )
    .agg(
        count("tx_id").alias("liczba_tx"),
        _round(_sum("amount"), 2).alias("suma_PLN")
    )
)

query = (
    windowed.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .start()
)

query.awaitTermination()

#python producer.py
#spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0-preview2 lab4_zad1.py