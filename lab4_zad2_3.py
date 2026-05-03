from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, to_json, struct, lit, round as _round
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

alerts = (
    df
    .filter(col("amount") > 3000)
    .withColumn("ratio", _round(col("amount") / 400.0, 2))
    .select(
        to_json(
            struct(
                "tx_id",
                "user_id",
                "amount",
                "ratio", 
                "store",
                "category",
                col("timestamp").cast("string").alias("timestamp"),
                lit("HIGH").alias("alert_level"),
            )
        ).alias("value")
    )
)

query = (
    alerts.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("topic", "alerts")
    .option("checkpointLocation", "/tmp/checkpoints_alerts")
    .outputMode("append")
    .start()
)

query.awaitTermination()

#python producer.py
#spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0-preview2 lab4_zad2_3.py
#kafka-console-consumer.sh   --bootstrap-server broker:9092   --topic alerts   --from-beginning

#ZADANIE 3
#Po zatrzymaniu producenta dane przestają napływać, ale przez około 2 minuty wyniki nadal są widoczne, ponieważ należą do aktywnych okien.
#Po zamknięciu okien (po czasie okna i watermarka) wyniki przestają się pojawiać, a strumień nie generuje nowych danych.