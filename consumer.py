import os

os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime, to_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType

spark = (
    SparkSession.builder.appName("FraudDetectionToPostgres")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.3",
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

schema = (
    StructType()
    .add("id", StringType())
    .add("amount", DoubleType())
    .add("location", StringType())
    .add("timestamp", DoubleType())
)

df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "transactions")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

transactions = (
    df.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
)

fraud_transactions = (
    transactions.filter(col("amount") > 10000)
    .withColumn(
        "event_time", to_timestamp(from_unixtime(col("timestamp").cast("long")))
    )
    .select(
        col("id").alias("transaction_id"),
        col("amount"),
        col("location"),
        col("event_time"),
    )
)


def write_to_postgres(batch_df, batch_id):
    if batch_df.limit(1).count() == 0:
        return

    batch_df.write.format("jdbc").option(
        "url", "jdbc:postgresql://localhost:5432/fraud_db"
    ).option("dbtable", "fraud_transactions").option("user", "postgres").option(
        "password", "postgres"
    ).option("driver", "org.postgresql.Driver").mode("append").save()

    print(f"Batch {batch_id} written to PostgreSQL")


query = (
    fraud_transactions.writeStream.foreachBatch(write_to_postgres)
    .option("checkpointLocation", "checkpoints/postgres_fraud")
    .start()
)
query.awaitTermination()
