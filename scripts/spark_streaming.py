import os
import time
os.environ["HADOOP_HOME"]         = "C:\\hadoop"
os.environ["PYSPARK_PYTHON"]      = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import requests

INFLUX_URL    = "http://localhost:8086"
INFLUX_TOKEN  = "my-super-secret-token"
INFLUX_ORG    = "smartgrid"
INFLUX_BUCKET = "energy"

schema = StructType([
    StructField("timestamp",      StringType()),
    StructField("power",          DoubleType()),
    StructField("voltage",        DoubleType()),
    StructField("current",        DoubleType()),
    StructField("reactive_power", DoubleType()),
    StructField("apparent_power", DoubleType()),
    StructField("power_factor",   DoubleType()),
    StructField("sub_meter_1",    DoubleType()),
    StructField("sub_meter_2",    DoubleType()),
    StructField("sub_meter_3",    DoubleType()),
    StructField("frequency",      DoubleType()),
    StructField("temperature_c",  DoubleType()),
    StructField("humidity_pct",   DoubleType()),
    StructField("anomaly",        IntegerType()),
    StructField("co2_kg",         DoubleType()),
    StructField("cost_usd",       DoubleType()),
    StructField("meter_id",       StringType()),
    StructField("building_type",  StringType()),
    StructField("tariff_zone",    StringType()),
    StructField("grid_region",    StringType()),
])

spark = SparkSession.builder \
    .appName("SmartGridStreaming") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.streaming.checkpointLocation", "C:/tmp/spark-checkpoint") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("Spark session created successfully.")

raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "energy_stream") \
    .option("startingOffsets", "latest") \
    .load()

df = raw.select(
    from_json(col("value").cast("string"), schema).alias("d")
).select("d.*") \
 .withColumn("ts", to_timestamp("timestamp"))

# ── Anomaly detection: use dataset flag + threshold rule
POWER_THRESHOLD = 7.0

processed = df \
    .withColumn("anomaly_detected",
        when((col("anomaly") == 1) | (col("power") > POWER_THRESHOLD),
             lit(True)).otherwise(lit(False))) \
    .withColumn("anomaly_type",
        when(col("power") > 15,  lit("critical_spike"))
        .when(col("power") > 7,  lit("high_consumption"))
        .when(col("anomaly") == 1, lit("statistical_outlier"))
        .otherwise(lit("normal"))) \
    .withColumn("demand_category",
        when(col("building_type") == "industrial",  lit("high"))
        .when(col("building_type") == "commercial", lit("medium"))
        .otherwise(lit("low")))

def write_to_influx(batch_df, batch_id):
    rows = batch_df.collect()
    if not rows:
        print(f"Batch {batch_id}: no rows")
        return

    lines = []
    for row in rows:
        ts_ns = int(time.time_ns())

        # Tags (indexed, for filtering)
        tags = (f"host={row.meter_id},"
                f"building={row.building_type},"
                f"region={row.grid_region},"
                f"tariff={row.tariff_zone},"
                f"anomaly={str(row.anomaly_detected).lower()},"
                f"anomaly_type={row.anomaly_type}")

        # Fields (all numeric measurements)
        fields = (f"power={row.power},"
                  f"voltage={row.voltage},"
                  f"current={row.current},"
                  f"reactive_power={row.reactive_power},"
                  f"apparent_power={row.apparent_power},"
                  f"power_factor={row.power_factor},"
                  f"sub1={row.sub_meter_1},"
                  f"sub2={row.sub_meter_2},"
                  f"sub3={row.sub_meter_3},"
                  f"frequency={row.frequency},"
                  f"temperature={row.temperature_c},"
                  f"humidity={row.humidity_pct},"
                  f"co2_kg={row.co2_kg},"
                  f"cost_usd={row.cost_usd}")

        lines.append(f"energy,{tags} {fields} {ts_ns}")

    payload = "\n".join(lines)
    try:
        resp = requests.post(
            f"{INFLUX_URL}/api/v2/write",
            params={"org": INFLUX_ORG, "bucket": INFLUX_BUCKET, "precision": "ns"},
            headers={"Authorization": f"Token {INFLUX_TOKEN}"},
            data=payload,
            timeout=15
        )
        print(f"Batch {batch_id}: wrote {len(rows)} rows | status={resp.status_code}")
    except Exception as e:
        print(f"InfluxDB write error: {e}")

query = processed.writeStream \
    .foreachBatch(write_to_influx) \
    .outputMode("append") \
    .start()

print("Spark streaming started. Waiting for data...")
query.awaitTermination()