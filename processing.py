import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("SEVMON").getOrCreate()
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("log_level", StringType(), True),
    StructField("process_id", StringType(), True),
    StructField("component", StringType(), True),
    StructField("message", StringType(), True)
])

for subdir, dirs, files in os.walk("/home/sevmon/2t"):
    path = Path(subdir)
    if path.parent.name == "2t":
        print(f"Saving for state {path.name}")
        df = spark.read \
            .option("sep", "\t") \
            .option("header", False) \
            .option("inferSchema", True) \
            .schema(schema) \
            .csv(f"{path}/*.dat")
        df.write.parquet(f"{path}/parquet")