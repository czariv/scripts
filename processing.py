import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, StringType
from tqdm import tqdm

spark = SparkSession.builder.appName("SEVMON").getOrCreate()
schema = StructType([
    StructField("data", StringType(), True),
    StructField("mensagem", StringType(), True),
    StructField("uid", StringType(), True),
    StructField("app", StringType(), True),
    StructField("motivo", StringType(), True),
    StructField("code", StringType(), True),
    StructField("linha", StringType(), True),
    StructField("uf", StringType(), True),
    StructField("file_path", StringType(), True)
])

combined_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)  # Empty DataFrame

for subdir, dirs, files in os.walk("/home/sevmon/2t"):
    path = Path(subdir)
    if path.parent.name == "2t":
        print(f"Saving for state {path.name}")

        for file_name in tqdm(files):
            if file_name.endswith(".dat"):
                file_path = os.path.join(subdir, file_name)  # Get the full file path

                df = spark.read \
                    .option("sep", "\t") \
                    .option("header", False) \
                    .option("inferSchema", True) \
                    .csv(file_path)

                # Add the index column
                df = df.withColumn("linha", monotonically_increasing_id())

                # Add the name of the folder to 'uf' column
                df = df.withColumn("uf", lit(str(path.name).split('_')[1].upper()))

                # Add the path of the file to 'file_path' column
                df = df.withColumn("file_path", lit(file_path))

                # Union the current DataFrame with the combined DataFrame
                combined_df = combined_df.union(df)

# Remove duplicate rows from the combined DataFrame
combined_df = combined_df.dropDuplicates()

# Save the combined DataFrame as a zipped Parquet file
combined_df.write.mode("overwrite").parquet("combined_data")