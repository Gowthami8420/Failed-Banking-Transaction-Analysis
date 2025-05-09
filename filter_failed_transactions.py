from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("FilterFailedTransactions").getOrCreate()

# 🔧 Fix: Include header=True
df = spark.read.csv("gs://gowthami-bucket/raw_data/*.csv", header=True, inferSchema=True)

# Filter failed transactions
failed_df = df.filter(df["transaction_status"] == "FAILED")

# Save the filtered result
failed_df.write.csv("gs://gowthami-bucket/output/failed_transactions/", header=True, mode="overwrite")

spark.stop()


