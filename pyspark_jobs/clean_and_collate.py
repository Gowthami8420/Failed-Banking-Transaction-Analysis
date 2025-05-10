from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CleanAndCollate").getOrCreate()

# Read all CSV files from the Cloud Storage bucket
df = spark.read.option("header", True).csv("gs://gowthami-bucket/raw_data/*.csv")


# Drop rows with any null or blank values
clean_df = df.dropna()

# Save the cleaned data back to Cloud Storage
clean_df.write.mode("overwrite").csv("gs://gowthami-bucket/cleaned_data/")





