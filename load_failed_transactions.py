import pandas as pd
from google.cloud import storage
import mysql.connector
import io

# Set up Google Cloud Storage client
storage_client = storage.Client()
bucket = storage_client.bucket("gowthami-bucket")
blob = bucket.blob("output/failed_transactions/failed_transactions.csv")

# Download file content
data = blob.download_as_text()
df = pd.read_csv(io.StringIO(data))

# Connect to Cloud SQL (must allow external IP or run this in Compute Engine)
conn = mysql.connector.connect(
    host='34.46.209.230',
    user='p1-projectuser',
    password='gowthami',
    database='projectdb'
)
cursor = conn.cursor()

# Insert data
for _, row in df.iterrows():
    cursor.execute("""
        INSERT INTO failed_transactions (transaction_id, branch_id, city, amount, transaction_status, transaction_date)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, tuple(row))

conn.commit()
cursor.close()
conn.close()

print("✅ Failed transactions successfully inserted into Cloud SQL.")
