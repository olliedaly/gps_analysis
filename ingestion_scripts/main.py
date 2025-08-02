import functions_framework
from google.cloud import storage
from google.cloud import bigquery
from google.api_core import exceptions
import os
import time

# --- Configuration ---
PROJECT_ID = "gps-analysis-467512"
DATASET_ID = "geolife_raw"
TABLE_ID = "geolife__activities"
BATCH_SIZE = 10000  # Process and insert 10,000 rows at a time
# ---------------------

storage_client = storage.Client()
bigquery_client = bigquery.Client(project=PROJECT_ID)

TABLE_SCHEMA = [
    bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("activity_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("latitude", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("longitude", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("altitude_ft", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
]

def ensure_table_exists():
    """Checks if the BigQuery table exists, and creates it if not."""
    table_ref = bigquery_client.dataset(DATASET_ID).table(TABLE_ID)
    try:
        bigquery_client.get_table(table_ref)
    except exceptions.NotFound:
        print(f"Table {TABLE_ID} not found, creating it...")
        table = bigquery.Table(table_ref, schema=TABLE_SCHEMA)
        bigquery_client.create_table(table)
        print(f"Table {TABLE_ID} created. Pausing for 10s...")
        time.sleep(10)

def insert_batch(rows_to_insert):
    """Inserts a batch of rows into BigQuery."""
    if not rows_to_insert:
        return
    
    table_ref = bigquery_client.dataset(DATASET_ID).table(TABLE_ID)
    errors = bigquery_client.insert_rows_json(table_ref, rows_to_insert)
    if not errors:
        print(f"Successfully inserted batch of {len(rows_to_insert)} rows.")
    else:
        print(f"Encountered errors on batch insert: {errors}")


@functions_framework.cloud_event
def process_geolife_file(cloud_event):
    """Triggered by a file upload, processes data in batches."""
    ensure_table_exists()

    data = cloud_event.data
    bucket_name = data["bucket"]
    file_name = data["name"]

    if not file_name.startswith("Data/") or not file_name.endswith(".plt"):
        return

    print(f"Processing file: {file_name}")

    try:
        user_id = file_name.split('/')[1]
        activity_id = os.path.splitext(os.path.basename(file_name))[0]
    except IndexError:
        return

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    
    rows_to_insert = []
    line_count = 0
    
    # Open the file as a stream instead of downloading all at once
    with blob.open("r") as f:
        # Skip the first 6 header lines
        for _ in range(6):
            next(f, None)
            
        for line in f:
            line_count += 1
            try:
                fields = line.strip().split(",")
                if len(fields) != 7:
                    continue

                row = {
                    "user_id": user_id,
                    "activity_id": activity_id,
                    "latitude": float(fields[0]),
                    "longitude": float(fields[1]),
                    "altitude_ft": float(fields[3]),
                    "timestamp": f"{fields[5]} {fields[6]}",
                }
                rows_to_insert.append(row)

                # When the batch is full, insert it and clear the list
                if len(rows_to_insert) >= BATCH_SIZE:
                    insert_batch(rows_to_insert)
                    rows_to_insert = [] # Reset for the next batch

            except (ValueError, IndexError) as e:
                print(f"Skipping malformed line {line_count} in {file_name}: {e}")

    # Insert any remaining rows in the final batch
    if rows_to_insert:
        insert_batch(rows_to_insert)