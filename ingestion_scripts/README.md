# Cloud Function Ingestion Script

This folder contains the Python script (`main.py`) used for the data ingestion part of this project.

## Deployment

This script is not executed directly from this repository. It is deployed as a **Google Cloud Function** and is triggered by file uploads to the `ms-geolife-raw-data-eu` GCS bucket.

The code is stored here for version control and to document the complete end-to-end data pipeline.