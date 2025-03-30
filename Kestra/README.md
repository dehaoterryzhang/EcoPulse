### 🛠️ Prerequisites
- [Docker](https://www.docker.com/products/docker-desktop/) & [Docker Compose](https://docs.docker.com/compose/install/)
- A FRED API Key (Get one from the [FRED website](https://fredaccount.stlouisfed.org/apikeys))
- A GCP Service Account with the required permissions 

### 📌 Start Kestra
Run the following command to start Kestra using Docker Compose:

```bash
cd Kestra
docker-compose up
```
After Kestra UI is loaded, we can run two following flows:

### 🔑 set_kv: Configures Environment Variables

The flow ([set_kv.yaml](Kestra/set_kv.yaml)) configure the following project variables:
- `gcp_project_id`
- `gcp_location`
- `gcp_bucket_name`

### ⚡ data_load_gcs: Fetches data, saves as CSV, and uploads to GCS

The [data_load_gcs.yaml](Kestra/data_load_gcs.yaml) flow orchestrates the entire ingestion pipeline:

- Fetches data from the FRED API in Python and saves as CSV files
- Uploads the CSVs to the specified GCS bucket.
- Purges temporary files to keep the workflow clean.

Kestra Topology Diagram:
<p align="center"> <img src="../images/Kestra Flow Diagram.png" height="300" />
</p>