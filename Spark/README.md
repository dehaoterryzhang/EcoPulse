### 🛠️ Prerequisites
Depending on whether you want to use local vs cloud setup, ensure you have the following installed:

#### Running Locally:
- Python 3.12 (or your preferred version)
- Apache Spark

#### Running through GCP Dataproc Cluster:
- Google Cloud SDK (`gcloud`)
- A service Account with the required permissions
- Required JARs for Spark on Dataproc:
  - gcs-connector-hadoop3-latest.jar
  - spark-bigquery-with-dependencies_2.12-0.24.0.jar

### 🔄 Transformation Steps
The Spark transformation job performs the following steps:
1. Load raw data from GCS into Spark dataframe
2. Filter each dataframe to last 10 years of economic data
3. For the daily level series (`SP500`, `DGS10`, `VIXCLS`, `EFFR`), merge using the date column.
4. Add a categorical column that indicates the level of daily change on the `SP500` index.
5. Add `Month`, `Year` and `Year-Month` columns.
6. Similarly, process the monthly level series (Step 3 and 5).
7. Merge daily and monthly data on `Year-Month` for final processed table.
8. Load transformed data into BigQuery as the data warehouse.

![Transform Steps](images/Transform_Diagram.png)

### 🚀 Running the PySpark Job through Dataproc Cluster

**Step 1**: Create a dataproc cluster through [create_dataproc_cluster.sh](Spark/Dataproc%20Scripts/create_dataproc_cluster.sh):

```bash
cd Spark/Dataproc Scripts
chmod +x create_dataproc_cluster.sh
./create_dataproc_cluster.sh
```

**Step 2**: Submit Spark job to Dataproc through [submit_dataproc_job.sh](Spark/Dataproc%20Scripts/submit_dataproc_job.sh):

```bash
chmod +x submit_dataproc_job.sh
./submit_dataproc_job.sh
```