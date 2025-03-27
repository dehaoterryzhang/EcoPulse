# EcoPulse: Data Pipeline for Economic Data Insights

## Project Overview

**EcoPulse** is a data engineering project designed to analyze key economic indicators through an **end-to-end batch data pipeline**. The pipeline automates the ingestion, processing, and storage of economic data in a structured, query-optimized format. This solution provides seamless analysis through an interactive dashboard, enabling data-driven decision-making across industries such as finance, policy, and business strategy.

## Problem Statement

In today's fast-paced world, monitoring and analyzing key economic indicators—such as stock market, inflation, housing prices, and so on— is critical for making informed decisions in finance, policymaking, and business. However, gathering this data can often be a tedious and time-consuming task, especially when relying on manually pulling information from various sources.

**EcoPulse** automates this process by:
1. Extracting economic data from the Federal Reserve Economic Data (FRED) Python API.

2. Loading raw data into a GCS data lake.

3. Processing and transforming the data with Apache Spark.

4. Storing structured, partitioned, and clustered data in BigQuery for downstream analytics.

5. Visualizing insights through an interactive dashboard.

Here are the key economic indicators processed:

- **Financial Markets**: S&P 500 Index, 10-Year Treasury Yield, VIX (Volatility Index)
- **Interest Rates**: Federal Funds Rate
- **Inflation & Price Levels**: Consumer Price Index (CPI-U)
- **Labor Market**: Labor Force Participation Rate
- **Economic Activity**: Industrial Production Index
- **Housing Market**: House Price Index (Case-Shiller National Home Price Index)

## Architecture and Highlights

![EcoPulse Architecture](images/EcoPulse_Architecture_png.png)

### Solution Highlights:

✅ **Cloud-Native & Infrastructure as Code (IaC)**: built on GCP, leveraging cloud-based services to ensure scalability and reliability. **Terraform** is used as IaC tool to automate cloud resource provisioning.

✅ **Batch Data Pipeline with Workflow Orchestration**: follows a structured batch processing workflow, orchestrated using **Kestra** to automate data ingestion and load to GCS data lake storage.

✅ **Optimized Data Warehouse Design**: The data warehouse is structured in **BigQuery**, where tables are partitioned and clustered to enhance query performance and minimize costs. This ensures efficient data retrieval for analytical use cases.

✅ **Transformations with Spark**: The data undergoes transformations using **Apache Spark**, ensuring efficient handling of large datasets and preparing them for downstream analytics.

✅ **Interactive Dashboard**: built using Looker Studio to provide insights into economic trends.


## 🚀 Terraform Infrastructure Setup

The EcoPulse project leverages **Terraform** to provision and manage cloud resources efficiently. This ensures infrastructure as code (IaC) best practices, making deployments reproducible, scalable, and maintainable.


### 🛠️ Prerequisites
- [Terraform](https://developer.hashicorp.com/terraform/downloads)
- Google Cloud SDK (`gcloud`)  
- A GCP Service Account with the required permissions  

### 🔑 Authentication
Authenticate using your **GCP Service Account**:
```bash
source Terraform/setup.sh
```

### 🚀 Deploying Infrastructure
```bash
cd Terraform
./deploy.sh
```

## 🔄 Ingestion - Kestra & Python

EcoPulse leverages **Kestra** for workflow orchestration, automating the data pipeline to load the data through FRED API in a Python script and upload to GCS data lake.

### 🛠️ Prerequisites
- [Docker](https://www.docker.com/products/docker-desktop/) & [Docker Compose](https://docs.docker.com/compose/install/)
- A FRED API Key (Get one from the [FRED website](https://fredaccount.stlouisfed.org/apikeys))

### 📌 Spinning Up Kestra
Run the following command to start Kestra using Docker Compose:

```bash
cd Kestra
docker-compose up
```
After Kestra UI is loaded, we can proceed to run two flows:

### 🔑 set_kv: Setting Up Environment Variables

The flow ([set_kv.yaml](Kestra/set_kv.yaml)) configure the following project variables:
- `gcp_project_id`
- `gcp_location`
- `gcp_bucket_name`

### ⚡ data_load_gcs: Running the Ingestion

The [data_load_gcs.yaml](Kestra/data_load_gcs.yaml) flow orchestrates the entire ingestion pipeline:

- Fetches data from the FRED API in Python and saves as CSV files
- Uploads the CSVs to the specified GCS bucket.
- Purges temporary files to keep the workflow clean.

Kestra Topology Diagram:
<p align="center"> <img src="images/Kestra Flow Diagram.png" height="300" />
</p>

