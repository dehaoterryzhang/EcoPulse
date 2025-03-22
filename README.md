# EcoPulse: Economic Data Insights with Automation

## Problem Statement

In today's fast-paced world, monitoring and analyzing key economic indicators like GDP, inflation, housing prices, and stock market trends is critical for making informed decisions in areas such as finance, policy, and business strategy. However, gathering this data can often be a tedious and time-consuming task, especially when relying on manually pulling information from various sources.

**EcoPulse** aims to solve this problem by automating the extraction, loading into cloud data warehouse and visualization of economic data, including critical indicators such as:

- **Gross Domestic Product (GDP)**
- **Consumer Price Index (CPI)**
- **Federal Funds Rate**
- **Housing Price Index**
- **S&P 500 Index**

## 🚀 Terraform Infrastructure Setup

The EcoPulse project leverages **Terraform** to provision and manage cloud resources efficiently. This ensures infrastructure as code (IaC) best practices, making deployments reproducible, scalable, and maintainable.


### 🛠️ Prerequisites
Before running Terraform, ensure you have the following installed:
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

