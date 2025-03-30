### Provisioned GCP Resources
- GCS Bucket
- Google BigQuery

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

### Clean Up Resources
```bash
terraform destroy
```