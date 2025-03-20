#!/bin/bash

set -e  # Exit on error

# Define variables
TF_VAR_FILE="variables.tf"

echo "🔹 Initializing Terraform..."
terraform init

echo "🔹 Validating Terraform configuration..."
terraform validate

echo "🔹 Creating Terraform plan..."
terraform plan -var-file=$TF_VAR_FILE -out=tfplan

echo "🔹 Applying Terraform configuration..."
terraform apply tfplan

echo "✅ Deployment complete!"
