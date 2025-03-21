#!/bin/bash

set -e  # Exit on error

echo "🔹 Initializing Terraform..."
terraform init

echo "🔹 Creating Terraform plan..."
terraform plan

echo "🔹 Applying Terraform configuration..."
terraform apply

echo "✅ Deployment complete!"
