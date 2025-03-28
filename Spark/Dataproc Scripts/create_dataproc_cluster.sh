#!/bin/bash

# Set variables
PROJECT_ID="animated-scope-447904-d6"
CLUSTER_NAME="ecopulse-cluster"
REGION="us-west1"
IMAGE_VERSION="2.0-debian10"
MACHINE_TYPE="n1-standard-2"
DISK_SIZE="30GB"

# Authenticate with GCP (if not already authenticated)
gcloud auth application-default login

# Create the Dataproc cluster
gcloud dataproc clusters create "$CLUSTER_NAME" \
    --region="$REGION" \
    --single-node \
    --master-machine-type="$MACHINE_TYPE" \
    --master-boot-disk-size="$DISK_SIZE" \
    --image-version="$IMAGE_VERSION" \
    --project="$PROJECT_ID"

echo "✅ Dataproc cluster '$CLUSTER_NAME' created successfully!"
