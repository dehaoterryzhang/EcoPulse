#!/bin/bash

# Variables
CLUSTER_NAME="ecopulse-cluster"
REGION="us-west1"
PYSPARK_SCRIPT="gs://animated-scope-447904-d6-ecopulse-bucket/code/transform_dataproc.py"

# Submit PySpark Job to Dataproc
gcloud dataproc jobs submit pyspark "$PYSPARK_SCRIPT" \
    --cluster="$CLUSTER_NAME" \
    --region="$REGION" \

echo "PySpark job submitted successfully to cluster: $CLUSTER_NAME"
