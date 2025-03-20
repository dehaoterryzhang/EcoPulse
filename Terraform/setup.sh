#!/bin/bash

# To replicate, define the path to your GCP service account key
GCP_KEY_PATH="$HOME/.gc/my-creds.json"

# Check if the file exists
if [ ! -f "$GCP_KEY_PATH" ]; then
  echo "❌ Error: GCP service account key not found at $GCP_KEY_PATH"
  echo "Please place your key file there or update the script."
  exit 1
fi

# Export GOOGLE_APPLICATION_CREDENTIALS
export GOOGLE_APPLICATION_CREDENTIALS="$GCP_KEY_PATH"
echo "✅ GOOGLE_APPLICATION_CREDENTIALS set successfully."

# Authenticate with GCP
gcloud auth activate-service-account --key-file="$GOOGLE_APPLICATION_CREDENTIALS"
echo "✅ Successfully authenticated with Google Cloud."

# Verify authentication
gcloud auth list
