#!/bin/bash
set -euo pipefail

MICROCEPH_BUCKET="testing"

# Detect host IP (same logic as python fixture)
HOST_IP=$(hostname -i)

echo "Detected host IP: $HOST_IP"

echo "Installing microceph"
sudo snap install microceph

echo "Bootstrapping cluster"
sudo microceph cluster bootstrap

echo "Adding loop disks"
sudo microceph disk add loop,4G,3

echo "Enabling RGW (no TLS)"
sudo microceph enable rgw

echo "Waiting for RGW to become ready..."
sleep 10

echo "Creating RGW user"
USER_JSON=$(sudo microceph.radosgw-admin user create \
  --uid test \
  --display-name test)

ACCESS_KEY=$(echo "$USER_JSON" | jq -r '.keys[0].access_key')
SECRET_KEY=$(echo "$USER_JSON" | jq -r '.keys[0].secret_key')

echo "Creating S3 bucket"
for _ in {1..3}; do
    if AWS_ACCESS_KEY_ID="$ACCESS_KEY" \
       AWS_SECRET_ACCESS_KEY="$SECRET_KEY" \
       aws --endpoint-url "http://$HOST_IP" \
       s3api create-bucket \
       --bucket "$MICROCEPH_BUCKET"
    then
        break
    fi
    echo "Retrying bucket creation..."
    sleep 2
done

S3_SERVER_URL="http://$HOST_IP"
S3_ACCESS_KEY="$ACCESS_KEY"
S3_SECRET_KEY="$SECRET_KEY"
S3_BUCKET="$MICROCEPH_BUCKET"
S3_REGION="default"

export S3_SERVER_URL
export S3_ACCESS_KEY
export S3_SECRET_KEY
export S3_BUCKET
export S3_REGION

echo "S3_ENDPOINT=$S3_SERVER_URL S3_ACCESS_KEY=$S3_ACCESS_KEY S3_SECRET_KEY=$S3_SECRET_KEY S3_BUCKET=$S3_BUCKET S3_REGION=$S3_REGION"
