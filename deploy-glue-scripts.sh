#!/bin/bash

# Get the bucket name from CDK outputs
# BUCKET_NAME=$(aws cloudformation describe-stacks \
#     --stack-name FinanceDataPlatform \
#     --query 'Stacks[0].Outputs[?OutputKey==`GlueScriptsBucketName`].OutputValue' \
#     --output text)

BUCKET_NAME="financedataplatform-glue-scripts"

# Upload all scripts from glue_scripts directory
aws s3 sync glue_scripts/ "s3://${BUCKET_NAME}/"

echo "Glue scripts deployed to s3://${BUCKET_NAME}/"