#!/bin/bash

# Get the Athena results bucket name (using the new output name)
RESULTS_BUCKET=$(aws cloudformation describe-stacks \
    --stack-name FinanceDataPlatform \
    --query 'Stacks[0].Outputs[?OutputKey==`AthenaResultsBucketName`].OutputValue' \
    --output text)

# Create Athena workgroup with proper configuration
WORKGROUP_NAME="finance-analysis"
aws athena create-work-group \
    --name "$WORKGROUP_NAME" \
    --configuration "ResultConfiguration={OutputLocation=s3://${RESULTS_BUCKET}/},EnforceWorkGroupConfiguration=true,PublishCloudWatchMetricsEnabled=true,BytesScannedCutoffPerQuery=1073741824" \
    --description "Workgroup for analyzing finance data" \
    --state ENABLED \
    2>/dev/null || true

echo "Waiting for workgroup to be available..."
# Wait for workgroup to be available with timeout
MAX_ATTEMPTS=100
ATTEMPT=1
while [ $ATTEMPT -le $MAX_ATTEMPTS ]; do
    if aws athena get-work-group --work-group "$WORKGROUP_NAME" >/dev/null 2>&1; then
        echo "Workgroup is now available"
        break
    fi
    echo "Attempt $ATTEMPT of $MAX_ATTEMPTS - Workgroup not ready yet..."
    sleep 2
    ATTEMPT=$((ATTEMPT + 1))
done

if [ $ATTEMPT -gt $MAX_ATTEMPTS ]; then
    echo "Timeout waiting for workgroup to be available"
    exit 1
fi

echo "Athena workgroup '$WORKGROUP_NAME' configured to use s3://${RESULTS_BUCKET}/"

# Example query to test setup
DATABASE_NAME=$(aws cloudformation describe-stacks \
    --stack-name FinanceDataPlatform \
    --query 'Stacks[0].Outputs[?OutputKey==`GlueDatabaseName`].OutputValue' \
    --output text)

echo -e "\nTesting Athena setup with a sample query..."
QUERY="SELECT ticker, COUNT(*) as count FROM \"${DATABASE_NAME}\".combined_finance_data GROUP BY ticker LIMIT 5;"

# Execute query with proper error handling
QUERY_ID=$(aws athena start-query-execution \
    --query-string "$QUERY" \
    --query-execution-context Database="${DATABASE_NAME}" \
    --work-group "$WORKGROUP_NAME" \
    --result-configuration "OutputLocation=s3://${RESULTS_BUCKET}/query-results/" \
    --query 'QueryExecutionId' \
    --output text 2>/dev/null)

if [ $? -ne 0 ] || [ -z "$QUERY_ID" ]; then
    echo "Failed to start query execution. Checking workgroup status..."
    aws athena get-work-group --work-group "$WORKGROUP_NAME"
    exit 1
fi

echo "Started query with ID: $QUERY_ID"

# Wait for query to complete
while true; do
    STATUS=$(aws athena get-query-execution \
        --query-execution-id "$QUERY_ID" \
        --query 'QueryExecution.Status.State' \
        --output text)
    
    echo "Query status: $STATUS"
    
    if [[ "$STATUS" == "SUCCEEDED" || "$STATUS" == "FAILED" || "$STATUS" == "CANCELLED" ]]; then
        break
    fi
    sleep 2
done

if [[ "$STATUS" == "SUCCEEDED" ]]; then
    echo -e "\nQuery results:"
    aws athena get-query-results \
        --query-execution-id "$QUERY_ID" \
        --query 'ResultSet.Rows[*].Data[*].VarCharValue' \
        --output table
else
    # Get error details if query failed
    ERROR_MESSAGE=$(aws athena get-query-execution \
        --query-execution-id "$QUERY_ID" \
        --query 'QueryExecution.Status.StateChangeReason' \
        --output text)
    echo "Query failed: $ERROR_MESSAGE"
fi 