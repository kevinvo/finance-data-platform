#!/bin/bash

# Get the Athena results bucket name
RESULTS_BUCKET=$(aws cloudformation describe-stacks \
    --stack-name FinanceDataPlatform \
    --query 'Stacks[0].Outputs[?OutputKey==`AthenaResultsBucketName`].OutputValue' \
    --output text)

WORKGROUP_NAME=$(aws cloudformation describe-stacks \
    --stack-name FinanceDataPlatform \
    --query 'Stacks[0].Outputs[?OutputKey==`AthenaWorkGroupName`].OutputValue' \
    --output text)

echo "Using Athena workgroup '$WORKGROUP_NAME' with results bucket s3://${RESULTS_BUCKET}/"

# Example query to test setup
DATABASE_NAME=$(aws cloudformation describe-stacks \
    --stack-name FinanceDataPlatform \
    --query 'Stacks[0].Outputs[?OutputKey==`GlueDatabaseName`].OutputValue' \
    --output text)

echo -e "\nTesting Athena setup with a sample query..."
QUERY="SELECT ticker, COUNT(*) as count FROM \"${DATABASE_NAME}\".combined_finance_data GROUP BY ticker LIMIT 5;"

# Execute query
QUERY_ID=$(aws athena start-query-execution \
    --query-string "$QUERY" \
    --query-execution-context Database="${DATABASE_NAME}" \
    --work-group "$WORKGROUP_NAME" \
    --query 'QueryExecutionId' \
    --output text)

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
    ERROR_MESSAGE=$(aws athena get-query-execution \
        --query-execution-id "$QUERY_ID" \
        --query 'QueryExecution.Status.StateChangeReason' \
        --output text)
    echo "Query failed: $ERROR_MESSAGE"
fi 