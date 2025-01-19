#!/bin/bash

# Get the database name
DATABASE_NAME=$(aws cloudformation describe-stacks \
    --stack-name FinanceDataPlatform \
    --query 'Stacks[0].Outputs[?OutputKey==`GlueDatabaseName`].OutputValue' \
    --output text)

# Create a workgroup if it doesn't exist
WORKGROUP_NAME="finance-analysis"
aws athena create-work-group \
    --name "$WORKGROUP_NAME" \
    --configuration ResultConfiguration={OutputLocation=s3://financedataplatform-processed-data/athena-results/} \
    2>/dev/null || true

# Sample queries
QUERIES=(
    "SELECT COUNT(*) as total_records FROM \"$DATABASE_NAME\".combined_finance_data;"
    "SELECT ticker, COUNT(*) as records FROM \"$DATABASE_NAME\".combined_finance_data GROUP BY ticker;"
    "SELECT ticker, MIN(date) as earliest, MAX(date) as latest FROM \"$DATABASE_NAME\".combined_finance_data GROUP BY ticker;"
)

for QUERY in "${QUERIES[@]}"; do
    echo -e "\nExecuting query: $QUERY"
    
    # Start query execution
    QUERY_ID=$(aws athena start-query-execution \
        --query-string "$QUERY" \
        --query-execution-context Database="$DATABASE_NAME" \
        --work-group "$WORKGROUP_NAME" \
        --query 'QueryExecutionId' \
        --output text)
    
    # Wait for query to complete
    while true; do
        STATUS=$(aws athena get-query-execution --query-execution-id "$QUERY_ID" --query 'QueryExecution.Status.State' --output text)
        if [[ "$STATUS" == "SUCCEEDED" || "$STATUS" == "FAILED" || "$STATUS" == "CANCELLED" ]]; then
            break
        fi
        sleep 1
    done
    
    # Get results
    if [[ "$STATUS" == "SUCCEEDED" ]]; then
        aws athena get-query-results --query-execution-id "$QUERY_ID" --query 'ResultSet.Rows[*].Data[*].VarCharValue' --output table
    else
        echo "Query failed with status: $STATUS"
    fi
done 