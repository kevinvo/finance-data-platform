#!/bin/bash

# Get the crawler name from CloudFormation outputs
CRAWLER_NAME=$(aws cloudformation describe-stacks \
    --stack-name FinanceDataPlatform \
    --query 'Stacks[0].Outputs[?OutputKey==`GlueCrawlerName`].OutputValue' \
    --output text)

# Start the crawler
aws glue start-crawler --name "$CRAWLER_NAME"

echo "Started Glue crawler: $CRAWLER_NAME"

# Monitor the crawler status
while true; do
    STATUS=$(aws glue get-crawler --name "$CRAWLER_NAME" --query 'Crawler.State' --output text)
    
    echo "Crawler status: $STATUS"
    
    if [[ "$STATUS" == "READY" ]]; then
        break
    fi
    
    sleep 30
done

# Get the database name
DATABASE_NAME=$(aws cloudformation describe-stacks \
    --stack-name FinanceDataPlatform \
    --query 'Stacks[0].Outputs[?OutputKey==`GlueDatabaseName`].OutputValue' \
    --output text)

# Show tables in the database
echo "Tables in database $DATABASE_NAME:"
aws glue get-tables --database-name "$DATABASE_NAME" \
    --query 'TableList[*].{Name:Name,LastUpdated:UpdateTime}' \
    --output table 