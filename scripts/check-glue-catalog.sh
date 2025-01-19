#!/bin/bash

# Get the database name from CloudFormation outputs
DATABASE_NAME=$(aws cloudformation describe-stacks \
    --stack-name FinanceDataPlatform \
    --query 'Stacks[0].Outputs[?OutputKey==`GlueDatabaseName`].OutputValue' \
    --output text)

echo "Checking database: $DATABASE_NAME"

# List all tables in the database
echo -e "\nTables in database:"
aws glue get-tables \
    --database-name "$DATABASE_NAME" \
    --query 'TableList[*].{Name:Name,LastUpdated:UpdateTime,Location:StorageDescriptor.Location}' \
    --output table

# Get detailed schema for each table
echo -e "\nDetailed schema for tables:"
for TABLE in $(aws glue get-tables --database-name "$DATABASE_NAME" --query 'TableList[*].Name' --output text); do
    echo -e "\nTable: $TABLE"
    aws glue get-table \
        --database-name "$DATABASE_NAME" \
        --name "$TABLE" \
        --query 'Table.{Columns:StorageDescriptor.Columns[*].{Name:Name,Type:Type},Partitions:PartitionKeys[*].{Name:Name,Type:Type}}' \
        --output yaml
done

# Get partition information
echo -e "\nPartition information:"
for TABLE in $(aws glue get-tables --database-name "$DATABASE_NAME" --query 'TableList[*].Name' --output text); do
    echo -e "\nPartitions for table: $TABLE"
    aws glue get-partitions \
        --database-name "$DATABASE_NAME" \
        --table-name "$TABLE" \
        --query 'Partitions[*].Values' \
        --max-items 5 \
        --output table
done 