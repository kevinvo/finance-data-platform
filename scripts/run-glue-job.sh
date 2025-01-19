#!/bin/bash

# Get the job name from CloudFormation outputs
JOB_NAME=$(aws cloudformation describe-stacks \
    --stack-name FinanceDataPlatform \
    --query 'Stacks[0].Outputs[?OutputKey==`GlueJobName`].OutputValue' \
    --output text)

# Start the job
JOB_RUN_ID=$(aws glue start-job-run --job-name "$JOB_NAME" --query 'JobRunId' --output text)

echo "Started Glue job run: $JOB_RUN_ID"

# Monitor the job status
while true; do
    STATUS=$(aws glue get-job-run \
        --job-name "$JOB_NAME" \
        --run-id "$JOB_RUN_ID" \
        --query 'JobRun.JobRunState' \
        --output text)
    
    echo "Job status: $STATUS"
    
    if [[ "$STATUS" == "SUCCEEDED" || "$STATUS" == "FAILED" || "$STATUS" == "STOPPED" ]]; then
        break
    fi
    
    sleep 30
done

# Get the CloudWatch log group
LOG_GROUP=$(aws cloudformation describe-stacks \
    --stack-name FinanceDataPlatform \
    --query 'Stacks[0].Outputs[?OutputKey==`GlueJobLogGroupName`].OutputValue' \
    --output text)

# Show logs
echo "Job completed with status: $STATUS"
echo "Fetching logs from CloudWatch..."
aws logs get-log-events \
    --log-group-name "$LOG_GROUP" \
    --log-stream-name "finance-data-processing-job/$JOB_RUN_ID" \
    --query 'events[*].message' \
    --output text 