#!/bin/bash

# LOG_GROUP="/aws/lambda/DataPlatformStack-YahooFinanceETL"
REGION="ap-southeast-2"  # Replace with your region

# LOG_GROUP_NAME="/aws/lambda/FinanceDataPlatform-YahooFinanceETL3C2DA848-tXfVFdec0ulQ"
LOG_GROUP_NAME="/aws/lambda/FinanceDataPlatform-EDGARFinanceETL84E58F9B-nQ6uc47IH2wK"
INTERVAL=2  # seconds between checks

# Function to delete log group
delete_log_group() {
    echo "Are you sure you want to delete log group: $LOG_GROUP_NAME? (y/N)"
    read -r confirm
    if [[ $confirm == [yY] || $confirm == [yY][eE][sS] ]]; then
        echo "Deleting log group: $LOG_GROUP_NAME"
        aws logs delete-log-group --log-group-name "$LOG_GROUP_NAME"
        if [ $? -eq 0 ]; then
            echo "Log group deleted successfully"
            exit 0
        else
            echo "Failed to delete log group"
            exit 1
        fi
    else
        echo "Deletion cancelled"
        exit 0
    fi
}

# Add delete option to usage
if [ "$1" == "--delete" ]; then
    delete_log_group
fi

# Function to list all streams
list_streams() {
    echo "Available log streams:"
    aws logs describe-log-streams \
        --log-group-name "$LOG_GROUP_NAME" \
        --order-by LastEventTime \
        --descending \
        --query 'logStreams[*].[logStreamName,lastEventTimestamp]' \
        --output table
}

# Function to get latest events
get_latest_events() {
    local stream_name=$(aws logs describe-log-streams \
        --log-group-name "$LOG_GROUP_NAME" \
        --order-by LastEventTime \
        --descending \
        --limit 1 \
        --query 'logStreams[0].logStreamName' \
        --output text)
    
    if [ "$stream_name" == "None" ] || [ -z "$stream_name" ]; then
        echo "No log streams found"
        return 1
    fi
    
    echo "Watching stream: $stream_name"
    aws logs get-log-events \
        --log-group-name "$LOG_GROUP_NAME" \
        --log-stream-name "$stream_name" \
        --limit 10 \
        --query 'events[*].[timestamp,message]' \
        --output text
}

# Show available streams first
list_streams

echo "Starting log monitoring..."
echo "Press Ctrl+C to exit"

# Initial fetch
LAST_OUTPUT=$(get_latest_events)
echo "$LAST_OUTPUT"

# Continuous monitoring
while true; do
    sleep $INTERVAL
    NEW_OUTPUT=$(get_latest_events)
    if [ "$NEW_OUTPUT" != "$LAST_OUTPUT" ] && [ ! -z "$NEW_OUTPUT" ]; then
        echo "$NEW_OUTPUT"
        LAST_OUTPUT="$NEW_OUTPUT"
    fi
done 