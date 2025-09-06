#!/bin/bash

echo "=== Daily CRON JOB ==="

# Get current directory
CURRENT_DIR=$(pwd)
SCRIPT_PATH="$CURRENT_DIR/extract.sh"

# Check if extract.sh exists
if [ ! -f "$SCRIPT_PATH" ]; then
    echo "Error: extract.sh not found in current directory"
    echo "Please run this setup from the project root directory"
    exit 1
fi

# Make the script executable
chmod +x extract.sh
echo "=== $SCRIPT_PATH is executable"

# Cronjob notation; every 2 minutes
CRON_JOB="*/2 * * * * cd $CURRENT_DIR && ./extract.sh >> $CURRENT_DIR/scheduler.log 2>&1"

echo
echo "$CRON_JOB"
echo

# Add the cron job
( crontab -l 2>/dev/null | grep -v "$SCRIPT_PATH"; echo "$CRON_JOB" ) | crontab -

echo "Cron job active."


#sudo killall cron