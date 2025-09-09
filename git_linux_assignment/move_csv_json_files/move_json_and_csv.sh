#!/bin/bash

# Load environment variables from .env file
if [ -f .env ]; then
  source .env
else
  echo ".env file not found!"
  exit 1
fi

# Configuration - Change these paths as needed
SOURCE_FOLDER="${SOURCE_FOLDER:-.}"  # use current directory if no argument provided
DESTINATION_FOLDER="${DESTINATION_FOLDER}"

echo "=== Moving CSV and JSON Files"
echo "=== From: $SOURCE_FOLDER"
echo "=== To: $DESTINATION_FOLDER"
echo

# create destination folder if it doesn't exist
mkdir -p "$DESTINATION_FOLDER"

# get all files (case-insensitive, skip if fileformat does not exist) ---
shopt -s nullglob nocaseglob
files=( "$SOURCE_FOLDER"/*.csv "$SOURCE_FOLDER"/*.json )

csv_files=( "$SOURCE_FOLDER"/*.csv )
json_files=( "$SOURCE_FOLDER"/*.json )

if (( ${#files[@]} == 0 )); then
  echo "=== No CSV or JSON files found in: $SOURCE_FOLDER"
else
  echo "=== Found ${#csv_files[@]} CSV file(s) and ${#json_files[@]} JSON file(s) in: the $SOURCE_FOLDER folder"
fi

echo "=== Moving ${#files[@]} file(s) from '$SOURCE_FOLDER' to '$DESTINATION_FOLDER'"
mv -v "${files[@]}" "$DESTINATION_FOLDER"
echo "=== Files moved successfully."
