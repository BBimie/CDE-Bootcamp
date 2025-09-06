#!/bin/bash

# simple_move_files.sh - Move CSV and JSON files to json_and_CSV folder

# Configuration - Change these paths as needed
SOURCE_FOLDER="${SOURCE_FOLDER:-.}"  # Default to current directory if no argument provided
DESTINATION_FOLDER="./json_and_CSV"

echo "=== Moving CSV and JSON Files ==="
echo "From: $SOURCE_FOLDER"
echo "To: $DESTINATION_FOLDER"
echo

# # Check if source folder exists
# if [ ! -d "$SOURCE_FOLDER" ]; then
#     echo "❌ Error: Source folder '$SOURCE_FOLDER' does not exist!"
#     echo "Usage: $0 [source_folder]"
#     echo "Example: $0 /path/to/your/files"
#     exit 1
# fi

# Create destination folder if it doesn't exist
mkdir -p "$DESTINATION_FOLDER"
echo "=== Created destination folder: $DESTINATION_FOLDER"

# Count files
csv_files=$(find "$SOURCE_FOLDER" -maxdepth 1 -name "*.csv" -type f)
json_files=$(find "$SOURCE_FOLDER" -maxdepth 1 -name "*.json" -type f)

csv_num=$(echo "$csv_files" | grep -c . 2>/dev/null || echo 0)
json_num=$(echo "$json_files" | grep -c . 2>/dev/null || echo 0)

echo "Found $csv_count CSV files and $json_count JSON files"
echo

# Exit if no files found
if [ $csv_count -eq 0 ] && [ $json_count -eq 0 ]; then
    echo "⚠️  No CSV or JSON files found in $SOURCE_FOLDER"
    exit 0
fi

# Move CSV files
if [ $csv_count -gt 0 ]; then
    echo "Moving CSV files..."
    echo "$csv_files" | while read -r file; do
        if [ -f "$file" ]; then
            filename=$(basename "$file")
            mv "$file" "$DESTINATION_FOLDER/"
            echo "  ✅ Moved: $filename"
        fi
    done
fi

# Move JSON files  
if [ $json_count -gt 0 ]; then
    echo "📦 Moving JSON files..."
    echo "$json_files" | while read -r file; do
        if [ -f "$file" ]; then
            filename=$(basename "$file")
            mv "$file" "$DESTINATION_FOLDER/"
            echo "  ✅ Moved: $filename"
        fi
    done
fi

echo
echo "🎉 All CSV and JSON files moved to $DESTINATION_FOLDER"

# Show final contents
echo
echo "📁 Contents of $DESTINATION_FOLDER:"
ls -la "$DESTINATION_FOLDER" | grep -E '\.(csv|json)$' || echo "  (no files found)"