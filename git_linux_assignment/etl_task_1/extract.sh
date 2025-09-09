#!/bin/bash

# load environment variables from .env file
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
if [ -f "$SCRIPT_DIR/.env" ]; then
  source "$SCRIPT_DIR/.env"
else
  echo ".env file not found!"
  exit 1
fi


# Variables,
CSV_URL="${CSV_URL}"
RAW_FOLDER="${RAW_FOLDER}"
RAW_FILE=$(basename "$CSV_URL")

echo "=== EXTRACTION PROCESS ==="
#download csv file from provided URL

# Create raw folder if it doesn't exist
mkdir -p "$RAW_FOLDER"

# Download file
echo "Downloading $RAW_FILE from $CSV_URL"
curl -L "$CSV_URL" -o "$RAW_FOLDER/$RAW_FILE"

# Confirm that file is in the raw folder
if [ -f "$RAW_FOLDER/$RAW_FILE" ]; then
  echo "=== File $RAW_FILE successfully saved in $RAW_FOLDER folder."
else
  echo "=== File $RAW_FILE was not found in $RAW_FOLDER folder."
  exit 1
fi

# rename column and select specific columns
echo
echo "=== TRANSFORMATION PROCESS ==="

TRANSFORMED_FOLDER="${TRANSFORMED_FOLDER}"
OUTPUT_FILE="${TRANSFORMED_FILE}"

# Create transformed folder if it doesn't exist
mkdir -p "$TRANSFORMED_FOLDER"

echo "- Renaming 'Variable_code' to 'variable_code'"
echo "- Selecting columns: Year, Value, Units, variable_code"

# read csv file, rename column and select specific columns
awk -F',' '
BEGIN { 
    OFS="," 
    year_col = 0
    value_col = 0
    units_col = 0
    variable_col = 0
}
NR==1 {
    # find column positions in header row
    for (i=1; i<=NF; i++) {
        field = $i
    
        if (field == "Variable_code") variable_col = i
        if (field == "Year") year_col = i
        if (field == "Value") value_col = i
        if (field == "Units") units_col = i
    }
    
    # print new header with renamed column
    print "Year", "Value", "Units", "variable_code"
}
NR>1 {
    if (year_col > 0 && value_col > 0 && units_col > 0 && variable_col > 0) {
        # get values from the selected columns
        year_val = $(year_col)
        value_val = $(value_col)
        units_val = $(units_col)
        variable_val = $(variable_col)
        
        print year_val, value_val, units_val, variable_val
    }
}
' "$RAW_FOLDER/$RAW_FILE" > "$TRANSFORMED_FOLDER/$OUTPUT_FILE"

# confirm transformation was successful
echo
echo "=== LOAD VERIFICATION ==="

if [ -f "$TRANSFORMED_FOLDER/$OUTPUT_FILE" ]; then
    echo "File $OUTPUT_FILE successfully saved in $TRANSFORMED_FOLDER folder."
    echo
    echo "Column names: $(head -1 "$TRANSFORMED_FOLDER/$OUTPUT_FILE")"
    echo
    echo "First 3 rows:"
    head -4 "$TRANSFORMED_FOLDER/$OUTPUT_FILE" | tail -3
else
    echo "File $OUTPUT_FILE was not created in $TRANSFORMED_FOLDER folder."
    exit 1
fi


### LOAD ###
echo 
echo "=== LOAD PROCESS ==="
mkdir -p "./Gold"

cp "$TRANSFORMED_FOLDER/$OUTPUT_FILE" "./Gold/$OUTPUT_FILE"

if [ -f "./Gold/$OUTPUT_FILE" ]; then
  echo "=== File $OUTPUT_FILE successfully saved in ./Gold folder."
else
  echo "=== File $OUTPUT_FILE was not found in ./Gold folder."
  exit 1
fi