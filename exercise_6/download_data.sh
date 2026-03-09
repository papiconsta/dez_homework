#!/bin/bash
set -e

TAXI_TYPE=$1   # e.g., "yellow"
YEAR=$2        # e.g., 2025
MONTH_P=$3     # optional, e.g., 11

if [ -z "$TAXI_TYPE" ] || [ -z "$YEAR" ]; then
    echo "Usage: $0 TAXI_TYPE YEAR [MONTH]"
    exit 1
fi

URL_PREFIX="https://github.com/DataTalksClub/nyc-tlc-data/releases/download"

# Function to download a single month
download_month() {
    local MONTH=$1
    FMONTH=$(printf "%02d" "$MONTH")
    URL="${URL_PREFIX}/${TAXI_TYPE}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.csv.gz"
    LOCAL_PREFIX="data/raw/${TAXI_TYPE}/${YEAR}/${FMONTH}"
    LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}_${FMONTH}.csv.gz"
    LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"

    echo "Downloading ${URL} to ${LOCAL_PATH}"
    mkdir -p "${LOCAL_PREFIX}"
    wget -q --show-progress "${URL}" -O "${LOCAL_PATH}"
}

# If MONTH_P is provided, download only that month
if [ -n "$MONTH_P" ]; then
    download_month "$MONTH_P"
else
    # Download all months 1-12
    for m in {1..12}; do
        download_month "$m"
    done
fi
