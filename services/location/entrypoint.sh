#!/bin/bash

set -e

# Configuration
MIN_POPULATION=${MIN_POPULATION:-0}
DATA_DIR="/app/shared/data/location_data"
DB_PATH="/app/db/geonames.db"

echo "Starting location service with MIN_POPULATION=${MIN_POPULATION}"

# Create data directory if it doesn't exist
mkdir -p "$DATA_DIR"

# Check if database already exists
if [ -f "$DB_PATH" ]; then
    echo "Database already exists at $DB_PATH, skipping build"
else
    # Check if zip files already exist
    if [ -f "$DATA_DIR/allCountries.zip" ] && [ -f "$DATA_DIR/alternateNamesV2.zip" ]; then
        echo "GeoNames zip files already exist, skipping download"
    else
        echo "Downloading GeoNames data..."

        # Download GeoNames data
        # AND show progress bar
        # AND remove the carriage return
        if [ ! -f "$DATA_DIR/allCountries.zip" ]; then
            curl --progress-bar -L -o "$DATA_DIR/allCountries.zip" \
            https://download.geonames.org/export/dump/allCountries.zip 2>&1 \
            | stdbuf -oL -eL tr '\r' '\n'
        else
            echo "allCountries.zip already exists, skipping download"
        fi

        if [ ! -f "$DATA_DIR/alternateNamesV2.zip" ]; then
            curl --progress-bar -L -o "$DATA_DIR/alternateNamesV2.zip" \
            https://download.geonames.org/export/dump/alternateNamesV2.zip 2>&1 \
            | stdbuf -oL -eL tr '\r' '\n'
        else
            echo "alternateNamesV2.zip already exists, skipping download"
        fi
    fi

    echo "Building GeoNames database with min_population=${MIN_POPULATION}..."

    # Build the database
    geodb build \
        --all "$DATA_DIR/allCountries.zip" \
        --alt "$DATA_DIR/alternateNamesV2.zip" \
        --out "$DB_PATH" \
        --min-pop "$MIN_POPULATION"

    echo "Database built successfully"
fi

echo "Starting location service..."
exec geodb serve --db "$DB_PATH" --bind "0.0.0.0:8787"