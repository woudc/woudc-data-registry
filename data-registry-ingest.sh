#!/bin/bash

# The purpose of this script is to ingest all of the operational WOUDC Archive datasets and rename the generated ingest reports to avoid overwriting
# Initial setup and table initialization of WOUDC Data Registry is required to run this script

TODAY=$(date +"%Y-%m-%d")

INGESTED_FILES_DIR="/apps/data/wdr-ingest/$TODAY"
DATASET_PATHS=(
    "/apps/data/web/woudc-archive/Archive-NewFormat/Broad-band_1.0_1"
    "/apps/data/web/woudc-archive/Archive-NewFormat/Broad-band_2.0_1"
    "/apps/data/web/woudc-archive/Archive-NewFormat/Lidar_1.0_1"
    "/apps/data/web/woudc-archive/Archive-NewFormat/Multi-band_1.0_1"
    "/apps/data/web/woudc-archive/Archive-NewFormat/RocketSonde_1.0_1"
    "/apps/data/web/woudc-archive/Archive-NewFormat/OzoneSonde_1.0_1"
    "/apps/data/web/woudc-archive/Archive-NewFormat/OzoneSonde_1.0_2"
    "/apps/data/web/woudc-archive/Archive-NewFormat/TotalOzone_1.0_1"
    "/apps/data/web/woudc-archive/Archive-NewFormat/TotalOzone_2.0_1"
    "/apps/data/web/woudc-archive/Archive-NewFormat/TotalOzoneObs_1.0_1"
    "/apps/data/web/woudc-archive/Archive-NewFormat/Spectral_1.0_1"
    "/apps/data/web/woudc-archive/Archive-NewFormat/Spectral_2.0_1"
    "/apps/data/web/woudc-archive/Archive-NewFormat/UmkehrN14_1.0_1"
    "/apps/data/web/woudc-archive/Archive-NewFormat/UmkehrN14_2.0_1"
)

# Ensure the directory exists
mkdir -p "$INGESTED_FILES_DIR"

# Function to ingest a dataset and rename the report if successful
ingest_dataset() {
    local dataset_path=$1
    local dataset_name=$(basename "$dataset_path")

    echo "Ingesting: $dataset_name"
    if woudc-data-registry data ingest "$dataset_path" -y -r "$INGESTED_FILES_DIR"; then
        mv "$INGESTED_FILES_DIR/operator-report.csv" "$INGESTED_FILES_DIR/${dataset_name}.csv"
        mv "$INGESTED_FILES_DIR/run_report" "$INGESTED_FILES_DIR/${dataset_name}_run_report"
    else
        echo "Ingestion failed for: $dataset_name."
    fi
}

# Ingest datasets listed in DATASET_PATHS
for dataset_path in "${DATASET_PATHS[@]}"; do
    ingest_dataset "$dataset_path"
done

# Table generation process into registry
woudc-data-registry product totalozone generate -y /apps/data/web/woudc-archive/Archive-NewFormat/
woudc-data-registry product uv-index generate -y /apps/data/web/woudc-archive/Archive-NewFormat/
woudc-data-registry product ozonesonde generate -y /apps/data/web/woudc-archive/Archive-NewFormat/

# WDR sync all data and metadata tables (except data product tables) to ES
woudc-data-registry admin search sync

# WDR sync data product tables (uv_index_hourly, totalozone, and ozonesonde) to ES
woudc-data-registry admin search product-sync