#!/bin/bash
# =================================================================
#
# Terms and Conditions of Use
#
# Unless otherwise noted, computer program source code of this
# distribution is covered under Crown Copyright, Government of
# Canada, and is distributed under the MIT License.
#
# The Canada wordmark and related graphics associated with this
# distribution are protected under trademark law and copyright law.
# No permission is granted to use them outside the parameters of
# the Government of Canada's corporate identity program. For
# more information, see
# https://www.canada.ca/en/treasury-board-secretariat/services/government-communications/federal-identity-program.html
#
# Copyright title to all 3rd party software distributed with this
# software is held by the respective copyright holders as noted in
# those files. Users are asked to read the 3rd Party Licenses
# referenced with those assets.
#
# Copyright (c) 2026 Government of Canada
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================
#
# Dumps WOUDC product dataset tables (totalozone, ozonesonde,
# uv_index_hourly) to zipped CSV snapshots.
#
# Usage: woudc-data-make-snapshots.sh <output-dir> [dataset]
#
#   dataset (optional): totalozone, ozonesonde, uv_index_hourly
#
# =================================================================

# Function to log messages
log() {
    printf "%s %s\n" "$(date '+%Y-%m-%d %H:%M:%S')" "$1"
}

# Check required environment variables
required_vars=(
    WDR_DB_HOST
    WDR_DB_PORT
    WDR_DB_NAME
    WDR_DB_USERNAME
    WDR_DB_PASSWORD
    WDR_WAF_DATASET_SNAPSHOTS
)
for var in "${required_vars[@]}"; do
    if [ -z "${!var}" ]; then
        log "Error: Required environment variable $var is not set."
        exit 1
    fi
done

# Function to check for necessary tools
check_tool() {
    if ! command -v "$1" &> /dev/null; then
        log "Error: $1 is not installed."
        exit 1
    fi
}

# Check required tools
check_tool psql
check_tool zip

# Check if the correct number of arguments is provided
if [ "$#" -lt 1 ]; then
    log "Usage: $0 <output-dir> [dataset]"
    log "  dataset (optional): totalozone, ozonesonde, uv_index_hourly"
    exit 1
fi

# Set variables and check if BASEDIR exists
BASEDIR="$1"
FILTER_DATASET="${2:-}"  # Optional second argument

if [ ! -d "$BASEDIR" ]; then
    log "Error: Your output directory $BASEDIR does not exist. Be sure to create it first. Exiting."
    exit 1
fi

if [ ! -d "$WDR_WAF_DATASET_SNAPSHOTS" ]; then
    log "Error: Directory $WDR_WAF_DATASET_SNAPSHOTS does not exist. Exiting."
    exit 1
fi

declare -A DATASET_QUERIES
DATASET_QUERIES["totalozone"]="SELECT
    ozone_id,
    file_name,
    url,
    dataset_id,
    station_id,
    country_id,
    instrument_id,
    gaw_id,
    observation_date,
    daily_date,
    daily_wlcode,
    daily_obscode,
    daily_columno3,
    daily_stdevo3,
    daily_utc_begin,
    daily_utc_end,
    daily_utc_mean,
    daily_nobs,
    daily_mmu,
    daily_columnso2,
    monthly_date,
    monthly_columno3,
    monthly_stdevo3,
    monthly_npts,
    x,
    y,
    z
FROM totalozone"

DATASET_QUERIES["ozonesonde"]="SELECT
    ozone_id,
    file_name,
    url,
    dataset_id,
    station_id,
    country_id,
    instrument_id,
    flight_integratedo3,
    flight_correctioncode,
    flight_sondetotalo3,
    flight_correctionfactor,
    flight_totalo3,
    flight_wlcode,
    flight_obstype,
    profile_pressure,
    profile_o3partialpressure,
    profile_temperature,
    profile_windspeed,
    profile_winddirection,
    profile_levelcode,
    profile_duration,
    profile_gpheight,
    profile_relativehumidity,
    profile_sampletemperature,
    timestamp_date,
    x,
    y,
    z
FROM ozonesonde"

DATASET_QUERIES["uv_index_hourly"]="SELECT
    uv_id,
    url,
    dataset_id,
    station_id,
    country_id,
    instrument_id,
    gaw_id,
    solar_zenith_angle,
    uv_index,
    uv_daily_max,
    uv_index_qa,
    observation_date,
    observation_time,
    observation_utcoffset,
    x,
    y,
    z
FROM uv_index_hourly"

log "Dumping all WOUDC product datasets to CSV..."

# Validate optional dataset filter
if [ -n "$FILTER_DATASET" ]; then
    if [ -z "${DATASET_QUERIES[$FILTER_DATASET]}" ]; then
        log "Error: Invalid dataset '$FILTER_DATASET'."
        log "Valid datasets are: totalozone, ozonesonde, uv_index_hourly"
        exit 1
    fi
    log "Filtering to dataset: $FILTER_DATASET"
fi

# Loop through each dataset and perform operations
for ds in "${!DATASET_QUERIES[@]}"; do

    # Skip if a specific dataset was requested and this isn't it
    if [ -n "$FILTER_DATASET" ] && [ "$ds" != "$FILTER_DATASET" ]; then
        continue
    fi
    
    log "Dumping $ds"

    # Run psql COPY and check for errors
    if ! PGPASSWORD=$WDR_DB_PASSWORD psql \
        -h "$WDR_DB_HOST" \
        -p "$WDR_DB_PORT" \
        -d "$WDR_DB_NAME" \
        -U "$WDR_DB_USERNAME" \
        -c "\COPY (${DATASET_QUERIES[$ds]}) TO '$BASEDIR/$ds.csv' WITH CSV HEADER"; then
        log "Error: Failed to dump dataset $ds to CSV. Continuing with next dataset."
        continue
    fi

    # Check if the CSV exists before zipping
    if [ -f "$BASEDIR/$ds.csv" ]; then
        zip -j "$BASEDIR/$ds.zip" "$BASEDIR/$ds.csv"
        # Adjust permissions
        chmod 775 "$BASEDIR/$ds.zip"
        chgrp woudc-operator "$BASEDIR/$ds.zip"
    else
        log "Warning: CSV file for $ds not found, skipping zip."
        continue
    fi

    # Remove the CSV file if it exists
    if [ -f "$BASEDIR/$ds.csv" ]; then
        rm -f "$BASEDIR/$ds.csv"
    else
        log "Warning: CSV file $BASEDIR/$ds.csv not found, skipping deletion."
    fi

    # Move the zip file to the final directory, overwriting if necessary
    mv -f "$BASEDIR/$ds.zip" "$WDR_WAF_DATASET_SNAPSHOTS"
    log "$BASEDIR/$ds.zip moved to $WDR_WAF_DATASET_SNAPSHOTS"
done

log "Done"