#!/bin/bash
# =================================================================
#
# Terms and Conditions of Use 
#
# Unless otherwise noted, computer program source code of this
# distribution # is covered under Crown Copyright, Government of
# Canada, and is distributed under the MIT License.
#
# The Canada wordmark and related graphics associated with this
# distribution are protected under trademark law and copyright law.
# No permission is granted to use them outside the parameters of
# the Government of Canada's corporate identity program. For 
# more information, see 
# http://www.tbs-sct.gc.ca/fip-pcim/index-eng.asp
#
# Copyright title to all 3rd party software distributed with this
# software is held by the respective copyright holders as noted in
# those files. Users are asked to read the 3rd Party Licenses
# referenced with those assets.
#
# Copyright (c) 2020 Government of Canada
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

while getopts "o:" arg; do
    case $arg in
        o)
            OUTPUT_DIR=$OPTARG;;
    esac
done

if [[
     -z $WOUDC_ARCHIVE_HOSTNAME
  || -z $WOUDC_ARCHIVE_HOSTPORT
  || -z $WOUDC_ARCHIVE_DBNAME
  || -z $WOUDC_ARCHIVE_USERNAME
  || -z $WOUDC_DATAMART_HOSTNAME
  || -z $WOUDC_DATAMART_HOSTPORT
  || -z $WOUDC_DATAMART_DBNAME
  || -z $WOUDC_DATAMART_USERNAME
  || -z $OUTPUT_DIR
  || -z $PGPASSWORD
  ]]; then
    echo "USAGE:"
    echo "  . migration.env"
    echo "  $0 -o OUTPUT_DIR"
    exit 1
fi

PROJECTS_QUERY="SELECT DISTINCT(project.project_acronym) AS project_id FROM project"

DATASETS_QUERY="SELECT data_category AS dataset_id, data_class, data_level FROM dataset_type_definition"

CONTRIBUTORS_QUERY="SELECT agency.agency_name AS name, agency.acronym  AS acronym, country.country_code AS country_id, project.project_acronym AS project_id, country.wmo_region AS wmo_region_id, agency.url, REPLACE(email.email_address, ',', ';') AS email, agency.ftpdir AS ftp_username, DATE(agency.eff_start_datetime) AS start_date, DATE(agency.eff_end_datetime) AS end_date, ST_X(agency.the_geom) AS x, ST_Y(agency.the_geom) AS y FROM agency JOIN country USING (country_id) JOIN email USING (email_id) JOIN project USING (project_id)"

STATIONS_QUERY="SELECT DISTINCT ON (station_id) platform.woudc_platform_identifier AS station_id, platform.platform_name AS station_name, platform_type AS station_type, gaw.gaw_platform_identifier AS gaw_id, country.country_code AS country_id, country.wmo_region AS wmo_region_id, DATE(platform.eff_start_datetime) AS start_date, DATE(platform.eff_end_datetime) AS end_date, ST_X(gaw.the_geom) AS x, ST_Y(gaw.the_geom) AS y, ST_Z(gaw.the_geom) AS z FROM platform JOIN platform_type_definition USING (platform_type_id) JOIN agency USING (agency_id) JOIN country ON platform.country_id = country.country_id JOIN platform_gaw_properties gaw ON platform.platform_id = gaw.platform_id"

STATION_NAMES_QUERY="(SELECT DISTINCT woudc_platform_identifier AS station_id, data_payload.platform_name AS name FROM data_payload FULL JOIN platform ON data_payload.platform_id = platform.platform_id WHERE data_payload.platform_name IS NOT NULL) UNION (SELECT DISTINCT woudc_platform_identifier AS station_id, platform_name AS name FROM platform) ORDER BY station_id"

INSTRUMENTS_QUERY="SELECT platform.woudc_platform_identifier AS station_id, dtd.data_category AS dataset_id, dtd.data_level AS data_level, itd.instrument_type AS name, im.instrument_model AS model, instrument.instrument_serial_number AS serial, agency.acronym AS contributor, project_acronym AS project, DATE(instrument.eff_start_datetime) AS start_date, DATE(instrument.eff_end_datetime) AS end_date, ST_X(instrument.the_geom) AS x, ST_Y(instrument.the_geom) AS y, ST_Z(instrument.the_geom) AS z FROM instrument JOIN platform USING (platform_id) JOIN agency USING (agency_id) JOIN project USING (project_id) JOIN dataset_type_definition dtd USING (dataset_type_id) JOIN instrument_model im USING (instrument_model_id) JOIN instrument_type_definition itd USING (instrument_type_id)"

DEPLOYMENTS_QUERY="SELECT platform.woudc_platform_identifier AS station_id, CONCAT(agency.acronym, ':', project.project_acronym) AS contributor_id, DATE(platform.eff_start_datetime) AS start_date, DATE(platform.eff_end_datetime) AS end_date FROM agency JOIN platform USING (agency_id) JOIN project USING (project_id)"

NOTIFICATIONS_QUERY="SELECT title_en, title_fr, description_en, description_fr, tags_en, tags_fr, published, banner, visible, ST_X(the_geom) AS x, ST_Y(the_geom) AS y FROM notifications"

WMO_COUNTRIES_URL="https://community.wmo.int/membersdata/membersandterritories.json"

echo "Extracting metadata"

echo " Projects..."
psql -h $WOUDC_ARCHIVE_HOSTNAME -p $WOUDC_ARCHIVE_HOSTPORT -d $WOUDC_ARCHIVE_DBNAME -U $WOUDC_ARCHIVE_USERNAME -c "\\COPY ($PROJECTS_QUERY) TO $OUTPUT_DIR/projects.csv WITH CSV HEADER;"

echo " Datasets..."
psql -h $WOUDC_ARCHIVE_HOSTNAME -p $WOUDC_ARCHIVE_HOSTPORT -d $WOUDC_ARCHIVE_DBNAME -U $WOUDC_ARCHIVE_USERNAME -c "\\COPY ($DATASETS_QUERY) TO $OUTPUT_DIR/datasets.csv WITH CSV HEADER;"

echo " Contributors..."
psql -h $WOUDC_ARCHIVE_HOSTNAME -p $WOUDC_ARCHIVE_HOSTPORT -d $WOUDC_ARCHIVE_DBNAME -U $WOUDC_ARCHIVE_USERNAME -c "\\COPY ($CONTRIBUTORS_QUERY) TO $OUTPUT_DIR/contributors.csv WITH CSV HEADER;"

echo " Stations..."
psql -h $WOUDC_ARCHIVE_HOSTNAME -p $WOUDC_ARCHIVE_HOSTPORT -d $WOUDC_ARCHIVE_DBNAME -U $WOUDC_ARCHIVE_USERNAME -c "\\COPY ($STATIONS_QUERY) TO $OUTPUT_DIR/stations.csv WITH CSV HEADER;"

echo " Station Names..."
psql -h $WOUDC_ARCHIVE_HOSTNAME -p $WOUDC_ARCHIVE_HOSTPORT -d $WOUDC_ARCHIVE_DBNAME -U $WOUDC_ARCHIVE_USERNAME -c "\\COPY ($STATION_NAMES_QUERY) TO $OUTPUT_DIR/station-names.csv WITH CSV HEADER;"

echo " Instruments..."
psql -h $WOUDC_ARCHIVE_HOSTNAME -p $WOUDC_ARCHIVE_HOSTPORT -d $WOUDC_ARCHIVE_DBNAME -U $WOUDC_ARCHIVE_USERNAME -c "\\COPY ($INSTRUMENTS_QUERY) TO $OUTPUT_DIR/instruments.csv WITH CSV HEADER;"

echo " Deployments..."
psql -h $WOUDC_ARCHIVE_HOSTNAME -p $WOUDC_ARCHIVE_HOSTPORT -d $WOUDC_ARCHIVE_DBNAME -U $WOUDC_ARCHIVE_USERNAME -c "\\COPY ($DEPLOYMENTS_QUERY) TO $OUTPUT_DIR/deployments.csv WITH CSV HEADER;"

echo " Notifications..."
psql -h $WOUDC_DATAMART_HOSTNAME -p $WOUDC_DATAMART_HOSTPORT -d $WOUDC_DATAMART_DBNAME -U $WOUDC_DATAMART_USERNAME -c "\\COPY ($NOTIFICATIONS_QUERY) TO $OUTPUT_DIR/notifications.csv WITH CSV HEADER;"

echo "Fetching WMO countries list..."
curl -k -o "$OUTPUT_DIR/wmo-countries.json" $WMO_COUNTRIES_URL
