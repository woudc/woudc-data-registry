
while getopts "h:p:d:u:o:" arg; do
    case $arg in
        h)
            HOSTNAME=$OPTARG;;
        p)
            HOSTPORT=$OPTARG;;
        d)
            DB_SCHEMA=$OPTARG;;
        u)
            USERNAME=$OPTARG;;
        o)
            OUTPUT_DIR=$OPTARG;;
    esac
done

if [[ -z $HOSTNAME || -z $HOSTPORT || -z DB_SCHEMA || -z $USERNAME || -z $OUTPUT_DIR || -z $PGPASSWORD ]]; then
    echo "USAGE:"
    echo "  export PGPASSWORD=..."
    echo "  $0 -h HOST -p PORT -d SCHEMA_NAME -u USERNAME -o OUTPUT_DIR"
    exit 1
fi

PROJECTS_QUERY="SELECT DISTINCT(project.project_acronym) AS project_id FROM project"

DATASETS_QUERY="SELECT DISTINCT(incoming_data_category) AS dataset_id FROM data_payload"

CONTRIBUTORS_QUERY="SELECT agency.agency_name AS name, agency.acronym  AS acronym, country.country_code AS country_id, project.project_acronym AS project_id, country.wmo_region AS wmo_region_id, agency.url, REPLACE(email.email_address, ',', ';') AS email, agency.ftpdir AS ftp_username, ST_X(agency.the_geom) AS x, ST_Y(agency.the_geom) AS y FROM agency JOIN country USING (country_id) JOIN email USING (email_id) JOIN project USING (project_id)"

STATIONS_QUERY="SELECT DISTINCT ON (station_id) platform.woudc_platform_identifier AS station_id, platform.platform_name AS station_name, platform_type AS station_type, gaw.gaw_platform_identifier AS gaw_id, country.country_code AS country_id, country.wmo_region AS wmo_region_id, DATE(platform.eff_start_datetime) AS start_date, DATE(platform.eff_end_datetime) AS end_date, ST_X(gaw.the_geom) AS x, ST_Y(gaw.the_geom) AS y, ST_Z(gaw.the_geom) AS z FROM platform JOIN platform_type_definition USING (platform_type_id) JOIN agency USING (agency_id) JOIN country ON platform.country_id = country.country_id JOIN platform_gaw_properties gaw ON platform.platform_id = gaw.platform_id"

STATION_NAMES_QUERY="(SELECT DISTINCT woudc_platform_identifier AS station_id, data_payload.platform_name AS name FROM data_payload FULL JOIN platform ON data_payload.platform_id = platform.platform_id WHERE data_payload.platform_name IS NOT NULL) UNION (SELECT DISTINCT woudc_platform_identifier AS station_id, platform_name AS name FROM platform) ORDER BY station_id"

INSTRUMENTS_QUERY="SELECT platform.woudc_platform_identifier AS station_id, dtd.data_category AS dataset_id, itd.instrument_type AS name, im.instrument_model AS model, instrument.instrument_serial_number AS serial, ST_X(instrument.the_geom) AS x, ST_Y(instrument.the_geom) AS y, ST_Z(instrument.the_geom) AS z FROM instrument JOIN platform USING (platform_id) JOIN dataset_type_definition dtd USING (dataset_type_id) JOIN instrument_model im USING (instrument_model_id) JOIN instrument_type_definition itd USING (instrument_type_id)"

DEPLOYMENTS_QUERY="SELECT platform.woudc_platform_identifier AS station_id, CONCAT(agency.acronym, ':', project.project_acronym) AS contributor_id, DATE(platform.eff_start_datetime) AS start_date, DATE(platform.eff_end_datetime) AS end_date FROM agency JOIN platform USING (agency_id) JOIN project USING (project_id)"

echo "Projects..."
psql -h $HOSTNAME -p $HOSTPORT -d $DB_SCHEMA -U $USERNAME -c "\\COPY ($PROJECTS_QUERY) TO $OUTPUT_DIR/projects.csv WITH CSV HEADER;"

echo "Datasets..."
psql -h $HOSTNAME -p $HOSTPORT -d $DB_SCHEMA -U $USERNAME -c "\\COPY ($DATASETS_QUERY) TO $OUTPUT_DIR/datasets.csv WITH CSV HEADER;"

echo "Contributors..."
psql -h $HOSTNAME -p $HOSTPORT -d $DB_SCHEMA -U $USERNAME -c "\\COPY ($CONTRIBUTORS_QUERY) TO $OUTPUT_DIR/contributors.csv WITH CSV HEADER;"

echo "Stations..."
psql -h $HOSTNAME -p $HOSTPORT -d $DB_SCHEMA -U $USERNAME -c "\\COPY ($STATIONS_QUERY) TO $OUTPUT_DIR/stations.csv WITH CSV HEADER;"

echo "Station Names..."
psql -h $HOSTNAME -p $HOSTPORT -d $DB_SCHEMA -U $USERNAME -c "\\COPY ($STATION_NAMES_QUERY) TO $OUTPUT_DIR/station-names.csv WITH CSV HEADER;"

echo "Instruments..."
psql -h $HOSTNAME -p $HOSTPORT -d $DB_SCHEMA -U $USERNAME -c "\\COPY ($INSTRUMENTS_QUERY) TO $OUTPUT_DIR/instruments.csv WITH CSV HEADER;"

echo "Deployments..."
psql -h $HOSTNAME -p $HOSTPORT -d $DB_SCHEMA -U $USERNAME -c "\\COPY ($DEPLOYMENTS_QUERY) TO $OUTPUT_DIR/deployments.csv WITH CSV HEADER;"

echo "Fetching WMO countries list..."
curl -k -X GET https://cpdb.wmo.int/data/membersandterritories.json > "$OUTPUT_DIR/wmo-countries.json"
