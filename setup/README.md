# WOUDC Data Registry Setup Instructions

This guide provides step by step instructions to setup and display WOUDC data 
within a queryable Elasticsearch system.


```bash
# Clone WOUDC Data Registry from github
git clone https://github.com/woudc/woudc-data-registry

cd woudc-data-registry

# Activate python virtual environment for Woudc Data Registry: 
source bin/activate

cd woudc-data-registry

# Install WOUDC Data Registry
python setup.py install
 
# Setup config file: 
vim foo.env

# Run contents of config file
. foo.env

# Initialize the WOUDC Data Registry tables
woudc-data-registry admin registry setup

cd /migration/bps 

# Setup migration.env file
vim migration.env

# Run contents of migration.env
. migration.env

# Generate WOUDC Data Registry extCSV files
get-bps-metadata.sh -o /path/to/output-dir/ 

# Initalize WOUDC Data Registry
woudc-data-registry admin init -d /path/to/output-dir/

# Ingest files into WOUDC Data Registry
woudc-data-registry data ingest /path/to/dir

# various ingest options including an operator report and skipping prompts found at https://github.com/woudc/woudc-data-registry

# Sync contents of the WOUDC Data Registry database to Elasticsearch
woudc-data-registry admin search sync



