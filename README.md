# WOUDC Data Registry

[![Build Status](https://github.com/woudc/woudc-data-registry/workflows/build%20%E2%9A%99%EF%B8%8F/badge.svg)](https://github.com/woudc/woudc-data-registry/actions)
[![Coverage Status](https://coveralls.io/repos/github/woudc/woudc-data-registry/badge.svg?branch=master)](https://coveralls.io/github/woudc/woudc-data-registry?branch=master)
[![Documentation](https://readthedocs.org/projects/woudc-data-registry/badge/)](https://woudc-data-registry.readthedocs.org)

## Overview

WOUDC Data Registry is a platform that manages ozone and ultraviolet
radiation data in support of the [World Ozone and Ultraviolet Radiation Data
Centre (WOUDC)](https://woudc.org), one of six World Data Centres as part of
the [Global Atmosphere Watch](https://community.wmo.int/activity-areas/gaw) programme of the
[WMO](https://www.wmo.int).

## Installation

### Requirements
- [Python](https://python.org) 3 and above
- [virtualenv](https://virtualenv.pypa.io/)
- [Elasticsearch](https://www.elastic.co/products/elasticsearch) (5.5.0 and above)
- [woudc-extcsv](https://github.com/woudc/woudc-extcsv)

### Dependencies
Dependencies are listed in [requirements.txt](requirements.txt). Dependencies
are automatically installed during installation.

### Installing woudc-data-registry

```bash
# setup virtualenv
python3 -m venv woudc-data-registry_env
cd woudc-data-registry_env
source bin/activate

# clone woudc-extcsv and install
git clone https://github.com/woudc/woudc-extcsv.git
cd woudc-extcsv
pip install -r requirements.txt
pip install .
cd ..

# clone codebase and install
git clone https://github.com/woudc/woudc-data-registry.git
cd woudc-data-registry
pip install .
# optional: for PostgreSQL backends
pip install -r requirements-pg.txt


# set system environment variables
cp default.env foo.env
vi foo.env  # edit database connection parameters, etc.
. foo.env
```


### Initializing the Database

```bash
# create database
make ENV=foo.env createdb

# drop database
make ENV=foo.env dropdb

# show configuration
woudc-data-registry admin config

# initialize model (database tables)
woudc-data-registry admin registry setup

# initialize search engine
woudc-data-registry admin search setup

# load core metadata
woudc-data-registry admin init -d data/

# cleanups

# re-initialize model (database tables)
woudc-data-registry admin registry teardown
woudc-data-registry admin registry setup

# re-initialize search engine
woudc-data-registry admin search teardown
woudc-data-registry admin search setup

# If required reinitialized StationDobsonCorrections table and index
woudc-data-registry admin setup-dobson-correction -d data/
```

### Running woudc-data-registry

TIP: autocompletion can be made available in some shells via:

```bash
eval "$(_WOUDC_DATA_REGISTRY_COMPLETE=source woudc-data-registry)"
```

#### Core Metadata Management

```bash
# list all instances of foo (where foo is one of:
#  project|dataset|contributor|country|station|instrument|deployment)
woudc-data-registry <foo> list
# e.g.
woudc-data-registry contributor list

# show a specific instance of foo with a given registry identifier
woudc-data-registry <foo> show <identifier>
# e.g.
woudc-data-registry station show 023
woudc-data-registry instrument show ECC:2Z:4052:002:OzoneSonde

# add a new instance of foo (contributor|country|station|instrument|deployment)
woudc-data-registry <foo> add <options>
# e.g.
woudc-data-registry deployment add -s 001 -c MSC:WOUDC
woudc-data-registry contributor add -id foo -n "Contributor name" -c Canada -w IV -u https://example.org -e you@example.org -f foouser -g -75,45

# update an existing instance of foo with a given registry identifier
woudc-data-registry <foo> update -id <identifier> <options>
# e.g.
woudc-data-registry station update -n "New station name"
woudc-data-registry deployment update --end-date 'Deployment end date'

# delete an instance of foo with a given registry identifier
woudc-data-registry <foo> delete <identifier>
# e.g.
woudc-data-registry deployment delete 018:MSC:WOUDC

# for more information about options on operation (add|update):
woudc-data-registry <foo> <operation> --help
# e.g.
woudc-data-registry instrument update --help
```

#### Data Processing

```bash
# Gather the files from the ftp account
woudc-data-registry data gather /path/to/dir

# ingest directory of files (walks directory recursively)
woudc-data-registry data ingest /path/to/dir

# ingest single file
woudc-data-registry data ingest foo.dat

# ingest without asking permission checks
woudc-data-registry data ingest foo.dat -y

# verify directory of files (walks directory recursively)
woudc-data-registry data verify /path/to/dir

# verify single file
woudc-data-registry data verify foo.dat

# verify core metadata only
woudc-data-registry data verify foo.dat -l

# ingest with only core metadata checks
woudc-data-registry data ingest /path/to/dir -l
```

#### Dobson Section Corrections
```bash
# Corrects both AD and CD data from TotalOzone Dobson Data
woudc-data-registry correction dobson-correction /path/to/dir --mode [test|ops]

# --code gives to option to choose to correct a specific code 
woudc-data-registry correction dobson-correction /path/to/dir --code [AD|CD] --mode [test|ops]

# --weeklyingest outputs the files in a specific folder structure, similar to incoming folders
woudc-data-registry correction dobson-correction /path/to/dir --mode [test|ops] --weeklyingest
```

#### Search Index Generation

```bash
# sync all data and metadata tables (except data product tables) to ElasticSearch
woudc-data-registry admin search sync

# sync the data product tables (uv_index_hourly, totalozone, and ozonesonde) to ElasticSearch
woudc-data-registry admin search product-sync
```

#### UV Index Generation

```bash
# Teardown and generate entire uv_index_hourly table
woudc-data-registry product uv-index generate /path/to/archive/root


# Only generate uv_index_hourly records within year range
woudc-data-registry product uv-index update -sy start-year -ey end-year /path/to/archive/root
```

#### Total Ozone Generation

```bash
# Teardown and generate entire totalozone table
woudc-data-registry product totalozone generate /path/to/archive/root
```

#### OzoneSonde Generation

```bash
# Teardown and generate entire ozonesonde table
woudc-data-registry product ozonesonde generate /path/to/archive/root
```
#### Report Generation

The `woudc-data-registry data ingest` command accepts a `-r/--report` flag, which is a path pointing to a directory.
When that flag is provided, an operator report and a run report are automatically written to that directory
while the files are being processing.

`woudc-data-registry data ingest /path/to/dir -r /path/to/reports/location`

The run report has a filename `run_report`. The file contains a series of blocks,
one per contributor in a processing run, of the following format:

```
<contributor acronym>
<status>: <filepath>
<status>: <filepath>
<status>: <filepath>
...
```

Where `<status>` is either `Pass` or `Fail`, depending on how the file reported in that line fared in processing.

The operator report is a more in-depth error log in CSV format, with a filename like `operator-report-<date>.csv`.
Operator reports contain one line per error or warning that happened during the processing run. The operator report
is meant to be a human-readable log which makes specific errors easy to find and diagnose.

#### Sending Emails to Contributors

To generate emails for contributors:

```bash
woudc-data-registry data generate-emails /path/to/dir
```

#### Publishing Notifications to MQTT Server
```bash
woudc-data-registry data publish-notification --hours number_of_hours
```

#### Delete Record

```bash
woudc-data-registry data delete-record /path/to/bad/file/
```

If a bad file was previously ingested, it can be removed using this command. This removes the file from the registry and the WAF.

### Development

```bash
# install dev requirements
pip install -r requirements-dev.txt
```

#### Building the Documentation

```bash
# build local copy of https://woudc.github.io/woudc-data-registry
cd docs
make html
python3 -m http.server  # view on http://localhost:8000/
```

#### Running Tests

```bash
# run tests like this:
cd woudc_data_registry/tests
python3 test_data_registry.py
python3 test_delete_record.py

# or this:
python3 setup.py test

# measure code coverage
coverage run --source=woudc_data_registry -m unittest woudc_data_registry.tests.test_data_registry
coverage report -m
```

#### Code Conventions

* [PEP8](https://www.python.org/dev/peps/pep-0008)

### Bugs and Issues

All bugs, enhancements and issues are managed on [GitHub](https://github.com/woudc/woudc-data-registry/issues).

## Contact

* [Tom Kralidis](https://github.com/tomkralidis)

