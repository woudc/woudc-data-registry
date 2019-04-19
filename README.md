# WOUDC Data Registry

[![Build Status](https://travis-ci.org/woudc/woudc-data-registry.png)](https://travis-ci.org/woudc/woudc-data-registry)
[![Coverage Status](https://coveralls.io/repos/github/woudc/woudc-data-registry/badge.svg?branch=master)](https://coveralls.io/github/woudc/woudc-data-registry?branch=master)

## Overview

WOUDC Data Registry is a platform that manages ozone and ultraviolet
radiation data in support of the [World Ozone and Ultraviolet Radiation Data
Centre (WOUDC)](https://woudc.org), one of six World Data Centres as part of
the [Global Atmosphere Watch](http://www.wmo.int/gaw) programme of the
[WMO](https://wmo.int).

## Installation

### Requirements
- [Python](https://python.org) 3 and above
- [virtualenv](https://virtualenv.pypa.io/)
- [Elasticsearch](https://www.elastic.co/products/elasticsearch) (5.5.0 and above)

### Dependencies
Dependencies are listed in [requirements.txt](requirements.txt). Dependencies
are automatically installed during installation.

### Installing woudc-data-registry

```bash
# setup virtualenv
python3 -m venv --system-site-packages woudc-data-registry
cd woudc-data-registry
source bin/activate

# clone codebase and install
git clone https://github.com/woudc/woudc-data-registry.git
cd woudc-data-registry
python setup.py build
python setup.py install
# for PostgreSQL backends
pip install -r requirements-pg.txt


# set system environment variables
cp default.env foo.env
vi foo.env  # edit database connection parameters, etc.
. foo.env

# create database
make ENV=foo.env createdb

# drop database
make ENV=foo.env dropdb

# initialize model (database tables)
woudc-data-registry admin setup

# initialize search engine
woudc-data-registry admin search create-index

# load core metadata

# fetch WMO country list
mkdir data
curl -o data/wmo-countries.json https://www.wmo.int/cpdb/data/membersandterritories.json
woudc-data-registry admin init -d data/

# cleanups

# re-initialize model (database tables)
woudc-data-registry admin teardown
woudc-data-registry admin setup

# re-initialize search engine
woudc-data-registry admin search delete-index
woudc-data-registry admin search create-index

# drop database
make ENV=foo.env dropdb

```

### Running woudc-data-registry

TIP: autocompletion can be made available in some shells via:

```bash
eval "$(_WOUDC_DATA_REGISTRY_COMPLETE=source woudc-data-registry)"
```

#### Core Metadata Management

```bash
# list all contributors
woudc-data-registry contributor list

# show a single contributor details
woudc-data-registry contributor show MSC

# add a contributor
woudc-data-registry contributor add -id foo -n "Contributor name" -c Canada -w IV -u https://example.org -e you@example.org -f foouser -g -75,45

# update a contributor
woudc-data-registry contributor update -id foo -n "New Contributor name"

# delete a contributor
woudc-data-registry contributor delete foo
```

#### Data Processing

```bash
# ingest directory of files (walks directory recursively)
woudc-data-registry data ingest -d /path/to/dir

# ingest single file
woudc-data-registry data ingest -f foo.dat

# verify directory of files (walks directory recursively)
woudc-data-registry data verify -d /path/to/dir

# verify single file
woudc-data-registry data verify -f foo.dat
```

### Running Tests

```bash
# install dev requirements
pip install -r requirements-dev.txt

# run tests like this:
cd woudc_data_registry/tests
python test_data_registry.py

# or this:
python setup.py test

# measure code coverage
coverage run --source=woudc_data_registry -m unittest woudc_data_registry.tests.test_data_registry
coverage report -m
```

### Code Conventions

* [PEP8](https://www.python.org/dev/peps/pep-0008)

### Bugs and Issues

All bugs, enhancements and issues are managed on [GitHub](https://github.com/woudc/woudc-data-registry/issues).

## Contact

* [Tom Kralidis](https://github.com/tomkralidis)
