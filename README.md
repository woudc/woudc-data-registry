# WOUDC Data Registry

[![Build Status](https://travis-ci.org/woudc/woudc-data-registry.png)](https://travis-ci.org/woudc/woudc-data-registry)
[![Coverage Status](https://coveralls.io/repos/github/woudc/woudc-data-registry/badge.svg?branch=master)](https://coveralls.io/github/woudc/woudc-data-registry?branch=master)
[![Documentation](https://readthedocs.org/projects/woudc-data-registry/badge/)](https://woudc-data-registry.readthedocs.org)

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
woudc-data-registry admin registry setup

# initialize search engine
woudc-data-registry admin search setup

# load core metadata

# fetch WMO country list
mkdir data
curl -o data/wmo-countries.json https://www.wmo.int/cpdb/data/membersandterritories.json
woudc-data-registry admin init -d data/

# cleanups

# re-initialize model (database tables)
woudc-data-registry admin registry teardown
woudc-data-registry admin registry setup

# re-initialize search engine
woudc-data-registry admin search teardown
woudc-data-registry admin search setup

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
# list all instances of foo (where foo is one of:
#  project|dataset|contributor|country|station|instrument|deployment)
woudc-data-registry <foo> list
 e.g.
woudc-data-registry contributor list

# show a specific instance of foo with a given registry identifier
woudc-data-registry <foo> show <identifier>
 e.g.
woudc-data-registry station show 023
woudc-data-registry instrument show ECC:2Z:4052:002:OzoneSonde

# add a new instance of foo (contributor|country|station|instrument|deployment)
woudc-data-registry <foo> add <options>
 e.g.
woudc-data-registry deployment add -s 001 -c MSC:WOUDC
woudc-data-registry contributor add -id foo -n "Contributor name" -c Canada -w IV -u https://example.org -e you@example.org -f foouser -g -75,45

# update an existing instance of foo with a given registry identifier
woudc-data-registry <foo> update -id <identifier> <options>
 e.g.
woudc-data-registry station update -n "New station name"
woudc-data-registry deployment update --end-date 'Deployment end date'

# delete an instance of foo with a given registry identifier
woudc-data-registry <foo> delete <identifier>
 e.g.
woudc-data-registry deployment delete 018:MSC:WOUDC

# for more information about options on operation (add|update):
woudc-data-registry <foo> <operation> --help
 e.g.
woudc-data-registry instrument update --help
```

#### Data Processing

```bash
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
python -m http.server  # view on http://localhost:8000/
```

#### Running Tests

```bash
# run tests like this:
cd woudc_data_registry/tests
python test_data_registry.py

# or this:
python setup.py test

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
