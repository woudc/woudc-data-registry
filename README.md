# WOUDC Data Registry

[![Build Status](https://travis-ci.org/woudc/woudc-data-registry.png)](https://travis-ci.org/woudc/woudc-data-registry)
[![Coverage Status](https://coveralls.io/repos/github/woudc/woudc-data-registry/badge.svg?branch=master)](https://coveralls.io/github/woudc/woudc-data-registry?branch=master)

## Overview

WOUDC Data Registry is a platform that manages Ozone and Ultraviolet
Radiation data in support of the [World Ozone and Ultraviolet Radiation Data
Centre (WOUDC)](https://woudc.org), one of six World Data Centres as part of
the [Global Atmosphere Watch](http://www.wmo.int/gaw) programme of the
[WMO](http://www.wmo.int).


## Installation

### Requirements
- [Python](https://www.python.org) 3 and above
- [virtualenv](https://virtualenv.pypa.io/)
- [Elasticsearch](https://www.elastic.co/products/elasticsearch) (5.5.0 and above)

### Dependencies
Dependencies are listed in [requirements.txt](requirements.txt). Dependencies
are automatically installed during woudc-data-registry installation.

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

# set system environment variables
cp default.env foo.env
vi foo.env  # edit database connection parameters, etc.
. foo.env

# create database
make ENV=foo.env createdb

# drop database
make ENV=foo.env dropdb

# initialize model (database tables)
woudc-data-registry manage setup

# initialize search engine
woudc-data-registry search create-index

# load core metadata
woudc-data-registry manage init

# cleanups

# re-initialize model (database tables)
woudc-data-registry manage teardown
woudc-data-registry manage setup

# re-initialize search engine
woudc-data-registry search delete-index
woudc-data-registry search create-index

# drop database
make ENV=foo.env dropdb

```

### Running woudc-data-registry

```bash
# ingest directory of files (walks directory recursively)
woudc-data-registry data ingest -d /path/to/dir

# ingest single file
woudc-data-registry data ingest -f foo.dat

# verify directory of files (walks directory recursively)
woudc-data-registry data ingest -d /path/to/dir --verify

# verify single file
woudc-data-registry data ingest -f foo.dat --verify

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
