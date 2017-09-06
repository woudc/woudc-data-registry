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
# Copyright (c) 2017 Government of Canada
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

include $(ENV)

PG_FLAGS=-h $(DB_HOST) -p $(DB_PORT) $(DB_NAME) -U $(DB_USERNAME)

help:
	@echo
	@echo "make targets:"
	@echo
	@echo " createdb: create PostgreSQL/PostGIS database"
	@echo " dropdb: drop PostgreSQL/PostGIS database"
	@echo " setup: create models and search index"
	@echo " teardown: delete models and search index"
	@echo " test: run tests"
	@echo " coverage: run code coverage"
	@echo " package: create Python wheel"
	@echo " clean: remove transitory files"
	@echo

clean:
	find . -type d -name __pycache__ -exec rm -fr {} +
	rm -fr *.egg-info
	rm -fr .pybuild
	rm -fr build
	rm -fr dist
	rm -f debian/files
	rm -f debian/woudc-data-registry.postinst.debhelper
	rm -f debian/woudc-data-registry.prerm.debhelper
	rm -f debian/woudc-data-registry.substvars
	rm -fr debian/woudc-data-registry

coverage:
	coverage run --source=woudc_data_registry -m unittest woudc_data_registry.tests.run_tests
	coverage report -m

createdb:
	createdb $(PG_FLAGS) -E UTF8 --template=template0
	psql $(PG_FLAGS) -c "create extension postgis;"

dropdb:
	dropdb $(PG_FLAGS)

flake8:
	flake8 woudc_data_registry

package:
	python setup.py sdist bdist_wheel

setup:
	woudc-data-registry manage setup
	woudc-data-registry search create_index

teardown:
	woudc-data-registry manage teardown
	woudc-data-registry search delete_index

test:
	python setup.py test

.PHONY: clean coverage createdb dropdb flake8 help package setup teardown test
