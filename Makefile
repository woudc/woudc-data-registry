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
# Copyright (c) 2019 Government of Canada
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

PG_FLAGS=-h $(WDR_DB_HOST) -p $(WDR_DB_PORT) -U $(WDR_DB_USERNAME) -W $(WDR_DB_NAME)

help:
	@echo
	@echo "make targets:"
	@echo
	@echo " createdb: create PostgreSQL database"
	@echo " dropdb: drop PostgreSQL database"
	@echo " setup: create models and search index"
	@echo " setup_data: download core metadata"
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
	@echo "NOT removing data/"

coverage:
	coverage run --source=woudc_data_registry -m unittest woudc_data_registry.tests.test_data_registry
	coverage report -m

createdb:
	createdb $(PG_FLAGS) -E UTF8

dropdb:
	dropdb $(PG_FLAGS)

flake8:
	flake8 woudc_data_registry

bps-migrate:
	. migration/bps/migration.env && migration/bps/get-bps-metadata.sh -o data

init:
	woudc-data-registry admin init -d data/

package:
	python3 setup.py sdist bdist_wheel

setup:
	woudc-data-registry admin registry setup
	woudc-data-registry admin search setup

setup_data:
	mkdir -p data
	curl -o data/wmo-countries.json https://www.wmo.int/cpdb/data/membersandterritories.json

teardown:
	woudc-data-registry admin registry teardown
	woudc-data-registry admin search teardown

reset:
	@echo "This command will wipe out the following:"; \
	echo "- ES indexes with basename '${WDR_SEARCH_INDEX_BASENAME}' on $(shell echo ${WDR_SEARCH_URL} | sed 's|^[^@]*@||' | sed 's|/.*||')"; \
	echo "- Registry database '${WDR_DB_NAME}' on ${WDR_DB_HOST}"; \
	echo ""; \
	echo "Then it will fully rebuild the registry and search index with your initial data."; \
	read -p "Are you sure you want to proceed? (y/N) " confirm && [ "$$confirm" = "y" ] && $(MAKE) --no-print-directory teardown bps-migrate setup init || echo "Aborted."

test:
	python3 setup.py test

.PHONY: clean coverage createdb dropdb flake8 help init package setup setup_data teardown test
