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


import os
import yaml

import shutil
import logging

from datetime import datetime

from woudc_data_registry import config
from woudc_data_registry.models import (Contributor, DataRecord, Dataset,
                                        Deployment, Instrument, Project,
                                        Station, StationName)
from woudc_data_registry.parser import (DOMAINS, ExtendedCSV,
                                        MetadataValidationError,
                                        NonStandardDataError)
from woudc_data_registry.dataset_validators import get_validator
from woudc_data_registry.util import is_text_file, read_file

from woudc_data_registry.epicentre.station import build_station_name
from woudc_data_registry.epicentre.instrument import build_instrument
from woudc_data_registry.epicentre.deployment import build_deployment


LOGGER = logging.getLogger(__name__)

with open(config.WDR_ALIAS_CONFIG) as alias_definitions:
    ALIASES = yaml.safe_load(alias_definitions)


class Process(object):
    """
    Generic processing definition

    - detect
    - parse
    - validate
    - verify
    - register
    - index
    """

    def __init__(self, registry_conn, search_index_conn):
        """constructor"""

        self.status = None
        self.code = None
        self.message = None
        self.process_start = datetime.utcnow()
        self.process_end = None
        self.registry = registry_conn
        self.search_index = search_index_conn

        self._registry_updates = []
        self._search_index_updates = []

        self.warnings = []
        self.errors = []

    def _warning(self, error_code, line, message=None):
        """
        Record <message> as an error with code <error_code> that took place
        at line <line> in the input file.

        :param error_code: Numeric error code from the error definition files.
        :param line: Line number in the input file where the error was found.
        :param message: String message describing the error.
        :returns: void
        """

        LOGGER.warning(message)
        self.warnings.append((error_code, message, line))

    def _error(self, error_code, line, message=None):
        """
        Record <message> as an error with code <error_code> that took place
        at line <line> in the input file.

        :param error_code: Numeric error code from the error definition files.
        :param line: Line number in the input file where the error was found.
        :param message: String message describing the error.
        :returns: void
        """

        LOGGER.error(message)
        self.errors.append((error_code, message, line))

    def validate(self, infile, metadata_only=False, verify_only=False,
                 bypass=False):
        """
        Process incoming data record.

        :param infile: Path to incoming data file.
        :param metadata_only: `bool` of whether to only verify common
                              metadata tables.
        :param verify_only: `bool` of whether to verify the file for
                            correctness without processing.
        :param bypass: `bool` of whether to skip permission prompts
                        to add records.
        :returns: `bool` of whether the operation was successful.
        """

        # detect incoming data file
        data = None
        self.extcsv = None

        self.warnings = []
        self.errors = []

        LOGGER.info('Processing file {}'.format(infile))
        LOGGER.info('Detecting file')
        if not is_text_file(infile):
            msg = 'Binary file detected'
            self._error(1000, None, msg)
            return False

        try:
            data = read_file(infile)
        except UnicodeDecodeError as err:
            self.status = 'failed'
            self.code = 'NonStandardDataError'
            self.message = err
            LOGGER.error('Unknown file: {}'.format(err))
            return False

        try:
            LOGGER.info('Parsing data record')
            self.extcsv = ExtendedCSV(data)
            LOGGER.info('Validating Extended CSV')
            self.extcsv.validate_metadata_tables()
            if not metadata_only:
                self.extcsv.validate_dataset_tables()
            LOGGER.info('Valid Extended CSV')
        except NonStandardDataError as err:
            self.status = 'failed'
            self.code = 'NonStandardDataError'
            self.message = str(err).strip()
            LOGGER.error('Invalid Extended CSV: {}'.format(self.message))
            return False
        except MetadataValidationError as err:
            self.status = 'failed'
            self.code = 'MetadataValidationError'
            self.message = err
            LOGGER.error('Invalid Extended CSV: {}'.format(err.errors))
            return False

        LOGGER.info('Data is valid Extended CSV')
        LOGGER.info('Verifying data record against core metadata fields')

        project_ok = self.check_project()
        dataset_ok = self.check_dataset()

        if not project_ok:
            LOGGER.warning('Skipping contributor check: depends on'
                           ' values with errors')
            contributor_ok = False
        else:
            contributor_ok = self.check_contributor()

        platform_ok = self.check_station(bypass=bypass, verify=verify_only)

        if not all([project_ok, contributor_ok, platform_ok]):
            LOGGER.warning('Skipping deployment check: depends on'
                           ' values with errors')
            deployment_ok = False
        else:
            LOGGER.debug('Validating agency deployment')
            deployment_ok = self.check_deployment()

            project = self.extcsv.extcsv['CONTENT']['Class']
            agency = self.extcsv.extcsv['DATA_GENERATION']['Agency']
            platform_id = str(self.extcsv.extcsv['PLATFORM']['ID'])

            if not deployment_ok:
                deployment_id = ':'.join([platform_id, agency, project])
                deployment_name = '{}@{}'.format(agency, platform_id)
                LOGGER.warning('Deployment {} not found'.format(deployment_id))

                if verify_only:
                    LOGGER.info('Verify mode. Skipping deployment addition.')
                    deployment_ok = True
                elif self.add_deployment(bypass=bypass):
                    deployment_ok = True

                    msg = 'New deployment {} queued'.format(deployment_name)
                    self._warning(202, None, msg)
                else:
                    msg = 'Deployment {} not added. Skipping file.' \
                          .format(deployment_id)
                    LOGGER.warning(msg)

                    msg = 'No deployment {} found in registry' \
                        .format(deployment_id)
                    line = self.extcsv.line_num('PLATFORM') + 2
                    self._error(65, line, msg)

        LOGGER.debug('Validating instrument')
        if not all([dataset_ok, platform_ok]):
            LOGGER.warning('Skipping instrument check: depends on'
                           ' values with errors')
            instrument_ok = False
        else:
            instrument_model_ok = self.check_instrument_name_and_model()

            if not instrument_model_ok:
                LOGGER.warning('Instrument name and model failed to validate;'
                               ' aborting instrument checks')
                instrument_ok = False
            else:
                instrument_ok = self.check_instrument()

                if not instrument_ok:
                    # Attempt to fix the serial by left-stripping zeroes
                    old_serial = \
                        str(self.extcsv.extcsv['INSTRUMENT']['Number'])
                    new_serial = old_serial.lstrip('0') or '0'

                    if old_serial != new_serial:
                        LOGGER.debug('Attempting to search instrument serial'
                                     ' number {}'.format(new_serial))

                        self.extcsv.extcsv['INSTRUMENT']['Number'] = new_serial
                        instrument_ok = self.check_instrument()

                if not instrument_ok:
                    # Attempt to add a new record with the new serial number
                    # using name and model from the registry
                    LOGGER.warning('No instrument with serial {} found'
                                   ' in registry'.format(old_serial))
                    self.extcsv.extcsv['INSTRUMENT']['Number'] = old_serial

                    if verify_only:
                        LOGGER.info('Verify mode. Skipping instrument'
                                    ' addition.')
                        instrument_ok = True
                    else:
                        instrument_ok = self.add_instrument(verify=verify_only,
                                                            bypass=False)

                    if instrument_ok:
                        msg = 'New instrument serial number queued'
                        self._warning(201, None, msg)

        if not instrument_ok:
            msg = 'Failed to validate instrument against registry'
            line = self.extcsv.line_num('INSTRUMENT') + 2
            self._error(139, line, msg)

            location_ok = False
        else:
            location_ok = self.check_location()

        content_ok = self.check_content()
        data_generation_ok = self.check_data_generation()

        if not all([project_ok, dataset_ok, contributor_ok,
                    platform_ok, deployment_ok, instrument_ok,
                    location_ok, content_ok, data_generation_ok]):
            return False

        if metadata_only:
            msg = 'Lax mode detected. NOT validating dataset-specific tables'
            LOGGER.info(msg)
        else:
            dataset = self.extcsv.extcsv['CONTENT']['Category']
            dataset_validator = get_validator(dataset)

            time_series_ok = self.check_time_series()
            dataset_validated = dataset_validator.check_all(self.extcsv)

            if not all([time_series_ok, dataset_validated]):
                return False

        LOGGER.info('Validating data record')
        data_record = DataRecord(self.extcsv)
        data_record.ingest_filepath = infile
        data_record.filename = os.path.basename(infile)
        data_record.output_filepath = data_record.get_waf_path(
            config.WDR_WAF_BASEDIR)
        data_record.url = data_record.get_waf_path(config.WDR_WAF_BASEURL)
        self.process_end = datetime.utcnow()

        data_record_ok = self.check_data_record(data_record)

        if data_record_ok:
            LOGGER.info('Data record is valid and verified')
            self._registry_updates.append(data_record)
            self._search_index_updates.append(data_record)

        return data_record_ok

    def persist(self):
        """
        Publish all changes from the previous file parse to the data registry
        and ElasticSearch index, including instrument/deployment updates.
        Copies the input file to the WAF.

        :returns: void
        """

        data_records = []

        if not config.EXTRAS['processing']['registry_enabled']:
            LOGGER.info('Data registry persistence disabled, skipping.')
        else:
            LOGGER.info('Beginning persistence to data registry')
            for model in self._registry_updates:
                LOGGER.debug('Saving {} to registry'.format(str(model)))
                self.registry.save(model)

                if isinstance(model, DataRecord):
                    data_records.append(model)

        if not config.EXTRAS['processing']['search_index_enabled']:
            LOGGER.info('Search index persistence disabled, skipping.')
        else:
            LOGGER.info('Beginning persistence to search index')
            for model in self._search_index_updates:
                if not isinstance(model, DataRecord):
                    allow_update_model = True
                else:
                    # Do not persist older versions of data records.
                    esid = model.es_id
                    prev_version = self.search_index.get_record_version(esid)
                    now_version = model.data_generation_version

                    if prev_version or now_version > prev_version:
                        allow_update_model = True
                        data_records.append(model)
                    else:
                        allow_update_model = False

                if allow_update_model:
                    LOGGER.debug('Saving {} to search index'.format(model))
                    self.search_index.index(type(model),
                                            model.__geo_interface__)

        for record in data_records:
            LOGGER.info('Saving data record CSV to WAF')
            os.makedirs(os.path.dirname(record.output_filepath), exist_ok=True)
            shutil.copy2(record.ingest_filepath, record.output_filepath)

        LOGGER.info('Persistence complete')
        self._registry_updates = []
        self._search_index_updates = []

    def add_deployment(self, bypass=False):
        """
        Create a new deployment instance for the input Extended CSV file's
        #PLATFORM and #DATA_GENERATION.Agency. Queues the new deployment
        to be saved next time the publish method is called.

        Unless <bypass> is provided and True, there will be a permission
        prompt before a record is created. If permission is denied, no
        deployment will be queued and False will be returned.

        :param bypass: `bool` of whether to skip permission checks
                       to add the deployment.
        :returns: void
        """

        deployment = build_deployment(self.extcsv)

        if bypass:
            LOGGER.info('Bypass mode. Skipping permission check.')
            allow_add_deployment = True
        else:
            response = input('Deployment {} not found. Add? (y/n) [n]: '
                             .format(deployment.deployment_id))
            allow_add_deployment = response.lower() in ['y', 'yes']

        if not allow_add_deployment:
            return False
        else:
            LOGGER.info('Queueing new deployment...')

            self._registry_updates.append(deployment)
            self._search_index_updates.append(deployment)
            return True

    def add_station_name(self, bypass=False):
        """
        Create an alternative station name for the input Extended CSV file's
        #PLATFORM.Name and #PLATFORM.ID. Queues the new station name
        record to be saved next time the publish method is called.

        Unless <bypass> is provided and True, there will be a permission
        prompt before a record is created. If permission is denied, no
        station name will be queued and False will be returned.

        :param bypass: `bool` of whether to skip permission checks
                       to add the name.
        :returns: `bool` of whether the operation was successful.
        """

        station_name_object = build_station_name(self.extcsv)

        if bypass:
            LOGGER.info('Bypass mode. Skipping permission check')
            allow_add_station_name = True
        else:
            response = input('Station name {} not found. Add? (y/n) [n]: '
                             .format(station_name_object.station_name_id))
            allow_add_station_name = response.lower() in ['y', 'yes']

        if not allow_add_station_name:
            return False
        else:
            LOGGER.info('Queueing new station name...')

            self._registry_updates.append(station_name_object)
            return True

    def add_instrument(self, bypass=False):
        """
        Create a new instrument record from the input Extended CSV file's
        #INSTRUMENT table and queue it to be saved next time the publish
        method is called.

        Unless <bypass> is provided and True, there will be a permission
        prompt before a record is created. If permission is denied, no
        new instrument will be queued and False will be returned.

        :param bypass: `bool` of whether to skip permission checks
                       to add the instrument.
        :returns: `bool` of whether the operation was successful.
        """

        instrument = build_instrument(self.extcsv)

        if bypass:
            LOGGER.info('Bypass mode. Skipping permission check')
            allow_add_instrument = True
        else:
            response = input('Instrument {} not found. Add? (y/n) [n]: '
                             .format(instrument.instrument_id))
            allow_add_instrument = response.lower() in ['y', 'yes']

        if allow_add_instrument:
            LOGGER.info('Queueing new instrument...')

            self._registry_updates.append(instrument)
            self._search_index_updates.append(instrument)
            return True
        else:
            return False

    def check_project(self):
        """
        Validates the instance's Extended CSV source file's #CONTENT.Class,
        and returns True if no errors are found.

        :returns: `bool` of whether the input file's project
                  validated successfully.
        """

        project = self.extcsv.extcsv['CONTENT']['Class']

        LOGGER.debug('Validating project {}'.format(project))
        self.projects = self.registry.query_distinct(Project.project_id)

        if project in self.projects:
            LOGGER.debug('Match found for project {}'.format(project))
            return True
        else:
            msg = 'Project {} not found in registry'.format(project)
            line = self.extcsv.line_num('CONTENT') + 2

            self._error(53, line, msg)
            return False

    def check_dataset(self):
        """
        Validates the instance's Extended CSV source file's #CONTENT.Category,
        and returns True if no errors are found.

        Adjusts the Extended CSV contents if necessary to form a match.

        :returns: `bool` of whether the input file's dataset
                  validated successfully.
        """

        dataset = self.extcsv.extcsv['CONTENT']['Category']

        LOGGER.debug('Validating dataset {}'.format(dataset))
        dataset_model = {'dataset_id': dataset}

        fields = ['dataset_id']
        response = self.registry.query_multiple_fields(Dataset, dataset_model,
                                                       fields, fields)
        if response:
            LOGGER.debug('Match found for dataset {}'.format(dataset))
            self.extcsv.extcsv['CONTENT']['Category'] = response.dataset_id
            return True
        else:
            msg = 'Dataset {} not found in registry'.format(dataset)
            line = self.extcsv.line_num('CONTENT') + 2

            self._error(56, line, msg)
            return False

    def check_contributor(self):
        """
        Validates the instance's Extended CSV source file's
        #DATA_GENERATION.Agency, and returns True if no errors are found.

        Adjusts the Extended CSV contents if necessary to form a match.

        Prerequisite: #CONTENT.Class is a trusted value.

        :returns: `bool` of whether the input file's contributor
                  validated successfully.
        """

        agency = self.extcsv.extcsv['DATA_GENERATION']['Agency']
        project = self.extcsv.extcsv['CONTENT']['Class']

        if agency in ALIASES['Agency']:
            msg = 'Correcting agency {} to {} using alias table' \
                  .format(agency, ALIASES['Agency'][agency])
            line = self.extcsv.line_num('DATA_GENERATION') + 2
            self._warning(1000, line, msg)

            agency = ALIASES['Agency'][agency]
            self.extcsv.extcsv['DATA_GENERATION']['Agency'] = agency

        LOGGER.debug('Validating contributor {} under project {}'
                     .format(agency, project))
        contributor = {
            'contributor_id': '{}:{}'.format(agency, project),
            'project_id': project
        }

        fields = ['contributor_id']
        result = self.registry.query_multiple_fields(Contributor, contributor,
                                                     fields, fields)
        if result:
            contributor_name = result.acronym
            self.extcsv.extcsv['DATA_GENERATION']['Agency'] = contributor_name

            LOGGER.debug('Match found for contributor ID {}'
                         .format(result.contributor_id))
            return True
        else:
            msg = 'Contributor {} not found in registry' \
                  .format(contributor['contributor_id'])
            line = self.extcsv.line_num('DATA_GENERATION') + 2

            self._error(127, line, msg)
            return False

    def check_station(self, bypass=False, verify=False):
        """
        Validates the instance's Extended CSV source file's #PLATFORM table
        and returns True if no errors are found.

        Adjusts the Extended CSV contents if necessary to form a match.

        :param bypass: `bool` of whether to skip permission prompts
                        to add records.
        :param verify_only: `bool` of whether to verify the file for
                            correctness without processing.
        :returns: `bool` of whether the input file's station
                  validated successfully.
        """

        identifier = str(self.extcsv.extcsv['PLATFORM']['ID'])
        pl_type = self.extcsv.extcsv['PLATFORM']['Type']
        name = self.extcsv.extcsv['PLATFORM']['Name']
        country = self.extcsv.extcsv['PLATFORM']['Country']
        # gaw_id = self.extcsv.extcsv['PLATFORM'].get('GAW_ID', None)

        # TODO: consider adding and checking #PLATFORM_Type
        LOGGER.debug('Validating station {}:{}'.format(identifier, name))
        values_line = self.extcsv.line_num('PLATFORM') + 2

        water_codes = ['*IW', 'IW', 'XZ']
        if pl_type == 'SHP' and any([not country, country in water_codes]):
            msg = 'Ship #PLATFORM.Country = \'{}\' corrected to \'XY\'' \
                  ' to meet ISO-3166 standards'.format(country)
            line = self.extcsv.line_num('PLATFORM') + 2
            self._warning(105, line, msg)

            self.extcsv.extcsv['PLATFORM']['Country'] = country = 'XY'

        if len(identifier) < 3:
            msg = '#PLATFORM.ID {} is too short: left-padding with zeros' \
                  .format(identifier)
            self._warning(1000, values_line, msg)

            identifier = identifier.rjust(3, '0')
            self.extcsv.extcsv['PLATFORM']['ID'] = identifier

        station = {
            'station_id': identifier,
            'station_type': pl_type,
            'current_name': name,
            'country_id': country
        }

        LOGGER.debug('Validating station id...')
        response = self.registry.query_by_field(Station, 'station_id',
                                                identifier)
        if response:
            LOGGER.debug('Validated station with id: {}'.format(identifier))
        else:
            msg = 'Station {} not found in registry'.format(identifier)
            self._error(129, values_line, msg)
            return False

        LOGGER.debug('Validating station type...')
        platform_types = ['STN', 'SHP']
        type_ok = pl_type in platform_types

        if type_ok:
            LOGGER.debug('Validated station type {}'.format(type_ok))
        else:
            msg = 'Station type {} not found in registry'.format(pl_type)
            self._error(128, values_line, msg)

        LOGGER.debug('Validating station name...')
        model = {'station_id': identifier, 'name': station['current_name']}
        response = self.registry.query_multiple_fields(StationName, model,
                                                       model.keys(), ['name'])
        name_ok = bool(response)
        if name_ok:
            self.extcsv.extcsv['PLATFORM']['Name'] = name = response.name
            LOGGER.debug('Validated with name {} for id {}'.format(
                name, identifier))
        elif verify:
            LOGGER.info('Verify mode. Skipping station name addition.')
            name_ok = True
        elif self.add_station_name(bypass=bypass):
            LOGGER.info('Added new station name {}'
                        .format(station['current_name']))
            name_ok = True
        else:
            msg = 'Station name: {} did not match data for id: {}' \
                  .format(name, identifier)
            self._error(130, values_line, msg)

        LOGGER.debug('Validating station country...')
        fields = ['station_id', 'country_id']
        response = self.registry.query_multiple_fields(Station, station,
                                                       fields, ['country_id'])
        country_ok = bool(response)
        if country_ok:
            country = response.country
            self.extcsv.extcsv['PLATFORM']['Country'] = country.country_id
            LOGGER.debug('Validated with country: {} ({}) for id: {}'
                         .format(country.name_en, country.country_id,
                                 identifier))
        else:
            msg = 'Station country: {} did not match data for id: {}' \
                  .format(country, identifier)
            self._error(131, values_line, msg)

        return type_ok and name_ok and country_ok

    def check_deployment(self):
        """
        Validates the instance's Extended CSV source file's combination of
        #DATA_GENERATION.Agency and #PLATFORM.ID, and returns True if no
        errors are found.

        Updates the deployment's start and end date if a match is found.

        Prerequisite: #DATA_GENERATION.Agency,
                      #PLATFORM_ID, and
                      #CONTENT.Class are all trusted values.

        :returns: `bool` of whether the input file's station-contributor
                  pairing validated successfully.
        """

        station = str(self.extcsv.extcsv['PLATFORM']['ID'])
        agency = self.extcsv.extcsv['DATA_GENERATION']['Agency']
        project = self.extcsv.extcsv['CONTENT']['Class']
        timestamp_date = self.extcsv.extcsv['TIMESTAMP']['Date']

        deployment_id = ':'.join([station, agency, project])
        deployment = self.registry.query_by_field(Deployment, 'deployment_id',
                                                 deployment_id)
        if not deployment:
            LOGGER.warning('Deployment {} not found'.format(deployment_id))
            return False
        else:
            LOGGER.debug('Found deployment match for {}'
                         .format(deployment_id))
            if deployment.start_date > timestamp_date:
                deployment.start_date = timestamp_date
                self._registry_updates.append(deployment)
                LOGGER.debug('Deployment start date updated.')
            elif deployment.end_date and deployment.end_date < timestamp_date:
                deployment.end_date = timestamp_date
                self._registry_updates.append(deployment)
                LOGGER.debug('Deployment end date updated.')
            return True

    def check_instrument_name_and_model(self):
        """
        Validates the instance's Extended CSV source vile's #INSTRUMENT.Name
        and #INSTRUMENT.Model and returns True if no errors are found.

        Adjusts the Extended CSV contents if necessary to form a match.

        :returns: `bool` of whether the input file's instrument name and model
                  validated successfully.
        """

        name_ok = True
        model_ok = True

        name = self.extcsv.extcsv['INSTRUMENT']['Name']
        model = self.extcsv.extcsv['INSTRUMENT']['Model']

        instrument_valueline = self.extcsv.line_num('INSTRUMENT') + 2

        if not name or name.lower() in ['na', 'n/a']:
            msg = '#INSTRUMENT.Name is null or empty'
            self._error(72, instrument_valueline, msg)
            name_ok = False

            self.extcsv.extcsv['INSTRUMENT']['Name'] = name = 'UNKNOWN'
        if not model or str(model).lower() in ['na', 'n/a']:
            msg = '#INSTRUMENT.Model is null or empty'
            self._error(1000, instrument_valueline, msg)
            model_ok = False

            self.extcsv.extcsv['INSTRUMENT']['Model'] = model = 'na'

        if not name_ok or not model_ok:
            return False

        LOGGER.debug('Casting name and model to string for further checking')
        name = str(name)
        model = str(model)

        # Check data registry for matching instrument name
        instrument = self.registry.query_by_field(Instrument, 'name', name,
                                                  case_insensitive=True)
        if instrument:
            name = instrument.name
            self.extcsv.extcsv['INSTRUMENT']['Name'] = instrument.name
        else:
            msg = 'No match found for #INSTRUMENT.Name = {}'.format(name)
            self._error(1000, instrument_valueline, msg)
            name_ok = False

        # Check data registry for matching instrument model
        instrument = self.registry.query_by_field(Instrument, 'model', model,
                                                  case_insensitive=True)
        if instrument:
            model = instrument.model
            self.extcsv.extcsv['INSTRUMENT']['Model'] = instrument.model
        else:
            msg = 'No match found for #INSTRUMENT.Model = {}'.format(model)
            self._error(1000, instrument_valueline, msg)
            model_ok = False

        return name_ok and model_ok

    def check_instrument(self):
        """
        Validates the instance's Extended CSV source file's #INSTRUMENT table
        and returns True if no errors are found.

        Adjusts the Extended CSV contents if necessary to form a match.

        Prerequisite: #INSTRUMENT.Name,
                      #INSTRUMENT.Model,
                      #PLATFORM.ID and
                      #CONTENT.Category are all trusted values.

        :returns: `bool` of whether the input file's instrument collectively
                  validated successfully.
        """

        serial = self.extcsv.extcsv['INSTRUMENT']['Number']
        if not serial or str(serial).lower() in ['na', 'n/a']:
            self.extcsv.extcsv['INSTRUMENT']['Number'] = serial = 'na'

        instrument = build_instrument(self.extcsv)
        fields = ['name', 'model', 'serial', 'station_id', 'dataset_id']
        case_insensitive = ['name', 'model', 'serial']

        model = {field: getattr(instrument, field) for field in fields}
        response = self.registry.query_multiple_fields(
            Instrument, model, fields, case_insensitive)

        if not response:
            LOGGER.warning('No instrument {} found in registry'
                           .format(instrument.instrument_id))
            return False
        else:
            LOGGER.debug('Found instrument match for {}'
                         .format(instrument.instrument_id))

            self.extcsv.extcsv['INSTRUMENT']['Number'] = response.serial
            return True

    def check_location(self):
        """
        Validates the instance's Extended CSV source file's #LOCATION table
        against the location of the instrument from the file, and returns
        True if no errors are found.

        :returns: `bool` of whether the input file's location
                  validated successfully.
        """

        instrument_id = build_instrument(self.extcsv).instrument_id

        process_config = config.EXTRAS['processing']

        lat = self.extcsv.extcsv['LOCATION']['Latitude']
        lon = self.extcsv.extcsv['LOCATION']['Longitude']
        height = self.extcsv.extcsv['LOCATION'].get('Height', None)
        values_line = self.extcsv.line_num('LOCATION') + 2

        try:
            lat_numeric = float(lat)
            if -90 <= lat_numeric <= 90:
                LOGGER.debug('Validated instrument latitude')
            else:
                msg = '#LOCATION.Latitude is not within the range [-90]-[90]'
                self._warning(76, values_line, msg)
            lat_ok = True
        except ValueError:
            msg = '#LOCATION.Latitude contains invalid characters'
            LOGGER.error(msg)
            self._error(75, values_line, msg)
            lat_numeric = None
            lat_ok = False

        try:
            lon_numeric = float(lon)
            if -180 <= lon_numeric <= 180:
                LOGGER.debug('Validated instrument longitude')
            else:
                msg = '#LOCATION.Longitude is not within' \
                      ' allowable range [-180]-[180]'
                self._warning(76, values_line, msg)
            lon_ok = True
        except ValueError:
            msg = '#LOCATION.Longitude contains invalid characters'
            LOGGER.error(msg)
            self._error(75, values_line, msg)
            lon_numeric = None
            lon_ok = False

        try:
            height_numeric = float(height) if height else None
            if not height or -50 <= height_numeric <= 5100:
                LOGGER.debug('Validated instrument height')
            else:
                msg = '#LOCATION.Height is not within the range [-50]-[5100]'
                self._warning(76, values_line, msg)
        except ValueError:
            msg = '#LOCATION.Height contains invalid characters'
            self._warning(75, values_line, msg)
            height_numeric = None

        station_type = self.extcsv.extcsv['PLATFORM'].get('Type', 'STN')
        ignore_ships = not process_config['ships_ignore_location']

        if not all([lat_ok, lon_ok]):
            return False
        elif station_type == 'SHP' and ignore_ships:
            LOGGER.debug('Not validating shipboard instrument location')
            return True
        elif instrument_id is not None:
            instrument = self.registry.query_by_field(Instrument,
                                                      'instrument_id',
                                                    instrument_id)
            if not instrument:
                return True

            lat_interval = process_config['latitude_error_distance']
            lon_interval = process_config['longitude_error_distance']
            height_interval = process_config['height_error_distance']

            polar_latitude_range = process_config['polar_latitude_range']
            ignore_polar_lon = process_config['polar_ignore_longitude']

            in_polar_region = lat_numeric is not None \
                and abs(lat_numeric) > 90 - polar_latitude_range

            if lat_numeric is not None and instrument.y is not None \
               and abs(lat_numeric - instrument.y) >= lat_interval:
                lat_ok = False
                msg = '#LOCATION.Latitude in file does not match database'
                LOGGER.error(msg)
                self._error(77, values_line, msg)
            if lon_numeric is not None and instrument.x is not None:
                if in_polar_region and ignore_polar_lon:
                    LOGGER.info('Skipping longitude check in polar region')
                elif abs(lon_numeric - instrument.x) >= lon_interval:
                    lon_ok = False
                    msg = '#LOCATION.Longitude in file does not match database'
                    LOGGER.error(msg)
                    self._error(77, values_line, msg)
            if height_numeric is not None and instrument.z is not None \
               and abs(height_numeric - instrument.z) >= height_interval:
                msg = '#LOCATION.Height in file does not match database'
                self._warning(77, values_line, msg)

        return all([lat_ok, lon_ok])

    def check_content(self):
        """
        Validates the instance's Extended CSV source file's #CONTENT.Level
        and #CONTENT.Form by comparing them to other tables. Returns
        True if no errors were encountered.

        Fill is the Extended CSV with missing values if possible.

        Prerequisite: #CONTENT.Category is a trusted value.

        :returns: `bool` of whether the input file's #CONTENT table
                  collectively validated successfully.
        """

        dataset = self.extcsv.extcsv['CONTENT']['Category']
        level = self.extcsv.extcsv['CONTENT']['Level']
        form = self.extcsv.extcsv['CONTENT']['Form']

        level_ok = True
        form_ok = True
        values_line = self.extcsv.line_num('CONTENT') + 2

        if not level:
            if dataset == 'UmkehrN14' and 'C_PROFILE' in self.extcsv.extcsv:
                msg = 'Missing #CONTENT.Level, resolved to 2.0'
                self._warning(59, values_line, msg)

                self.extcsv.extcsv['CONTENT']['Level'] = level = 2.0
            else:
                msg = 'Missing #CONTENT.Level, resolved to 1.0'
                self._warning(58, values_line, msg)

                self.extcsv.extcsv['CONTENT']['Level'] = level = 1.0
        elif not isinstance(level, float):
            try:
                msg = '#CONTENT.Level = {}, corrected to {}' \
                      .format(level, float(level))
                self._warning(57, values_line, msg)

                self.extcsv.extcsv['CONTENT']['Level'] = level = float(level)
            except ValueError:
                msg = 'Invalid characters in #CONTENT.Level'
                self._error(60, values_line, msg)
                level_ok = False

        if str(level) not in DOMAINS['Datasets'][dataset]:
            msg = 'Unknown #CONTENT.Level for category {}'.format(dataset)
            self._error(60, values_line, msg)
            level_ok = False

        if not isinstance(form, int):
            try:
                msg = '#CONTENT.Form = {}, corrected to {}' \
                      .format(form, int(form))
                self._warning(61, values_line, msg)

                self.extcsv.extcsv['CONTENT']['Form'] = form = int(form)
            except ValueError:
                msg = 'Cannot resolve #CONTENT.Form: is empty or' \
                      ' has invalid characters'
                self._error(62, values_line, msg)
                form_ok = False

        return level_ok and form_ok

    def check_data_generation(self):
        """
        Validates the instance's Extended CSV source file's
        #DATA_GENERATION.Date and #DATA_GENERATION.Version by comparison
        with other tables. Returns True if no errors were encountered.

        Fill in the Extended CSV with missing values if possible.

        :returns: `bool` of whether the input file's #DATA_GENERATION table
                  collectively validated successfully.
        """

        dg_date = self.extcsv.extcsv['DATA_GENERATION'].get('Date', None)
        version = self.extcsv.extcsv['DATA_GENERATION'].get('Version', None)
        version_ok = True

        values_line = self.extcsv.line_num('DATA_GENERATION')

        if not dg_date:
            msg = 'Missing #DATA_GENERATION.Date, resolved to processing date'
            self._warning(63, values_line, msg)

            kwargs = {key: getattr(self.process_start, key)
                      for key in ['year', 'month', 'day']}
            today_date = datetime(**kwargs)

            self.extcsv.extcsv['DATA_GENERATION']['Date'] = today_date
            dg_date = today_date

        try:
            numeric_version = float(version)
        except TypeError:
            msg = 'Empty #DATA_GENERATION.Version, defaulting to 1.0'
            self._warning(1000, values_line, msg)

            self.extcsv.extcsv['DATA_GENERATION']['Version'] = version = '1.0'
            numeric_version = 1.0
        except ValueError:
            try:
                while version.count('.') > 1 and version.endswith('.0'):
                    version = version[:-2]
                numeric_version = float(version)
            except ValueError:
                msg = '#DATA_GENERATION.Version contains invalid characters'
                self._error(68, values_line, msg)
                version_ok = False

        if version_ok:
            if not 0 <= numeric_version <= 20:
                msg = '#DATA_GENERATION.Version is not within' \
                      ' allowable range [0.0]-[20.0]'
                self._warning(66, values_line, msg)
            if str(version) == str(int(numeric_version)):
                self.extcsv.extcsv['DATA_GENERATION']['Version'] = \
                    numeric_version

                msg = '#DATA_GENERATION.Version corrected to one decimal place'
                self._warning(67, values_line, msg)

        return version_ok

    def check_time_series(self):
        """
        Validate the input Extended CSV source file's dates across all tables
        to ensure that no date is more recent that #DATA_GENERATION.Date.

        :returns: `bool` of whether the input file's time fields collectively
                  validated successfully.
        """

        dg_date = self.extcsv.extcsv['DATA_GENERATION']['Date']
        ts_time = self.extcsv.extcsv['TIMESTAMP'].get('Time', None)

        dates_ok = True
        err_base = '#{}.Date cannot be more recent than #DATA_GENERATION.Date'

        for table, body in self.extcsv.extcsv.items():
            if table == 'DATA_GENERATION':
                continue

            values_line = self.extcsv.line_num(table) + 2

            date_column = body.get('Date', [])
            if not isinstance(date_column, list):
                date_column = [date_column]

            for line, other_date in enumerate(date_column, values_line):
                if other_date > dg_date:
                    err_code = 31 if table.startswith('TIMESTAMP') else 32
                    msg = err_base.format(table)
                    self._error(err_code, line, msg)

            time_column = body.get('Time', [])
            if not isinstance(time_column, list):
                time_column = [time_column]

            if ts_time:
                for line, other_time in enumerate(time_column, values_line):
                    if other_time and other_time < ts_time:
                        msg = 'First #TIMESTAMP.Time cannot be more recent' \
                              ' than other time(s)'
                        self._warning(22, line, msg)

        return dates_ok

    def check_data_record(self, data_record):
        """
        Validate the data record made from the input Extended CSV file,
        and look for collisions with any previous submissions of the
        same data.

        Prerequisite: #DATA_GENERATION.Date,
                      #DATA_GENERATION.Version,
                      #INSTRUMENT.Name,
                      and #INSTRUMENT.Number are all trusted values
                      and self.data_record exists.

        :returns: `bool` of whether the data record metadata validated
                  successfully.
        """

        dg_date = self.extcsv.extcsv['DATA_GENERATION']['Date']
        version = self.extcsv.extcsv['DATA_GENERATION']['Version']
        dg_valueline = self.extcsv.line_num('DATA_GENERATION') + 2

        id_components = data_record.data_record_id.split(':')
        id_components[-1] = r'.*\..*'  # Pattern for floating-point number
        identifier_pattern = ':'.join(id_components)

        LOGGER.debug('Verifying if URN already exists')

        response = self.registry.query_by_pattern(
            DataRecord, 'data_record_id', identifier_pattern)

        if not response:
            return True

        old_dg_date = response.data_generation_date
        old_version = float(response.data_generation_version)

        dg_date_equal = dg_date == old_dg_date
        dg_date_before = dg_date < old_dg_date
        version_equal = version == old_version

        dg_date_ok = True
        version_ok = True

        if dg_date_before:
            msg = 'Submitted file #DATA_GENERATION.Date is earlier than' \
                  ' previously submitted version'
            self._error(103, dg_valueline, msg)
            dg_date_ok = False
        elif dg_date_equal and version_equal:
            msg = 'Submitted file version and #DATA_GENERATION.Date' \
                  ' identical to previously submitted file'
            self._error(102, dg_valueline, msg)
            dg_date_ok = version_ok = False
        elif dg_date_equal:
            msg = 'Submitted file #DATA_GENERATION.Date identical to' \
                  ' previously submitted version'
            self._error(143, dg_valueline, msg)
            dg_date_ok = False
        elif version_equal:
            msg = 'Submitted version number identical to previous file'
            self._error(141, dg_valueline, msg)
            version_ok = False

        instrument_name = self.extcsv.extcsv['INSTRUMENT']['Name']
        instrument_serial = self.extcsv.extcsv['INSTRUMENT']['Number']
        old_serial = response.instrument.serial

        if instrument_name == 'ECC' and instrument_serial != old_serial:
            msg = 'ECC instrument serial number different from previous file'
            instrument_valueline = self.extcsv.line_num('INSTRUMENT') + 2
            self._warning(145, instrument_valueline, msg)

        return dg_date_ok and version_ok


class ProcessingError(Exception):
    """custom exception handler"""
    pass
