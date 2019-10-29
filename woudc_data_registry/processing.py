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
from woudc_data_registry.util import read_file

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

    def __init__(self, registry_conn, search_engine_conn):
        """constructor"""

        self.status = None
        self.code = None
        self.message = None
        self.process_start = datetime.utcnow()
        self.process_end = None
        self.registry = registry_conn
        self.search_engine = search_engine_conn

        self._registry_updates = []
        self._search_engine_updates = []
        self.data_record = None

        self.warnings = []
        self.errors = []

    def _warning(self, error_code, line, message=None):
        """
        Record <message> as an error with code <error_code> that took place
        at line <line> in the input file.
        """

        LOGGER.warning(message)
        self.warnings.append((error_code, message, line))

    def _error(self, error_code, line, message=None):
        """
        Record <message> as an error with code <error_code> that took place
        at line <line> in the input file.
        """

        LOGGER.error(message)
        self.errors.append((error_code, message, line))

    def validate(self, infile, core_only=False, bypass=False):
        """
        Process incoming data record.

        :param infile: Path to incoming data file.
        :param core_only: Whether to only verify core metadata tables.
        :param bypass: Whether to skip permission prompts to add records.

        :returns: `bool` of processing result
        """

        # detect incoming data file
        data = None
        self.extcsv = None

        self.warnings = []
        self.errors = []

        LOGGER.info('Processing file {}'.format(infile))
        LOGGER.info('Detecting file')

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
            if not core_only:
                self.extcsv.validate_dataset_tables()
            LOGGER.info('Valid Extended CSV')
        except NonStandardDataError as err:
            self.status = 'failed'
            self.code = 'NonStandardDataError'
            self.message = err
            LOGGER.error('Invalid Extended CSV: {}'.format(str(err)).strip())
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

        contributor_ok = self.check_contributor()
        platform_ok = self.check_station()

        LOGGER.debug('Validating agency deployment')
        deployment_ok = self.check_deployment()

        project = self.extcsv.extcsv['CONTENT']['Class']
        agency = self.extcsv.extcsv['DATA_GENERATION']['Agency']
        platform_id = str(self.extcsv.extcsv['PLATFORM']['ID'])

        if not deployment_ok:
            deployment_id = ':'.join([platform_id, agency, project])
            deployment_name = '{}@{}'.format(agency, platform_id)
            LOGGER.warning('Deployment {} not found'.format(deployment_id))

            if bypass:
                LOGGER.info('Bypass mode. Skipping permission check')
                permission = True
            else:
                response = input('Deployment {} not found. Add? [y/n] '
                                 .format(deployment_name))
                permission = response.lower() in ['y', 'yes']

            if permission:
                self.add_deployment()
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
        instrument_ok = self.check_instrument()

        if not instrument_ok:
            # Attempt to fix the serial by left-stripping zeroes
            old_serial = str(self.extcsv.extcsv['INSTRUMENT']['Number'])
            new_serial = old_serial.lstrip('0') or '0'

            if old_serial != new_serial:
                LOGGER.debug('Attempting to search instrument serial number'
                             ' {}'.format(new_serial))

                self.extcsv.extcsv['INSTRUMENT']['Number'] = new_serial
                instrument_ok = self.check_instrument()

        if not instrument_ok:
            # Attempt to add a new record with the new serial number
            # using name and model from the registry
            LOGGER.warning('No instrument with serial {} found in registry'
                           .format(old_serial))
            self.extcsv.extcsv['INSTRUMENT']['Number'] = old_serial
            instrument_ok = self.add_instrument()

            if instrument_ok:
                msg = 'New instrument serial number queued'
                self._warning(201, None, msg)

        instrument_args = [
            self.extcsv.extcsv['INSTRUMENT']['Name'],
            str(self.extcsv.extcsv['INSTRUMENT']['Model']),
            str(self.extcsv.extcsv['INSTRUMENT']['Number']),
            str(self.extcsv.extcsv['PLATFORM']['ID']),
            self.extcsv.extcsv['CONTENT']['Category']]
        instrument_id = ':'.join(instrument_args)

        if not instrument_ok:
            # Attempt to force the new instrument name/model into the registry
            response = input('Instrument {} not found. Add? [y/n] '
                             .format(instrument_id))
            if response.lower() in ['y', 'yes']:
                if self.add_instrument(force=True):
                    msg = 'New instrument name, model, and serial queued'
                    self._warning(1000, None, msg)

                    instrument_ok = True

        if instrument_ok:
            location_ok = self.check_location(instrument_id)
        else:
            msg = 'Failed to validate instrument against registry'
            line = self.extcsv.line_num('INSTRUMENT') + 2
            self._error(139, line, msg)

            location_ok = False

        content_ok = self.check_content()
        data_generation_ok = self.check_data_generation()

        if not all([project_ok, dataset_ok, contributor_ok,
                    platform_ok, deployment_ok, instrument_ok,
                    location_ok, content_ok, data_generation_ok]):
            return False

        if core_only:
            msg = 'Core mode detected. NOT validating dataset-specific tables'
            LOGGER.info(msg)
        else:
            time_series_ok = self.check_time_series()
            if not time_series_ok:
                return False

            dataset = self.extcsv.extcsv['CONTENT']['Category']
            dataset_validator = get_validator(dataset)

            dataset_validated = dataset_validator.check_all(self.extcsv)
            if not dataset_validated:
                return False

        LOGGER.info('Validating data record')
        self.data_record = DataRecord(self.extcsv)
        self.data_record.ingest_filepath = infile
        self.data_record.filename = os.path.basename(infile)
        self.data_record.url = self.data_record.get_waf_path(
            config.WDR_WAF_BASEURL)
        self.process_end = datetime.utcnow()

        LOGGER.debug('Verifying if URN already exists')
        results = self.registry.query_by_field(
            DataRecord, 'data_record_id', self.data_record.data_record_id)

        if results:
            msg = 'Data exists'
            self.status = 'failed'
            self.code = 'ProcessingError'
            self.message = msg
            LOGGER.error(msg)
            return False

        LOGGER.info('Data record is valid and verified')
        return True

    def persist(self):
        """
        Publish all changes from the previous file parse to the data registry
        and ElasticSearch index, including instrument/deployment updates.
        Copies the input file to the WAF.
        """

        LOGGER.info('Beginning persistence to data registry')
        for model in self._registry_updates:
            LOGGER.debug('Saving {} to registry'.format(str(model)))
            self.registry.save(model)

        # TODO
        # LOGGER.info('Beginning persistence to search engine')
        # for model in self._search_engine_updates:
        #     LOGGER.debug('Saving {} to search engine')

        LOGGER.info('Saving data record to registry')
        self.registry.save(self.data_record)

        prev_version = \
            self.search_engine.get_record_version(self.data_record.es_id)
        if not prev_version \
           or self.data_record.data_generation_version > prev_version:
            LOGGER.info('Saving data record to search index')
            self.search_engine.index_data_record(
                self.data_record.__geo_interface__)

        LOGGER.info('Saving data record CSV to WAF')
        waf_filepath = self.data_record.get_waf_path(config.WDR_WAF_BASEDIR)
        os.makedirs(os.path.dirname(waf_filepath), exist_ok=True)
        shutil.copy2(self.data_record.ingest_filepath, waf_filepath)

        LOGGER.info('Persistence complete')
        self._registry_updates = []
        self._search_index_updates = []

    def add_deployment(self):
        """
        Create a new deployment instance for the input Extended CSV file's
        #PLATFORM and #DATA_GENERATION.Agency. Queues the new deployment
        to be saved next time the publish method is called.
        """

        station = str(self.extcsv.extcsv['PLATFORM']['ID'])
        agency = self.extcsv.extcsv['DATA_GENERATION']['Agency']
        project = self.extcsv.extcsv['CONTENT']['Class']
        date = self.extcsv.extcsv['TIMESTAMP']['Date']

        contributor_id = ':'.join([agency, project])
        deployment_id = ':'.join([station, agency, project])
        deployment_model = {
            'identifier': deployment_id,
            'station_id': station,
            'contributor_id': contributor_id,
            'start_date': date,
            'end_date': date
        }

        deployment = Deployment(deployment_model)
        self._registry_updates.append(deployment)

    def add_station_name(self, bypass=False):
        """
        Create an alternative station name for the input Extended CSV file's
        #PLATFORM.Name and #PLATFORM.ID. Queues the new station name
        record to be saved next time the publish method is called.

        Unless <bypass> is provided and True, there will be a permission
        prompt before a record is created. If permission is denied, no
        station name will be queued and False will be returned.

        :param bypass: Whether to skip permission checks to add the name.
        :returns: Whether the operation was successful.
        """

        station_id = str(self.extcsv.extcsv['PLATFORM']['ID'])
        station_name = self.extcsv.extcsv['PLATFORM']['Name']
        name_id = '{}:{}'.format(station_id, station_name)

        if bypass:
            LOGGER.info('Bypass mode. Skipping permission check')
            permission = True
        else:
            response = input('Station name {} not found. Add? [y/n] '
                             .format(name_id))
            permission = response.lower() in ['y', 'yes']

        if not permission:
            return False
        else:
            observation_time = self.extcsv.extcsv['TIMESTAMP']['Date']
            model = {
                'identifier': name_id,
                'station_id': station_id,
                'name': station_name,
                'first_seen': observation_time,
                'last_seen': observation_time
            }

            station_name_object = StationName(model)
            self._registry_updates.append(station_name_object)
            return True

    def add_instrument(self, force=False):
        """
        Create a new instrument record from the input Extended CSV file's
        #INSTRUMENT table and queue it to be saved next time the publish
        method is called.

        Unless <force> is True, the operation will only complete if the
        instrument's name and model are both in the registry already.
        Returns whether the operation was successful.

        :param force: Whether to insert if name or model are not found
                      in the registry.
        :returns: Whether the operation was successful.
        """

        name = self.extcsv.extcsv['INSTRUMENT']['Name']
        model = str(self.extcsv.extcsv['INSTRUMENT']['Model'])
        serial = str(self.extcsv.extcsv['INSTRUMENT']['Number'])
        station = str(self.extcsv.extcsv['PLATFORM']['ID'])
        dataset = self.extcsv.extcsv['CONTENT']['Category']
        location = [self.extcsv.extcsv['LOCATION'].get(f, None)
                    for f in ['Longitude', 'Latitude', 'Height']]

        instrument_id = ':'.join([name, model, serial, station, dataset])
        model = {
            'identifier': instrument_id,
            'name': name,
            'model': model,
            'serial': serial,
            'station_id': station,
            'dataset_id': dataset,
            'x': location[0],
            'y': location[1],
            'z': location[2]
        }

        if force:
            LOGGER.debug('Force-adding instrument. Skipping name/model check')
            permission = True
        else:
            fields = ['name', 'model', 'station_id', 'dataset_id']
            case_insensitive = ['name', 'model']
            response = self.registry.query_multiple_fields(Instrument, model,
                                                           fields,
                                                           case_insensitive)
            if response:
                model['name'] = response.name
                model['model'] = response.model
                self.extcsv.extcsv['INSTRUMENT']['Name'] = response.name
                self.extcsv.extcsv['INSTRUMENT']['Model'] = response.model

                LOGGER.debug('All other instrument data matches.')
                permission = True
            else:
                permission = False

        if permission:
            LOGGER.info('Creating instrument with new serial number...')

            instrument = Instrument(model)
            self._registry_updates.append(instrument)
            self._search_engine_updates.append(instrument)
            return True
        else:
            return False

    def check_project(self):
        """
        Validates the instance's Extended CSV source file's #CONTENT.Class,
        and returns True if no errors are found.
        """

        project = self.extcsv.extcsv['CONTENT']['Class']

        LOGGER.debug('Validating project {}'.format(project))
        self.projects = self.registry.query_distinct(Project.project_id)

        if not project:
            msg = 'Missing #CONTENT.Class: default to \'WOUDC\''
            line = self.extcsv.line_num('CONTENT') + 2
            self._warning(52, line, msg)

            self.extcsv.extcsv['CONTENT']['Class'] = project = 'WOUDC'

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

    def check_station(self):
        """
        Validates the instance's Extended CSV source file's #PLATFORM table
        and returns True if no errors are found.

        Adjusts the Extended CSV contents if necessary to form a match.
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
        elif self.add_station_name():
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
        """

        station = str(self.extcsv.extcsv['PLATFORM']['ID'])
        agency = self.extcsv.extcsv['DATA_GENERATION']['Agency']
        project = self.extcsv.extcsv['CONTENT']['Class']
        date = self.extcsv.extcsv['TIMESTAMP']['Date']

        deployment_id = ':'.join([station, agency, project])
        results = self.registry.query_by_field(Deployment, 'deployment_id',
                                               deployment_id)
        if not results:
            LOGGER.warning('Deployment {} not found'.format(deployment_id))
            return False
        else:
            deployment = results[0]
            LOGGER.debug('Found deployment match for {}'
                         .format(deployment_id))
            if deployment.start_date > date:
                deployment.start_date = date
                self._registry_updates.append(deployment)
                LOGGER.debug('Deployment start date updated.')
            elif deployment.end_date and deployment.end_date < date:
                deployment.end_date = date
                self._registry_updates.append(deployment)
                LOGGER.debug('Deployment end date updated.')
            return True

    def check_instrument(self):
        """
        Validates the instance's Extended CSV source file's #INSTRUMENT table
        and returns True if no errors are found.

        Adjusts the Extended CSV contents if necessary to form a match.

        Prerequisite: #PLATFORM.ID and
                      #CONTENT.Category are all trusted values.
        """

        name = self.extcsv.extcsv['INSTRUMENT']['Name']
        model = self.extcsv.extcsv['INSTRUMENT']['Model']
        serial = self.extcsv.extcsv['INSTRUMENT']['Number']
        station = str(self.extcsv.extcsv['PLATFORM']['ID'])
        dataset = self.extcsv.extcsv['CONTENT']['Category']

        if not name or name.lower() in ['na', 'n/a']:
            self.extcsv.extcsv['INSTRUMENT']['Name'] = name = 'UNKNOWN'
        if not model or str(model).lower() in ['na', 'n/a']:
            self.extcsv.extcsv['INSTRUMENT']['Model'] = model = 'na'
        if not serial or str(serial).lower() in ['na', 'n/a']:
            self.extcsv.extcsv['INSTRUMENT']['Number'] = serial = 'na'

        model = str(model)
        serial = str(serial)

        values_line = self.extcsv.line_num('INSTRUMENT') + 2
        if name == 'UNKNOWN':
            msg = '#INSTRUMENT.Name must not be null'
            self._error(72, values_line, msg)
            return False

        instrument_id = ':'.join([name, model, serial, station, dataset])
        instrument = {
            'name': name,
            'model': model,
            'serial': serial,
            'station_id': station,
            'dataset_id': dataset
        }

        fields = list(instrument.keys())
        case_insensitive = ['name', 'model', 'serial']
        result = self.registry.query_multiple_fields(Instrument, instrument,
                                                     fields, case_insensitive)
        if not result:
            LOGGER.warning('No instrument {} found in registry'
                           .format(instrument_id))
            return False
        else:
            LOGGER.debug('Found instrument match for {}'
                         .format(instrument_id))

            self.extcsv.extcsv['INSTRUMENT']['Name'] = result.name
            self.extcsv.extcsv['INSTRUMENT']['Model'] = result.model
            self.extcsv.extcsv['INSTRUMENT']['Number'] = result.serial
            return True

    def check_location(self, instrument_id):
        """
        Validates the instance's Extended CSV source file's #LOCATION table
        against the location of the instrument with ID <instrument_id>
        and returns True if no errors are found.
        """

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
        if not all([lat_ok, lon_ok]):
            return False
        elif station_type == 'SHP':
            LOGGER.debug('Not validating shipboard instrument location')
            return True
        elif instrument_id is not None:
            result = self.registry.query_by_field(Instrument, 'instrument_id',
                                                  instrument_id)
            if not result:
                return True

            instrument = result[0]
            if lat_numeric is not None and instrument.y is not None \
               and abs(lat_numeric - instrument.y) >= 1:
                lat_ok = False
                msg = '#LOCATION.Latitude in file does not match database'
                LOGGER.error(msg)
                self._error(77, values_line, msg)
            if lon_numeric is not None and instrument.x is not None \
               and abs(lon_numeric - instrument.x) >= 1:
                lon_ok = False
                msg = '#LOCATION.Longitude in file does not match database'
                LOGGER.error(msg)
                self._error(77, values_line, msg)
            if height_numeric is not None and instrument.z is not None \
               and abs(height_numeric - instrument.z) >= 1:
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

        if level not in DOMAINS['Datasets'][dataset]:
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
            self.extcsv.extcsv['DATA_GENERATION']['Date'] = date = today_date

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
        Validates the input Extended CSV source file's dates across all tables
        to ensure that no date is more recent that #DATA_GENERATION.Date.
        """

        dg_date = self.extcsv.extcsv['DATA_GENERATION']['Date']
        ts_time = self.extcsv.extcsv['TIMESTAMP']['Time']

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


class ProcessingError(Exception):
    """custom exception handler"""
    pass
