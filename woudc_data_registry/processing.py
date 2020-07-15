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
from woudc_data_registry.parser import DOMAINS
from woudc_data_registry.dataset_validators import get_validator

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

    def __init__(self, registry_conn, search_index_conn, reporter):
        """constructor"""

        self.status = None
        self.code = None
        self.message = None
        self.process_start = datetime.utcnow()
        self.process_end = None

        self.registry = registry_conn
        self.search_index = search_index_conn
        self.reports = reporter

        self._registry_updates = []
        self._search_index_updates = []

        self.warnings = []
        self.errors = []

    def _add_to_report(self, error_code, line=None, **kwargs):
        """
        Submit a warning or error of code <error_code> to the report generator,
        with was found at line <line> in the input file. Uses keyword arguments
        to detail the warning/error message.

        Returns False iff the error is serious enough to abort parsing.
        """

        message, severe = self.reports.add_message(error_code, line, **kwargs)
        if severe:
            LOGGER.error(message)
            self.errors.append(message)
        else:
            LOGGER.warning(message)
            self.warnings.append(message)

        return not severe

    def validate(self, extcsv, metadata_only=False, verify_only=False,
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
        self.extcsv = extcsv

        self.warnings = []
        self.errors = []

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

                    self._add_to_report(202)
                else:
                    msg = 'Deployment {} not added. Skipping file.' \
                          .format(deployment_name)
                    LOGGER.warning(msg)

                    line = self.extcsv.line_num('PLATFORM') + 2
                    deployment_ok = self._add_to_report(88, line,
                                                        ident=deployment_id)

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
                        instrument_ok = self.add_instrument(bypass=False)

                    if instrument_ok:
                        self._add_to_report(201)

        if not instrument_ok:
            line = self.extcsv.line_num('INSTRUMENT') + 2
            instrument_ok = self._add_to_report(87, line)

            location_ok = False
        else:
            location_ok = self.check_location()

        content_ok = self.check_content()
        data_generation_ok = self.check_data_generation()

        if not all([project_ok, dataset_ok, contributor_ok,
                    platform_ok, deployment_ok, instrument_ok,
                    location_ok, content_ok, data_generation_ok]):
            self._add_to_report(209)
            return None

        if metadata_only:
            msg = 'Lax mode detected. NOT validating dataset-specific tables'
            LOGGER.info(msg)
        else:
            dataset = self.extcsv.extcsv['CONTENT']['Category']
            dataset_validator = get_validator(dataset, self.reports)

            time_series_ok = self.check_time_series()
            dataset_validated = dataset_validator.check_all(self.extcsv)

            if not all([time_series_ok, dataset_validated]):
                self._add_to_report(209)
                return None

        LOGGER.info('Validating data record')
        data_record = DataRecord(self.extcsv)
        data_record_ok = self.check_data_record(data_record)

        if not data_record_ok:
            self._add_to_report(209)
            return None
        else:
            LOGGER.info('Data record is valid and verified')
            self._registry_updates.append(data_record)
            self._search_index_updates.append(data_record)

            self._add_to_report(200)
            return data_record

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

                    if not prev_version or now_version > prev_version:
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
            line = self.extcsv.line_num('CONTENT') + 2
            return self._add_to_report(51, line, value=project)

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
            line = self.extcsv.line_num('CONTENT') + 2
            return self._add_to_report(52, line, value=dataset)

    def check_contributor(self):
        """
        Validates the instance's Extended CSV source file's
        #DATA_GENERATION.Agency, and returns True if no errors are found.

        Adjusts the Extended CSV contents if necessary to form a match.

        Prerequisite: #CONTENT.Class is a trusted value.

        :returns: `bool` of whether the input file's contributor
                  validated successfully.
        """

        success = True

        agency = self.extcsv.extcsv['DATA_GENERATION']['Agency']
        project = self.extcsv.extcsv['CONTENT']['Class']

        if agency in ALIASES['Agency']:
            line = self.extcsv.line_num('DATA_GENERATION') + 2
            replacement = ALIASES['Agency'][agency]
            if not self._add_to_report(23, line, replacement):
                success = False

            agency = replacement
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
        else:
            line = self.extcsv.line_num('DATA_GENERATION') + 2
            if not self._add_to_report(67, line):
                success = False

        return success

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

        success = True

        identifier = str(self.extcsv.extcsv['PLATFORM']['ID'])
        pl_type = self.extcsv.extcsv['PLATFORM']['Type']
        name = self.extcsv.extcsv['PLATFORM']['Name']
        country = self.extcsv.extcsv['PLATFORM']['Country']
        # gaw_id = self.extcsv.extcsv['PLATFORM'].get('GAW_ID', None)

        # TODO: consider adding and checking #PLATFORM_Type
        LOGGER.debug('Validating station {}:{}'.format(identifier, name))
        valueline = self.extcsv.line_num('PLATFORM') + 2

        water_codes = ['*IW', 'IW', 'XZ']
        if pl_type == 'SHP' and any([not country, country in water_codes]):
            if not self._add_to_report(75, valueline):
                success = False

            self.extcsv.extcsv['PLATFORM']['Country'] = country = 'XY'

        if len(identifier) < 3:
            if not self._add_to_report(70, valueline):
                success = False

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
            self._add_to_report(71, valueline)
            return False

        LOGGER.debug('Validating station type...')
        platform_types = ['STN', 'SHP']
        type_ok = pl_type in platform_types

        if type_ok:
            LOGGER.debug('Validated station type {}'.format(type_ok))
        elif not self._add_to_report(72, valueline):
            success = False

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
        elif self.add_station_name(bypass=bypass):
            LOGGER.info('Added new station name {}'
                        .format(station['current_name']))
        elif not self._add_to_report(73, valueline, name=name):
            success = False

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
        elif not self._add_to_report(74, valueline):
            success = False

        return success

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

        success = True

        name = self.extcsv.extcsv['INSTRUMENT']['Name']
        model = self.extcsv.extcsv['INSTRUMENT']['Model']

        valueline = self.extcsv.line_num('INSTRUMENT') + 2

        if not name or name.lower() in ['na', 'n/a']:
            if not self._add_to_report(82, valueline):
                success = False
            self.extcsv.extcsv['INSTRUMENT']['Name'] = name = 'UNKNOWN'
        if not model or str(model).lower() in ['na', 'n/a']:
            if not self._add_to_report(83, valueline):
                success = False
            self.extcsv.extcsv['INSTRUMENT']['Model'] = model = 'UNKNOWN'

        if not success:
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
        elif not self._add_to_report(85, valueline, name=name):
            success = False

        # Check data registry for matching instrument model
        instrument = self.registry.query_by_field(Instrument, 'model', model,
                                                  case_insensitive=True)
        if instrument:
            model = instrument.model
            self.extcsv.extcsv['INSTRUMENT']['Model'] = instrument.model
        elif not self._add_to_report(86, valueline):
            success = False

        return success

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
            self.extcsv.extcsv['INSTRUMENT']['Number'] = serial = 'UNKNOWN'

        instrument = build_instrument(self.extcsv)
        fields = ['name', 'model', 'serial',
                  'station_id', 'dataset_id', 'deployment_id']
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

        success = True

        instrument_id = build_instrument(self.extcsv).instrument_id

        process_config = config.EXTRAS['processing']

        lat = self.extcsv.extcsv['LOCATION']['Latitude']
        lon = self.extcsv.extcsv['LOCATION']['Longitude']
        height = self.extcsv.extcsv['LOCATION'].get('Height', None)
        valueline = self.extcsv.line_num('LOCATION') + 2

        try:
            lat_numeric = float(lat)
            if -90 <= lat_numeric <= 90:
                LOGGER.debug('Validated instrument latitude')
            elif not self._add_to_report(78, valueline, field='Latitude',
                                         lower=-90, upper=90):
                success = False
        except ValueError:
            if not self._add_to_report(76, valueline, field='Longitude'):
                success = False

            self.extcsv.extcsv['LOCATION']['Latitude'] = lat = None
            lat_numeric = None

        try:
            lon_numeric = float(lon)
            if -180 <= lon_numeric <= 180:
                LOGGER.debug('Validated instrument longitude')
            elif not self._add_to_report(78, valueline, field='Longitude',
                                         lower=-180, upper=180):
                success = False
        except ValueError:
            if not self._add_to_report(76, valueline, field='Longitude'):
                success = False

            self.extcsv.extcsv['LOCATION']['Longitude'] = lon = None
            lon_numeric = None

        try:
            height_numeric = float(height) if height else None
            if not height or -50 <= height_numeric <= 5100:
                LOGGER.debug('Validated instrument height')
            elif not self._add_to_report(79, valueline, lower=-50, upper=5100):
                success = False
        except ValueError:
            if not self._add_to_report(77, valueline):
                success = False

            self.extcsv.extcsv['LOCATION']['Height'] = height = None
            height_numeric = None

        station_type = self.extcsv.extcsv['PLATFORM'].get('Type', 'STN')
        ignore_ships = not process_config['ships_ignore_location']

        if not success:
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
                if not self._add_to_report(80, valueline, field='Latitude'):
                    success = False
            if lon_numeric is not None and instrument.x is not None:
                if in_polar_region and ignore_polar_lon:
                    LOGGER.info('Skipping longitude check in polar region')
                elif abs(lon_numeric - instrument.x) >= lon_interval:
                    if not self._add_to_report(80, valueline,
                                               field='Longitude'):
                        success = False
            if height_numeric is not None and instrument.z is not None \
               and abs(height_numeric - instrument.z) >= height_interval:
                if not self._add_to_report(81, valueline):
                    success = False

        return success

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

        success = True

        dataset = self.extcsv.extcsv['CONTENT']['Category']
        level = self.extcsv.extcsv['CONTENT']['Level']
        form = self.extcsv.extcsv['CONTENT']['Form']

        valueline = self.extcsv.line_num('CONTENT') + 2

        if not level:
            if dataset == 'UmkehrN14' and 'C_PROFILE' in self.extcsv.extcsv:
                if not self._add_to_report(53, valueline, value=2.0):
                    success = False
                self.extcsv.extcsv['CONTENT']['Level'] = level = 2.0
            else:
                if not self._add_to_report(53, valueline, value=1.0):
                    success = False
                self.extcsv.extcsv['CONTENT']['Level'] = level = 1.0
        elif not isinstance(level, float):
            try:
                if not self._add_to_report(54, valueline, oldvalue=level,
                                           newvalue=float(level)):
                    success = False
                self.extcsv.extcsv['CONTENT']['Level'] = level = float(level)
            except ValueError:
                if not self._add_to_report(55, valueline):
                    success = False

        if str(level) not in DOMAINS['Datasets'][dataset]:
            if not self._add_to_report(56, valueline, dataset=dataset):
                success = False

        if not isinstance(form, int):
            try:
                if not self._add_to_report(57, valueline, oldvalue=form,
                                           newvalue=int(form)):
                    success = False
                self.extcsv.extcsv['CONTENT']['Form'] = form = int(form)
            except ValueError:
                if not self._add_to_report(58, valueline):
                    success = False

        return success

    def check_data_generation(self):
        """
        Validates the instance's Extended CSV source file's
        #DATA_GENERATION.Date and #DATA_GENERATION.Version by comparison
        with other tables. Returns True if no errors were encountered.

        Fill in the Extended CSV with missing values if possible.

        :returns: `bool` of whether the input file's #DATA_GENERATION table
                  collectively validated successfully.
        """

        success = True

        dg_date = self.extcsv.extcsv['DATA_GENERATION'].get('Date', None)
        version = self.extcsv.extcsv['DATA_GENERATION'].get('Version', None)

        valueline = self.extcsv.line_num('DATA_GENERATION')

        if not dg_date:
            if not self._add_to_report(62, valueline):
                success = False

            kwargs = {key: getattr(self.process_start, key)
                      for key in ['year', 'month', 'day']}
            today_date = datetime(**kwargs)

            self.extcsv.extcsv['DATA_GENERATION']['Date'] = today_date
            dg_date = today_date

        try:
            numeric_version = float(version)
        except TypeError:
            if not self._add_to_report(63, valueline, default=1.0):
                success = False

            self.extcsv.extcsv['DATA_GENERATION']['Version'] = version = '1.0'
            numeric_version = 1.0
        except ValueError:
            try:
                while version.count('.') > 1 and version.endswith('.0'):
                    version = version[:-2]
                numeric_version = float(version)
            except ValueError:
                if not self._add_to_report(66, valueline):
                    success = False

        if not success:
            return False

        if not 0 <= numeric_version <= 20:
            if not self._add_to_report(64, valueline, lower=0.0, upper=20.0):
                success = False
        if str(version) == str(int(numeric_version)):
            if not self._add_to_report(65, valueline):
                success = False

            self.extcsv.extcsv['DATA_GENERATION']['Version'] = \
                numeric_version

        return success

    def check_time_series(self):
        """
        Validate the input Extended CSV source file's dates across all tables
        to ensure that no date is more recent that #DATA_GENERATION.Date.

        :returns: `bool` of whether the input file's time fields collectively
                  validated successfully.
        """

        success = True

        dg_date = self.extcsv.extcsv['DATA_GENERATION']['Date']
        ts_time = self.extcsv.extcsv['TIMESTAMP'].get('Time', None)

        for table, body in self.extcsv.extcsv.items():
            if table == 'DATA_GENERATION':
                continue

            valueline = self.extcsv.line_num(table) + 2

            date_column = body.get('Date', [])
            if not isinstance(date_column, list):
                date_column = [date_column]

            for line, other_date in enumerate(date_column, valueline):
                if other_date > dg_date:
                    err_code = 91 if table.startswith('TIMESTAMP') else 92
                    if not self._add_to_report(err_code, line, table=table):
                        success = False

            time_column = body.get('Time', [])
            if not isinstance(time_column, list):
                time_column = [time_column]

            if ts_time:
                for line, other_time in enumerate(time_column, valueline):
                    if other_time and other_time < ts_time:
                        if not self._add_to_report(93, line):
                            success = False

        return success

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

        success = True

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

        if dg_date_before:
            if not self._add_to_report(95, dg_valueline):
                success = False
        elif dg_date_equal and version_equal:
            if not self._add_to_report(96, dg_valueline):
                success = False
        elif dg_date_equal:
            if not self._add_to_report(97, dg_valueline):
                success = False
        elif version_equal:
            if not self._add_to_report(98, dg_valueline):
                success = False

        instrument_name = self.extcsv.extcsv['INSTRUMENT']['Name']
        instrument_serial = self.extcsv.extcsv['INSTRUMENT']['Number']
        old_serial = response.instrument.serial

        if instrument_name == 'ECC' and instrument_serial != old_serial:
            instrument_valueline = self.extcsv.line_num('INSTRUMENT') + 2
            if not self._add_to_report(99, instrument_valueline):
                success = False

        return success


class ProcessingError(Exception):
    """custom exception handler"""
    pass
