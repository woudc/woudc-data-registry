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

from datetime import datetime
import logging
import os
import shutil

from woudc_data_registry import config, registry, search
from woudc_data_registry.models import (Contributor, DataRecord, Dataset,
                                        Instrument, Project, Station)
from woudc_data_registry.parser import (ExtendedCSV, MetadataValidationError,
                                        NonStandardDataError)
from woudc_data_registry.util import is_text_file, read_file

LOGGER = logging.getLogger(__name__)


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

    def __init__(self):
        """constructor"""

        self.status = None
        self.code = None
        self.message = None
        self.process_start = datetime.utcnow()
        self.process_end = None
        self.registry = registry.Registry()

    def process_data(self, infile, verify_only=False):
        """
        process incoming data record

        :param infile: incoming filepath
        :param verify_only: perform verification only (no ingest)

        :returns: `bool` of processing result
        """

        # detect incoming data file
        data = None
        self.data_record = None
        self.search_engine = search.SearchIndex()

        LOGGER.info('Processing file {}'.format(infile))
        LOGGER.info('Detecting file')
        if not is_text_file(infile):
            self.status = 'failed'
            self.code = 'NonStandardDataError'
            self.message = 'binary file detected'
            LOGGER.error('Unknown file: {}'.format(self.message))
            return False

        try:
            data = read_file(infile)
        except UnicodeDecodeError as err:
            self.status = 'failed'
            self.code = 'NonStandardDataError'
            self.message = err
            LOGGER.error('Unknown file: {}'.format(err))
            return False

        LOGGER.info('Parsing data record')
        ecsv = ExtendedCSV(data)

        try:
            LOGGER.info('Validating Extended CSV')
            ecsv.validate_metadata()
            LOGGER.info('Valid Extended CSV')
        except NonStandardDataError as err:
            self.status = 'failed'
            self.code = 'NonStandardDataError'
            self.message = err
            LOGGER.error('Invalid Extended CSV: {}'.format(err))
            return False
        except MetadataValidationError as err:
            self.status = 'failed'
            self.code = 'MetadataValidationError'
            self.message = err
            LOGGER.error('Invalid Extended CSV: {}'.format(err.errors))
            return False

        LOGGER.info('Data is valid Extended CSV')

        self.data_record = DataRecord(ecsv)
        self.data_record.ingest_filepath = infile
        self.data_record.filename = os.path.basename(infile)
        self.data_record.url = self.data_record.get_waf_path(
            config.WDR_WAF_BASEURL)
        self.process_end = datetime.utcnow()

        LOGGER.debug('Verifying if URN already exists')
        results = self.registry.query_by_field(
            DataRecord, self.data_record, 'identifier')

        if results:
            msg = 'Data exists'
            self.status = 'failed'
            self.code = 'ProcessingError'
            self.message = msg
            LOGGER.error(msg)
            return False

#        domains_to_check = [
#            'content_category',
#            'data_generation_agency',
#            'platform_type',
#            'platform_id',
#            'platform_name',
#            'platform_country',
#            'instrument_name',
#            'instrument_model'
#        ]

#        for domain_to_check in domains_to_check:
#            value = getattr(self.data_record, domain_to_check)
#            domain = getattr(DataRecord, domain_to_check)
#
#            if value not in self.registry.query_distinct(domain):
#                msg = 'value {} not in domain {}'.format(value,
#                                                         domain_to_check)
#                LOGGER.error(msg)
#                # raise ProcessingError(msg)

        LOGGER.info('Verifying data record against core metadata fields')

        LOGGER.debug('Validating project')
        self.projects = self.registry.query_distinct(Project.identifier)
        if self.data_record.content_class not in self.projects:
            msg = 'Project {} not found in registry'.format(
                self.data_record.content_class)
            LOGGER.error(msg)
            raise ProcessingError(msg)
        else:
            LOGGER.debug('Matched with project: {}'.format(
                self.data_record.content_class))

        LOGGER.debug('Validating dataset')
        self.datasets = self.registry.query_distinct(Dataset.identifier)
        if self.data_record.content_category not in self.datasets:
            msg = 'Dataset {} not found in registry'.format(
                self.data_record.content_category)
            LOGGER.error(msg)
            raise ProcessingError(msg)
        else:
            LOGGER.debug('Matched with dataset: {}'.format(
                self.data_record.content_category))

        LOGGER.debug('Validating contributor')
        self.contributors = self.registry.query_distinct(
            Contributor.identifier)
        file_contributor = '{}:{}'.format(
            self.data_record.data_generation_agency,
            self.data_record.content_class)
        if file_contributor not in self.contributors:
            msg = 'Contributor {} not found in registry'.format(
                file_contributor)
            LOGGER.error(msg)
            raise ProcessingError(msg)
        else:
            LOGGER.debug('Matched with contributor: {}'.format(
                file_contributor))

        # TODO: consider adding and checking #PLATFORM_Type
        LOGGER.debug('Validating station data')
        station = {
            'identifier': self.data_record.platform_id,
            'name': self.data_record.platform_name,
            'country_id': self.data_record.platform_country
        }

        LOGGER.debug('Validating station id...')
        results = self.registry.query_multiple_fields(
            Station, station, ['identifier'])
        if results:
            LOGGER.debug('Validated with id: {}'.format(
                self.data_record.platform_id))
        else:
            msg = 'Station {} not found in registry'.format(
                self.data_record.platform_id)
            LOGGER.error(msg)
            raise ProcessingError(msg)

        LOGGER.debug('Validating station name...')
        fields = ['identifier', 'name']
        results = self.registry.query_multiple_fields(Station, station, fields)
        if results:
            LOGGER.debug('Validated with name: {} for id: {}'.format(
                self.data_record.platform_name, self.data_record.platform_id))
        else:
            msg = 'Station name: {} did not match data for id: {}'.format(
                self.data_record.platform_id, self.data_record.platform_id)
            LOGGER.error(msg)
            raise ProcessingError(msg)

        LOGGER.debug('Validating station country...')
        fields = ['identifier', 'country_id']
        results = self.registry.query_multiple_fields(Station, station, fields)
        if results:
            LOGGER.debug('Validated with country: {} for id: {}'.format(
                self.data_record.platform_country,
                self.data_record.platform_id))
        else:
            msg = 'Station country: {} did not match data for id: {}'.format(
                self.data_record.platform_country,
                self.data_record.platform_id)
            LOGGER.error(msg)
            raise ProcessingError(msg)

        LOGGER.debug('Validating instrument')
        self.instruments = self.registry.query_distinct(Instrument.identifier)
        instrument_added = False
        instrument = [self.data_record.instrument_name,
                      self.data_record.instrument_model,
                      self.data_record.instrument_number,
                      self.data_record.platform_id,
                      self.data_record.content_category]
        instrument_id = ':'.join(instrument)
        if instrument_id not in self.instruments:
            instrument[2] = str(int(instrument[2]))
            old_instrument_id = ':'.join(instrument)
            if old_instrument_id not in self.instruments:
                msg = 'Instrument {} not found in registry'.format(
                    instrument_id)
                LOGGER.warning(msg)
                LOGGER.debug('Checking for new serial number...')
                instrument_added = self.new_serial(instrument_id, verify_only)
                if not instrument_added:
                    msg = 'Other instrument data for id:{} does not match '\
                          'existing records.'.format(instrument_id)
                    LOGGER.error(msg)
                    raise ProcessingError(msg)
                LOGGER.debug('Updating instruments list.')
                self.instruments = self.registry.\
                    query_distinct(Instrument.identifier)
            else:
                instrument_id = old_instrument_id

        if instrument_added and verify_only:
            LOGGER.debug('Skipping location check due to instrument '
                         'not being added in verification mode.')
        else:
            LOGGER.debug('Matched with instrument: {}'.format(instrument_id))
            LOGGER.debug('Checking instrument location...')
            location = {
                'identifier': instrument_id,
                'x': self.data_record.x,
                'y': self.data_record.y,
                'z': self.data_record.z
            }
            results = self.registry.query_multiple_fields(Instrument, location)
            if results:
                LOGGER.debug('Instrument location validated.')
            else:
                msg = 'Instrument location does not match database records.'
                LOGGER.error(msg)
                raise ProcessingError(msg)

        # TODO: duplicate data submitted
        # TODO: check new version of file

        LOGGER.info('Data record is valid and verified')

        if verify_only:  # do not save or index
            LOGGER.info('Verification mode detected. NOT saving to registry')
            return True

        LOGGER.info('Saving data record CSV to registry')
        self.registry.save(self.data_record)

        LOGGER.info('Saving data record CSV to WAF')
        waf_filepath = self.data_record.get_waf_path(config.WDR_WAF_BASEDIR)
        os.makedirs(os.path.dirname(waf_filepath), exist_ok=True)
        shutil.copy2(self.data_record.ingest_filepath, waf_filepath)

        LOGGER.info('Indexing data record search engine')
        self.search_engine.index_data_record(
            self.data_record.__geo_interface__)

        return True

    def new_serial(self, instrument_id, verify_only):
        data = {
            'identifier': instrument_id,
            'station_id': self.data_record.platform_id,
            'dataset_id': self.data_record.content_category,
            'name': self.data_record.instrument_name,
            'model': self.data_record.instrument_model,
            'serial': self.data_record.instrument_number,
            'x': self.data_record.x,
            'y': self.data_record.y,
            'z': self.data_record.z
        }

        fields = ['name',
                  'model',
                  'station_id',
                  'dataset_id']
        results = self.registry.query_multiple_fields(Instrument, data,
                                                      fields)
        if results:
            LOGGER.debug('All other instrument data matches.')
            LOGGER.info('Adding instrument with new serial number...')
            if verify_only:
                LOGGER.info('Verification mode detected. '
                            'Instrument not added.')
            else:
                instrument = Instrument(data)
                self.registry.save(instrument)
                LOGGER.info('Instrument successfully added.')
            return True
        else:
            return False


class ProcessingError(Exception):
    """custom exception handler"""
    pass
