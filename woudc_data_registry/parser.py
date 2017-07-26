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

import csv
from datetime import datetime
import io
import logging
import sys

import requests
from six import StringIO as StringIO

LOGGER = logging.getLogger(__name__)

ERROR_CODES = {
    'missing_table': 10000,
    'missing_data': 11000,
    'invalid_data': 12000,
}


def _read(filename, encoding='utf-8'):
    """read file contents"""

    LOGGER.debug('Reading file %s (encoding %s)', filename, encoding)
    with io.open(filename, encoding=encoding) as fh:
        return fh.read().strip()


def _get_value_type(field, value):
    """derive true type from data value"""

    field2 = field.lower()
    value2 = None

    if value == '':  # empty
        return None

    if field2 == 'date':
        value2 = datetime.strptime(value, '%Y-%m-%d')
    elif field2 == 'time':
        value2 = datetime.strptime(value, '%H:%M:%S')
    else:
        try:
            if '.' in value:  # float?
                value2 = float(value)
            elif value.startswith('0'):
                value2 = value
            else:  # int?
                value2 = int(value)
        except ValueError:  # string (default)?
                value2 = value

    return value2


class ExtendedCSV(object):
    """

    minimal WOUDC Extended CSV parser

    http://guide.woudc.org/en/#chapter-3-standard-data-format

    """

    def __init__(self, content):
        """read WOUDC Extended CSV file"""

        self.extcsv = {}
        self._raw = None

        self.metadata_tables = {
            'CONTENT': ['Class', 'Category', 'Level', 'Form'],
            'DATA_GENERATION': ['Date', 'Agency', 'Version'],
            'PLATFORM': ['Type', 'ID', 'Name', 'Country'],
            'INSTRUMENT': ['Name', 'Model', 'Number'],
            'LOCATION': ['Latitude', 'Longitude', 'Height'],
            'TIMESTAMP': ['UTCOffset', 'Date']
        }

        LOGGER.info('Reading into csv')
        self._raw = _read(content)
        reader = csv.reader(StringIO(self._raw))

        found_table = False
        table_name = None

        LOGGER.debug('Parsing object model')
        for row in reader:
            if len(row) == 1 and row[0].startswith('#'):  # table name
                    table_name = row[0].replace('#', '')
                    if table_name in self.metadata_tables.keys():
                        found_table = True
                        LOGGER.debug('Found new table %s', table_name)
                        self.extcsv[table_name] = {}
            elif found_table:  # fetch header line
                LOGGER.debug('Found new table header %s', table_name)
                self.extcsv[table_name]['_fields'] = row
                found_table = False
            elif len(row) > 0 and row[0].startswith('*'):  # comment
                LOGGER.debug('Found comment')
                continue
            elif len(row) == 0:  # blank line
                LOGGER.debug('Found blank line')
                continue
            else:  # process row data
                if table_name in self.metadata_tables.keys():
                    self.extcsv[table_name]['_line_num'] = int(reader.line_num + 1)
                    for idx, val in enumerate(row):
                        field = self.extcsv[table_name]['_fields'][idx]
                        self.extcsv[table_name][field] = _get_value_type(field,
                                                                         val)

        # delete transient fieldlist
        for key, value in self.extcsv.items():
            value.pop('_fields')

    def validate_metadata(self):
        """validate_metadata core metadata fields"""

        errors = []

        missing_tables = list(set(self.metadata_tables) -
                              set(self.extcsv.keys()))

        if missing_tables:
            for missing_table in missing_tables:
                errors.append({
                    'code': 'missing_table',
                    'locator': missing_table,
                    'text': 'ERROR: {}: missing table {}'.format(ERROR_CODES['missing_table'], missing_table)
                })

        for key, value in self.extcsv.items():
            missing_datas = list(set(self.metadata_tables[key]) -
                                 set(value.keys()))

            if missing_datas:
                for missing_data in missing_datas:
                    errors.append({
                        'code': 'missing_data',
                        'locator': missing_data,
                        'text': 'ERROR: {}: missing data {} (line number: {}'.format(ERROR_CODES['missing_data'], missing_data, value['_line_num'])
                    })

        if self.extcsv['LOCATION']['Latitude'] > 90 or self.extcsv['LOCATION']['Latitude'] < -90:
            errors.append({
                'code': 'invalid_data',
                'locator': 'LOCATION.Latitude',
                'text': 'ERROR: {}: Invalid Latitude: {} (line number: {}'.format(ERROR_CODES['invalid_data'], self.extcsv['LOCATION']['Latitude'], self.extcsv['LOCATION']['_line_num'])
            })
        if self.extcsv['LOCATION']['Longitude'] > 180 or self.extcsv['LOCATION']['Longitude'] < -180:
            errors.append({
                'code': 'invalid_data',
                'locator': 'LOCATION.Longitude',
                'text': 'ERROR: {}: Invalid Longitude: {} (line number: {}'.format(ERROR_CODES['invalid_data'], self.extcsv['LOCATION']['Longitude'], self.extcsv['LOCATION']['_line_num'])
            })

        return errors


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: {} <file>'.format(sys.argv[0]))
        sys.exit(1)

    ecsv = ExtendedCSV(sys.argv[1])
    print(ecsv.extcsv['LOCATION'])
    print(ecsv.validate_metadata())
