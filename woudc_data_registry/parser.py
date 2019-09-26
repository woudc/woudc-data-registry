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
import sys

import csv
import yaml

import logging

from io import StringIO
from datetime import datetime, time
from collections import OrderedDict


LOGGER = logging.getLogger(__name__)

PROJECT_ROOT = '/home/hurkaa/conda-woudc-data-registry'
table_definition_file = os.path.join(PROJECT_ROOT, 'data', 'tables.yaml')
with open(table_definition_file) as table_definitions:
    DOMAINS = yaml.safe_load(table_definitions)

ERROR_CODES = {
    'missing_table': 10000,
    'missing_data': 11000,
    'invalid_data': 12000,
}


def _typecast_value(field, value):
    """
    Returns a copy of the string <value> converted to the expected type
    for a column named <field>, if possible, or returns the original string
    otherwise.

    :param field: Name of column
    :param value: String containing a value
    :returns: Value cast to the appropriate type for its column
    """

    if value == '':  # empty
        return None

    lowered_field = field.lower()
    if lowered_field == 'date':
        return datetime.strptime(value, '%Y-%m-%d').date()
    elif lowered_field == 'time':
        hour, minute, second = [int(v) for v in value.split(':')]
        return time(hour, minute, second)

    try:
        if '.' in value:  # float?
            return float(value)
        elif len(value) > 1 and value.startswith('0'):
            return value
        else:  # int?
            return int(value)
    except ValueError:  # string (default)?
        return value


def is_empty_line(line):
    """
    Returns True iff <line> represents a non-content line of an Extended CSV
    file, i.e. a blank line or a comment.
    """

    if len(line) == 0:
        return True
    else:
        first = line[0].strip()
        return len(first) == 0 or first.startswith('*')


class ExtendedCSV(object):
    """

    minimal WOUDC Extended CSV parser

    https://guide.woudc.org/en/#chapter-3-standard-data-format

    """

    def __init__(self, content):
        """
        read WOUDC Extended CSV file

        :param content: buffer of Extended CSV data

        :returns: `woudc_data_registry.parser.ExtendedCSV`
        """

        self.extcsv = {}
        self.number_of_observations = 0
        self._raw = None

        self._table_count = {}
        self._line_num = {}
        self.errors = []

        LOGGER.debug('Reading into csv')
        self._raw = content
        reader = csv.reader(StringIO(self._raw))

        LOGGER.debug('Parsing object model')
        parent_table = None
        lines = enumerate(reader)

        for line_num, row in lines:
            if len(row) > 0 and row[0].startswith('#'):  # table name
                parent_table = ''.join(row).lstrip('#').rstrip()

                if parent_table not in self._table_count:
                    self._table_count[parent_table] = 1
                else:
                    updated_count = self._table_count[parent_table] + 1
                    self._table_count[parent_table] = updated_count
                    parent_table += '_' + str(updated_count)

                LOGGER.debug('Found new table {}'.format(parent_table))
                ln, fields = next(lines)
                while is_empty_line(fields):
                    msg = 'Unexpected empty line at line {}'.format(ln)
                    self.errors.append((8, msg))
                    ln, fields = next(lines)

                errors = self.init_table(parent_table, fields, line_num)
                self.errors.extend(errors)
            elif len(row) > 0 and row[0].startswith('*'):  # comment
                LOGGER.debug('Found comment')
                parent_table = None
                continue
            elif len(row) == 0:  # blank line
                LOGGER.debug('Found blank line')
                parent_table = None
                continue
            elif parent_table is not None:
                table_values = row
                errors = self.add_values_to_table(parent_table, table_values,
                                                  line_num)
                self.errors.extend(errors)
            else:
                msg = 'Unrecognized data {}'.format(row)
                self.errors.append((9, msg))

        for table, body in self.extcsv.items():
            arbitrary_column, values = next(iter(body.items()))

            if len(values) == 0:
                msg = 'Empty table {}'.format(table)
                self.errors.append((140, msg))
            elif len(values) == 1:
                for field in body.keys():
                    body[field] = _typecast_value(field, body[field][0])
            else:
                for field in body.keys():
                    body[field] = list(map(
                        lambda val: _typecast_value(field, val), body[field]))

        if len(errors) > 0:
            raise NonStandardDataError('Failed to validate Extended CSV file')

    def init_table(self, table_name, fields, line_num):
        """
        Record an empty Extended CSV table named <table_name> with
        fields given in the list <fields>, which starts at line <line_num>.

        Returns a list of errors encountered while recording the new table.

        :param table_name: Name of the new table
        :param fields: List of column names in the new table
        :param line_num: Line number of the table's header (its name)
        :returns: List of errors
        """

        self.extcsv[table_name] = OrderedDict()
        self._line_num[table_name] = line_num

        for field in fields:
            self.extcsv[table_name][field.strip()] = []

        return []

    def add_values_to_table(self, table_name, values, line_num):
        """
        Add the raw strings in <values> to the bottom of the columns
        in the tabled named <table_name>.

        Returns a list of errors encountered while adding the new values.

        :param table_name: Name of the table the values fall under
        :param values: A list of values from one row in the table
        :param line_num: Line number the row occurs at
        :returns: List of errors
        """

        fields = self.extcsv[table_name].keys()
        fillins = len(fields) - len(values)
        errors = []

        if fillins < 0:
            msg = 'Data row at line {} has more values than table columns' \
                      .format(line_num)
            errors.append((7, msg))

        values.extend([''] * fillins)
        values = values[:len(fields)]

        for field, value in zip(fields, values):
            self.extcsv[table_name][field].append(value)

        return errors

    def gen_woudc_filename(self):
        """generate WOUDC filename convention"""

        timestamp = self.extcsv['TIMESTAMP']['Date'].strftime('%Y%m%d')
        instrument_name = self.extcsv['INSTRUMENT']['Name']
        instrument_model = self.extcsv['INSTRUMENT']['Model']

        extcsv_serial = self.extcsv['INSTRUMENT'].get('Number', None)
        instrument_number = extcsv_serial or 'na'

        agency = self.extcsv['DATA_GENERATION']['Agency']

        filename = '{}.{}.{}.{}.{}.csv'.format(timestamp, instrument_name,
                                               instrument_model,
                                               instrument_number, agency)
        if ' ' in filename:
            LOGGER.warning('filename contains spaces: {}'.format(filename))
            file_slug = filename.replace(' ', '-')
            LOGGER.info('filename {} renamed to {}'
                        .format(filename, file_slug))
            filename = file_slug

        return filename

    def validate_metadata(self):
        """validate core metadata tables and fields"""

        errors = []
        missing_tables = [table for table in DOMAINS['Common']
                          if table not in self.extcsv.keys()]
        present_tables = [table for table in self.extcsv.keys()
                          if table in DOMAINS['Common']]

        if len(present_tables) == 0:
            msg = 'No core metadata tables found. Not an Extended CSV file'
            LOGGER.error(msg)
            raise NonStandardDataError(msg)

        for missing in missing_tables:
            errors.append({
                'code': 'missing_table',
                'locator': missing,
                'text': 'ERROR {}: {}'.format(ERROR_CODES['missing_table'],
                                              missing_table)
            })

        if errors:
            msg = 'Not an Extended CSV file'
            LOGGER.error(msg)
            raise MetadataValidationError(msg, errors)
        else:
            LOGGER.debug('No missing metadata tables.')

        for table, fields_definition in DOMAINS['Common'].items():
            missing_fields = [field for field in fields_definition['required']
                              if field not in self.extcsv[table].keys()]
            excess_fields = [field for field in self.extcsv[table].keys()
                             if field not in fields_definition['required']]

            if len(missing_fields) > 0:
                for missing in missing_fields:
                    errors.append({
                        'code': 'missing_data',
                        'locator': missing,
                        'text': 'ERROR: {}: (line number: {})'.format(
                            ERROR_CODES['missing_data'],
                            self._line_num[table])
                    })
            else:
                LOGGER.debug('No missing fields in table {}'.format(table))

            for field in excess_fields:
                if field in fields_definition.get('optional', ()):
                    LOGGER.debug('Found optional {} field {}'
                                 .format(table, field))
                else:
                    del self.extcsv[table][field]
                    errors.append({
                        'code': 'excess_data',
                        'locator': field,
                        'text': 'TODO'  # TODO
                    })

        if self.check_dataset():
            LOGGER.debug('All tables in file validated.')
        else:
            errors.append('Invalid table data. See logs for details.')

        if errors:
            LOGGER.error(errors)
            raise MetadataValidationError('Invalid metadata', errors)

    def check_dataset(self):
        tables = DOMAINS['Datasets']
        curr_dict = tables

        for field in ['Category', 'Level', 'Form']:
            key = self.extcsv['CONTENT'][field]

            if key in curr_dict:
                curr_dict = curr_dict[key]
            else:
                field_name = '#CONTENT.{}'.format(field.capitalize())
                LOGGER.error('Invalid value for {}: {}'
                             .format(field_name, key))
                return False

        if 1 in curr_dict.keys():
            passing_versions = map(self.check_tables, curr_dict.values())
            return any(passing_versions)
        else:
            return self.check_tables(curr_dict)

    def check_tables(self, schema):
        for table in schema['required'].keys():
            if table in self.extcsv:
                LOGGER.debug('{} table validated.'.format(table))
                # Consider adding order checking with orderdicts
                missing = set(schema['required'][table])\
                    - set(self.extcsv[table].keys())
                extra = set(self.extcsv[table].keys())\
                    - set(schema['required'][table])
                if missing:
                    msg = 'The following fields were missing from table {}'\
                          ': {}'.format(table, missing)
                    LOGGER.error(msg)
                elif extra:
                    msg = 'The following fields should not be in table {}'\
                          ': {}'.format(table, extra)
                    LOGGER.error(msg)
                else:
                    LOGGER.debug('All fields in table {} '
                                 'validated'.format(table))
            else:
                msg = 'Could not validate ({} requires {})'.format(
                    table, self.extcsv['CONTENT']['Category'])
                LOGGER.error(msg)

        if 'optional' in schema.keys():
            for table in schema['optional'].keys():
                if table not in self.extcsv:
                    LOGGER.warning('Optional table {} is not in file.'.format(
                                   table))

        return True


class NonStandardDataError(Exception):
    """custom exception handler"""
    pass


class MetadataValidationError(Exception):
    """custom exception handler"""

    def __init__(self, message, errors):
        """set error list/stack"""
        super(MetadataValidationError, self).__init__(message)
        self.errors = errors


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: {} <file>'.format(sys.argv[0]))
        sys.exit(1)

    with open(sys.argv[1]) as fh:
        ecsv = ExtendedCSV(fh.read())
    try:
        ecsv.validate_metadata()
    except MetadataValidationError as err:
        print(err.errors)
