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
        self.warnings = []

        LOGGER.debug('Reading into csv')
        self._raw = content
        reader = csv.reader(StringIO(self._raw))

        LOGGER.debug('Parsing object model')
        parent_table = None
        lines = enumerate(reader, 1)

        for line_num, row in lines:
            if len(row) == 1 and row[0].startswith('#'):  # table name
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
                    msg = 'Unexpected empty line'
                    self.warnings.append((8, msg, ln))
                    ln, fields = next(lines)

                self.init_table(parent_table, fields, line_num)
            elif len(row) > 0 and row[0].startswith('*'):  # comment
                LOGGER.debug('Found comment')
                parent_table = None
                continue
            elif len(row) == 0:  # blank line
                LOGGER.debug('Found blank line')
                parent_table = None
                continue
            elif parent_table is not None \
                 and any(map(lambda col: len(col) > 0, row)):
                table_values = row
                self.add_values_to_table(parent_table, table_values, line_num)
            else:
                msg = 'Unrecognized data {}'.format(','.join(row))
                self.errors.append((9, msg, line_num))

        for table, body in self.extcsv.items():
            arbitrary_column, values = next(iter(body.items()))

            if len(values) == 0:
                msg = 'Empty table {}'.format(table)
                line = self._line_num[table]
                self.warnings.append((140, msg, line))
            elif len(values) == 1:
                for field in body.keys():
                    body[field] = _typecast_value(field, body[field][0])
            else:
                for field in body.keys():
                    body[field] = list(map(
                        lambda val: _typecast_value(field, val), body[field]))

        if len(self.errors) > 0:
            raise NonStandardDataError(self.errors)

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

        if fillins < 0:
            msg = 'Data row has more values than table columns'
            self.warnings.append((7, msg, line_num))

        values.extend([''] * fillins)
        values = values[:len(fields)]

        for field, value in zip(fields, values):
            self.extcsv[table_name][field].append(value)

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

        missing_tables = [table for table in DOMAINS['Common']
                          if table not in self.extcsv.keys()]
        present_tables = [table for table in self.extcsv.keys()
                          if table in DOMAINS['Common']]
        errors = []

        if len(present_tables) == 0:
            msg = 'No core metadata tables found. Not an Extended CSV file'
            self.errors.append((161, msg, None))
        else:
            for missing in missing_tables:
                msg = 'Missing required table: {}'.format(missing)
                self.errors.append((1, msg, None))

        if self.errors:
            msg = 'Not an Extended CSV file'
            raise MetadataValidationError(msg, self.errors)
        else:
            LOGGER.debug('No missing metadata tables.')

        for table, fields_definition in DOMAINS['Common'].items():
            missing_fields = [field for field in fields_definition['required']
                              if field not in self.extcsv[table].keys()]
            excess_fields = [field for field in self.extcsv[table].keys()
                             if field not in fields_definition['required']]

            if len(missing_fields) > 0:
                for missing in missing_fields:
                    msg = 'Missing required {} field {}'.format(table, missing)
                    line = self._line_num[table] + 1
                    self.errors.append((3, msg, line))
            else:
                LOGGER.debug('No missing fields in table {}'.format(table))

            for field in excess_fields:
                if field in fields_definition.get('optional', ()):
                    LOGGER.debug('Found optional {} field {}'
                                 .format(table, field))
                else:
                    del self.extcsv[table][field]

                    msg = 'Field name {}.{} is not from approved list' \
                          .format(table, field)
                    line = self._line_num[table] + 1
                    self.warnings.append((4, msg, line))

        self.check_dataset()
        if len(self.errors) == 0:
            LOGGER.debug('All tables in file validated.')
        else:
            raise MetadataValidationError('Invalid metadata', self.errors)

    def check_dataset(self):
        tables = DOMAINS['Datasets']
        curr_dict = tables
        fields_line = self._line_num['CONTENT'] + 1

        for field in ['Category', 'Level', 'Form']:
            key = self.extcsv['CONTENT'][field]

            if key in curr_dict:
                curr_dict = curr_dict[key]
            else:
                field_name = '#CONTENT.{}'.format(field.capitalize())
                msg = 'Unknown {} value {}'.format(field_name, key)

                self.errors.append((56, msg, fields_line))
                return

        if 1 in curr_dict.keys():
            version = self.determine_version(curr_dict)
            LOGGER.info('Identified version as {}'.format(version))
            self.check_tables(curr_dict[version])
        else:
            self.check_tables(curr_dict)

    def determine_version(self, schema):
        versions = set(schema.keys())
        tables = {version: schema[version].keys() for version in versions}
        uniques = {}

        for version in versions:
            u = set(tables[version])
            others = versions - {version}

            for other_version in others:
                u -= set(tables[other_version])

            uniques[version] = u

        candidates = {version: [] for version in versions}
        for table in self.extcsv:
            for version in versions:
                if table in uniques[version]:
                    candidates[version].append(table)

        def rating(version):
            return len(candidates[version]) / len(uniques[version])

        candidate_scores = list(map(rating, versions))
        best_match = max(candidate_scores)
        if best_match == 0:
            msg = 'No version-unique tables found'
            self.errors.append((108, msg, None))
            raise NonStandardDataError(self.errors)
        else:
            for version in versions:
                if rating(version) == best_match:
                    return version

    def check_tables(self, schema):
        required_tables = [name for name, body in schema.items()
                           if 'required' in body]
        optional_tables = [name for name, body in schema.items()
                           if 'required' not in body]

        missing_tables = [table for table in required_tables
                          if table not in self.extcsv.keys()]
        present_tables = [table for table in self.extcsv.keys()
                          if table.rstrip('0123456789_') in schema]

        required_tables.extend(DOMAINS['Common'].keys())
        extra_tables = [table for table in self.extcsv.keys()
                        if table.rstrip('0123456789_') not in required_tables]

        dataset = self.extcsv['CONTENT']['Category']
        for missing in missing_tables:
            msg = 'Missing required table(s) {} for {}' \
                  .format(dataset, ', '.join(missing_tables))
            self.errors.append((1, msg, None))

        for table in present_tables:
            table_type = table.rstrip('0123456789_')
            fields_line = self._line_num[table]

            required = schema[table_type].get('required', ())
            lowered_provided = list(map(str.lower, self.extcsv[table].keys()))
            lowered_required = list(map(str.lower, required))

            missing_fields = [field for field in required
                              if field.lower() not in lowered_provided]
            extra_fields = [field for field in self.extcsv[table]
                            if field.lower() not in lowered_required]

            arbitrary_column = next(iter(self.extcsv[table].values()))
            null_value = '' if not isinstance(arbitrary_column, list) \
                else [''] * len(arbitrary_column)

            if len(missing_fields) > 0:
                for field in missing_fields:
                    msg = 'Missing field {}.{}'.format(table, field)
                    LOGGER.error(msg)
                    self.errors.append((3, msg, fields_line))
                    self.extcsv[table][field] = null_value
                LOGGER.info('Filled missing fields with null string values')

            if len(extra_fields) > 0:
                for field in extra_fields:
                    msg = 'Excess field {} should not be in {}' \
                          .format(field, table)
                    LOGGER.error(msg)
                    self.warnings.append((4, msg, fields_line))
                    del self.extcsv[table][field]
                LOGGER.debug('Removing excess columns from {}'.format(table))

            LOGGER.debug('Successfully validated table {}'.format(table))

        for table in extra_tables:
            table_type = table.rstrip('0123456789_')
            if table_type not in optional_tables:
                msg = 'Excess table {} does not belong in {} file: removing' \
                      .format(table, dataset)
                self.warnings.append((2, msg, None))
                del self.extcsv[table]

        for table in optional_tables:
            if table not in self.extcsv:
                LOGGER.warning('Optional table {} is not in file.'.format(
                               table))


class NonStandardDataError(Exception):
    """custom exception handler"""

    def __init__(self, errors):
        error_string = '\n' + '\n'.join(map(str, errors))
        super(NonStandardDataError, self).__init__(error_string)

        self.errors = errors


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
