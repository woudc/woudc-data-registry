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
import re
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


def non_content_line(line):
    """
    Returns True iff <line> represents a non-content line of an Extended CSV
    file, i.e. a blank line or a comment.
    """

    if len(line) == 0:
        return True
    elif len(line) == 1:
        first = line[0].strip()
        return len(first) == 0 or first.startswith('*')
    else:
        return line[0].strip().startswith('*')


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
            separators = []
            for bad_sep in ['::', ';', '$', '%', '|', '/', '\\']:
                if len(row) > 0 and bad_sep in row[0]:
                    separators.append(bad_sep)

            for separator in separators:
                comma_separated = row[0].replace(separator, ',')
                row = next(csv.reader(StringIO(comma_separated)))

                msg = 'Improper delimiter used \'{}\', corrected to \',\'' \
                      ' (comma)'.format(separator)
                self.warnings.append((7, msg, line_num))

            if len(row) == 1 and row[0].startswith('#'):  # table name
                parent_table = ''.join(row).lstrip('#').rstrip()

                if parent_table not in self._table_count:
                    self._table_count[parent_table] = 1
                else:
                    updated_count = self._table_count[parent_table] + 1
                    self._table_count[parent_table] = updated_count
                    parent_table += '_' + str(updated_count)

                try:
                    LOGGER.debug('Found new table {}'.format(parent_table))
                    ln, fields = next(lines)

                    while non_content_line(fields):
                        msg = 'Unexpected empty line between table name' \
                              ' and fields'
                        self.warnings.append((8, msg, ln))

                        ln, fields = next(lines)

                    self.init_table(parent_table, fields, line_num)
                except StopIteration:
                    msg = 'Table {} has no fields'.format(parent_table)
                    self.errors.append((6, msg, line_num))
            elif len(row) > 0 and row[0].startswith('*'):  # comment
                LOGGER.debug('Found comment')
                continue
            elif not non_content_line(row):  # blank line
                LOGGER.debug('Found blank line')
                continue
            elif parent_table is not None and not non_content_line(row):
                table_values = row
                self.add_values_to_table(parent_table, table_values, line_num)
            else:
                msg = 'Unrecognized data {}'.format(','.join(row))
                self.errors.append((9, msg, line_num))

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

    def typecast_value(self, table, field, value, line_num):
        """
        Returns a copy of the string <value> converted to the expected type
        for a column named <field> in table <table>, if possible, or returns
        the original string otherwise.

        :param table: Name of the table where the value was found
        :param field: Name of the column
        :param value: String containing a value
        :param line_num: Line number where the value was found
        :returns: Value cast to the appropriate type for its column
        """

        if value == '':  # Empty CSV cell
            return None

        lowered_field = field.lower()

        try:
            if lowered_field == 'time':
                return self.parse_timestamp(table, value, line_num)
            elif lowered_field == 'date':
                return self.parse_datestamp(table, value, line_num)
            elif lowered_field == 'utcoffset':
                return self.parse_utcoffset(table, value, line_num)
        except Exception as err:
            msg = 'Failed to parse #{}.{} value {} due to: {}' \
                  .format(table, field, value, str(err))
            self.errors.append((1000, msg, line_num))
            return value

        try:
            if '.' in value:  # Check float conversion
                return float(value)
            elif len(value) > 1 and value.startswith('0'):
                return value
            else:  # Check integer conversion
                return int(value)
        except Exception:  # Default type to string
            return value

    def parse_timestamp(self, table, timestamp, line_num):
        """
        Return a time object representing the time contained in string
        <timestamp> according to the expected HH:mm:SS format with optional
        'am' or 'pm' designation.

        Corrects common formatting errors and performs very simple validation
        checks. Raises ValueError if the string cannot be parsed.

        The other parameters are used for error reporting.

        :param table: Name of table the value was found under.
        :param timestamp: String value taken from a Time column.
        :param line_num: Line number where the value was found.
        :returns: The timestamp converted to a time object.
        """

        if timestamp[-2] in ['am', 'pm']:
            noon_indicator = timestamp[-2:]
            timestamp = timestamp[:-2].strip()
        else:
            noon_indicator = None

        separators = re.findall('[^\w\d]', timestamp)
        bad_seps = set(separators) - set(':')

        for separator in bad_seps:
            msg = '#{}.Time separator \'{}\' corrected to \':\' (colon)' \
                  .format(table, separator)
            self.warnings.append((15, msg, line_num))

            timestamp = timestamp.replace(separator, ':')

        tokens = value.split(':')
        hour = tokens[0] or '00'
        minute = tokens[1] or '00' if len(tokens) > 1 else '00'
        second = tokens[2] or '00' if len(tokens) > 2 else '00'

        hour_numeric = minute_numeric = second_numeric = None

        try:
            hour_numeric = int(hour)
        except ValueError:
            msg = '#{}.Time hour contains invalid characters'.format(table)
            self.errors.append((16, msg, line_num))
        try:
            minute_numeric = int(minute)
        except ValueError:
            msg = '#{}.Time minute contains invalid characters'.format(table)
            self.errors.append((16, msg, line_num))
        try:
            second_numeric = int(second)
        except ValueError:
            msg = '#{}.Time second contains invalid characters'.format(table)
            self.errors.append((16, msg, line_num))

        if noon_indicator == 'am' and hour == 12:
            msg = '#{}.Time corrected from 12-hour clock to 24-hour' \
                  ' YYYY-mm-dd format'.format(table)
            self.warnings.append((11, msg, line_num))
            hour = 0
        elif noon_indicator == 'pm' and hour != 12:
             msg = '#{}.Time corrected from 12-hour clock to 24-hour' \
                  ' YYYY-mm-dd format'.format(table)
            self.warnings.append((11, msg, line_num))
            hour += 12

        if second_numeric is not None and second_numeric not in range(0, 60):
            msg = '#{}.Time second is not within allowable range [00]-[59]' \
                  .format(table)
            self.warnings.append((14, msg, line_num))

            while second_numeric >= 60 and minute_numeric is not None:
                second_numeric -= 60
                minute_numeric += 1
        if minute_numeric is not None and minute_numeric not in range(0, 60):
            msg = '#{}.Time minute is not within allowable range [00]-[59]' \
                  .format(table)
            self.warnings.append((13, msg, line_num))

            while minute_numeric >= 60 and hour_numeric is not None:
                minute_numeric -= 60
                hour_numeric += 1
        if hour_numeric is not None and hour_numeric not in range(0, 24):
            msg = '#{}.Time hour is not within allowable range [00]-[23]' \
                  .format(table)
            self.warnings.append((12, msg, line_num))

        if None in [hour_numeric, minute_numeric, second_numeric]:
            raise ValueError('Validation errors found in timestamp {}'
                             .format(timestamp))
        else:
            return time(hour, minute, second)

    def parse_datestamp(self, table, datestamp, line_num):
        """
        Return a date object representing the date contained in string
        <datestamp> according to the expected YYYY-MM-DD format.

        Corrects common formatting errors and performs very simple validation
        checks. Raises ValueError if the string cannot be parsed.

        The other parameters are used for error reporting.

        :param table: Name of table the value was found under.
        :param datestamp: String value taken from a Date column.
        :param line_num: Line number where the value was found.
        :returns: The datestamp converted to a datetime object.
        """

        separators = re.findall('[^\w\d]', datestamp)
        bad_seps = set(separators) - set('-')

        for separator in bad_seps:
            msg = '#{}.Date separator \'{}\' corrected to \'-\' (hyphen)' \
                  .format(table, separator)
            self.warnings.append((17, msg, line_num))

            datestamp = datestamp.replace(separator, '-')

        if '-' not in datestamp:
            msg = '#{}.Date missing separator. Proper date format is' \
                  ' YYYY-MM-DD'.format(table)
            self.errors.append((18, msg, line_num))
            raise ValueError(msg)

        tokens = datestamp.split('-')
        if len(tokens) < 3:
            msg = '#{}.Date incomplete'.format(table)
            self.errors.append((22, msg, line_num))
            raise ValueError(msg)
        elif len(tokens) > 3:
            msg = '#{}.Date has too many separators'.format(table)
            self.errors.append((23, msg, line_num))
            raise ValueError(msg)

        year = month = day = None

        try:
            year = int(sections[0])
        except ValueError:
            msg = '#{}.Date year contains invalid characters'.format(table)
            self.errors.append((24, msg, line_num))
        try:
            month = int(sections[1])
        except ValueError:
            msg = '#{}.Date month contains invalid characters'.format(table)
            self.errors.append((24, msg, line_num))
        try:
            day = int(sections[2])
        except ValueError:
            msg = '#{}.Date year contains invalid characters'.format(table)
            self.errors.append((24, msg, line_num))

        present_year = datetime.now().year
        if year is not None and year not in range(1940, present_year + 1):
            msg = '#{}.Date year is not within allowable range' \
                  '[1940]-[PRESENT]'.format(table)
            self.warnings.append(msg)
        if month is not None and month not in range(1, 12 + 1):
            msg = '#{}.Date month is not within allowable range' \
                  '[01]-[12]'.format(table)
            self.warnings.append(msg)
        if day is not None and day not in range(1, 31 + 1):
            msg = '#{}.Date day is not within allowable range' \
                  '[01]-[31]'.format(table)
            self.warnings.append(msg)

        if None in [year, month, day]:
            raise ValueError('')
        else:
            return datetime.strptime(datestamp, '%Y-%m-%d').date()

    def parse_utcoffset(self, table, utcoffset, line_num):
        """
        Validates the raw string <utcoffset>, converting it to the expected
        format defined by the regular expression (+|-)\d\d:\d\d:\d\d if
        possible. Returns the converted value or else raises a ValueError.

        The other parameters are used for error reporting.

        :param table: Name of table the value was found under.
        :param utcoffset: String value taken from a UTCOffset column.
        :param line_num: Line number where the value was found.
        :returns: The value converted to expected UTCOffset format.
        """

        separators = re.findall('[^-\+\w\d]', utcoffset)
        bad_seps = set(separators) - set(':')

        for separator in bad_seps:
            msg = '#{}.UTCOffset separator \'{}\' corrected to \':\'' \
                  ' (colon)'.format(table, separator)
            self.warnings.append((1000, msg, line_num))
            utcoffset = utcoffset.replace(separator, ':')

        sign = '(\+|-|\+-)?'
        delim = '[^-\+\w\d]'
        mandatory_place = '([\d]{1,2})'
        optional_place = '({}([\d]{0,2}))?'.format(delim)

        template = '^{sign}{mandatory}{optional}{optional}$' \
                   .format(sign=sign, mandatory=mandatory_place,
                           optional=optional_place)
        match = re.findall(template, utcoffset)

        if len(match) == 1:
            sign, hour, _, minute, _, second = match[0]

            if len(hour) < 2:
                msg = '#{}.UTCOffset hour should be 2 digits long' \
                      .format(table)
                self.warnings.append((1000, msg, line_num))
                hour = hour.rjust(2, '0')

            if not minute:
                msg = 'Missing #{}.UTCOffset minute, defaulting to 00' \
                      .format(table)
                self.warnings.append((1000, msg, line_num))
                minute = '00'
            elif len(minute) < 2:
                msg = '#{}.UTCOffset minute should be 2 digits long' \
                      .format(table)
                self.warnings.append((1000, msg, line_num))
                minute = minute.rjust(2, '0')

            if not second:
                msg = 'Missing #{}.UTCOffset second, defaulting to 00' \
                      .format(table)
                self.warnings.append((1000, msg, values_line))
                second = '00'
            elif len(second) < 2:
                msg = '#{}.UTCOffset second should be 2 digits long' \
                      .format(table)
                self.warnings.append((1000, msg, line_num))
                second = second.rjust(2, '0')

            if not sign:
                msg = 'Missing sign in #{}.UTCOffset, default to +' \
                      .format(table)
                self.warnings.append((1000, msg, line_num))
                sign = '+'
            elif sign == '+-':
                msg = 'Invalid sign {} in #{}.UTCOffset, correcting to {}' \
                      .format(sign, table, '-')
                self.warnings.append((1000, msg, line_num))
                sign = '-'

            try:
                magnitude = time(int(hour), int(minute), int(second))
                return = '{}{}'.format(sign, magnitude)
            except (ValueError, TypeError) as err:
                msg = 'Improperly formatted #{}.UTCOffset {}: {}' \
                      .format(table, str(err))
                self.errors.append((24, msg, line_num))
                raise ValueError(msg)

        template = '^{sign}[0]+{delim}?[0]*{delim}?[0]*' \
                   .format(sign=sign, delim=delim)
        match = re.findall(template, utcoffset)

        if len(match) == 1:
            msg = '{}.UTCOffset is a series of zeroes, correcting to' \
                  ' +00:00:00'.format(table_name)
            LOGGER.warning(msg)
            self.warnings.append((23, msg, values_line))

            return '+00:00:00'

        msg = 'Improperly formatted #{}.UTCOffset {}' \
              .format(table, utcoffset)
        self.errors.append((24, msg, values_line))
        raise ValueError(msg)

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

    def validate_metadata_tables(self):
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

        for table in present_tables:
            body = self.extcsv[table]
            arbitrary_column, values = next(iter(body.items()))

            start_line = self._line_num[table]
            values_line = start_line + 2

            if len(values) == 0:
                msg = 'Empty table {}'.format(table)
                line = self._line_num[table]
                self.warnings.append((140, msg, start_line))
            elif len(values) == 1:
                for field in body.keys():
                    body[field] = self.typecast_value(table, field,
                                                      body[field][0],
                                                      values_line)
            else:
                for field in body.keys():
                    body[field] = list(map(
                        lambda val: self.typecast_value(table, field, val,
                                                        values_line),
                        body[field]))

        if len(self.errors) == 0:
            LOGGER.debug('All tables in file validated.')
        else:
            raise MetadataValidationError('Invalid metadata', self.errors)

    def validate_dataset_tables(self):
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

        for table in present_tables:
            body = self.extcsv[table]
            arbitrary_column, values = next(iter(body.items()))

            start_line = self._line_num[table]
            values_line = start_line + 2

            if len(values) == 0:
                msg = 'Empty table {}'.format(table)
                line = self._line_num[table]
                self.warnings.append((140, msg, start_line))
            elif len(values) == 1:
                for field in body.keys():
                    body[field] = self.typecast_value(table, field,
                                                      body[field][0],
                                                      values_line)
            else:
                for field in body.keys():
                    body[field] = list(map(
                        lambda val: self.typecast_value(table, field, val,
                                                        values_line),
                        body[field]))

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
