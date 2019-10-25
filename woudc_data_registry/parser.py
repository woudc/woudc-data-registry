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

from woudc_data_registry.util import parse_integer_range

LOGGER = logging.getLogger(__name__)

PROJECT_ROOT = '/home/hurkaa/conda-woudc-data-registry'
tables_file = 'tables_backfilling.yaml'

table_definition_path = os.path.join(PROJECT_ROOT, 'data', tables_file)
with open(table_definition_path) as table_definitions:
    DOMAINS = yaml.safe_load(table_definitions)


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

        self.warnings = []
        self.errors = []

        LOGGER.debug('Reading into csv')
        self._raw = content
        reader = csv.reader(StringIO(self._raw))

        LOGGER.debug('Parsing object model')
        parent_table = None
        lines = enumerate(reader, 1)

        success = True
        for line_num, row in lines:
            separators = []
            for bad_sep in ['::', ';', '$', '%', '|', '/', '\\']:
                if not non_content_line(row) and bad_sep in row[0]:
                    separators.append(bad_sep)

            for separator in separators:
                comma_separated = row[0].replace(separator, ',')
                row = next(csv.reader(StringIO(comma_separated)))

                msg = 'Improper delimiter used \'{}\', corrected to \',\'' \
                      ' (comma)'.format(separator)
                self._warning(7, line_num, msg)

            if len(row) == 1 and row[0].startswith('#'):  # table name
                parent_table = ''.join(row).lstrip('#').strip()

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
                        self._warning(8, ln, msg)

                        ln, fields = next(lines)

                    self.init_table(parent_table, fields, line_num)
                except StopIteration:
                    msg = 'Table {} has no fields'.format(parent_table)
                    self._error(6, line_num, msg)
                    success = False
            elif len(row) > 0 and row[0].startswith('*'):  # comment
                LOGGER.debug('Found comment')
                continue
            elif non_content_line(row):  # blank line
                LOGGER.debug('Found blank line')
                continue
            elif parent_table is not None and not non_content_line(row):
                table_values = row
                self.add_values_to_table(parent_table, table_values, line_num)
            else:
                msg = 'Unrecognized data {}'.format(','.join(row))
                self._error(9, line_num, msg)

        if not success:
            raise NonStandardDataError(self.errors)

    def line_num(self, table):
        """
        Returns the line in the source file at which <table> started.
        """

        return self._line_num[table]

    def table_count(self, table_type):
        """
        Returns the number of tables named <table_type> in the source file.
        """

        return self._table_count[table_type]

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
            self._warning(7, line_num, msg)

        values.extend([''] * fillins)
        values = values[:len(fields)]

        for field, value in zip(fields, values):
            self.extcsv[table_name][field].append(value.strip())

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
            self._error(1000, line_num, msg)
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

        separators = re.findall(r'[^\w\d]', timestamp)
        bad_seps = set(separators) - set(':')

        for separator in bad_seps:
            msg = '#{}.Time separator \'{}\' corrected to \':\' (colon)' \
                  .format(table, separator)
            self._warning(15, line_num, msg)

            timestamp = timestamp.replace(separator, ':')

        tokens = timestamp.split(':')
        hour = tokens[0] or '00'
        minute = tokens[1] or '00' if len(tokens) > 1 else '00'
        second = tokens[2] or '00' if len(tokens) > 2 else '00'

        hour_numeric = minute_numeric = second_numeric = None

        try:
            hour_numeric = int(hour)
        except ValueError:
            msg = '#{}.Time hour contains invalid characters'.format(table)
            self._error(16, line_num, msg)
        try:
            minute_numeric = int(minute)
        except ValueError:
            msg = '#{}.Time minute contains invalid characters'.format(table)
            self._error(16, line_num, msg)
        try:
            second_numeric = int(second)
        except ValueError:
            msg = '#{}.Time second contains invalid characters'.format(table)
            self._error(16, line_num, msg)

        if noon_indicator == 'am' and hour == 12:
            msg = '#{}.Time corrected from 12-hour clock to 24-hour' \
                  ' YYYY-mm-dd format'.format(table)
            self._warning(11, line_num, msg)
            hour = 0
        elif noon_indicator == 'pm' and hour != 12:
            msg = '#{}.Time corrected from 12-hour clock to 24-hour' \
                  ' YYYY-mm-dd format'.format(table)
            self._warning(11, line_num, msg)
            hour += 12

        if second_numeric is not None and second_numeric not in range(0, 60):
            msg = '#{}.Time second is not within allowable range [00]-[59]' \
                  .format(table)
            self._warning(14, line_num, msg)

            while second_numeric >= 60 and minute_numeric is not None:
                second_numeric -= 60
                minute_numeric += 1
        if minute_numeric is not None and minute_numeric not in range(0, 60):
            msg = '#{}.Time minute is not within allowable range [00]-[59]' \
                  .format(table)
            self._warning(13, line_num, msg)

            while minute_numeric >= 60 and hour_numeric is not None:
                minute_numeric -= 60
                hour_numeric += 1
        if hour_numeric is not None and hour_numeric not in range(0, 24):
            msg = '#{}.Time hour is not within allowable range [00]-[23]' \
                  .format(table)
            self._warning(12, line_num, msg)

        if None in [hour_numeric, minute_numeric, second_numeric]:
            raise ValueError('Validation errors found in timestamp {}'
                             .format(timestamp))
        else:
            return time(hour_numeric, minute_numeric, second_numeric)

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

        separators = re.findall(r'[^\w\d]', datestamp)
        bad_seps = set(separators) - set('-')

        for separator in bad_seps:
            msg = '#{}.Date separator \'{}\' corrected to \'-\' (hyphen)' \
                  .format(table, separator)
            self._warning(17, line_num, msg)

            datestamp = datestamp.replace(separator, '-')

        if '-' not in datestamp:
            msg = '#{}.Date missing separator. Proper date format is' \
                  ' YYYY-MM-DD'.format(table)
            self._error(18, line_num, msg)
            raise ValueError(msg)

        tokens = datestamp.split('-')
        if len(tokens) < 3:
            msg = '#{}.Date incomplete'.format(table)
            self._error(22, line_num, msg)
            raise ValueError(msg)
        elif len(tokens) > 3:
            msg = '#{}.Date has too many separators'.format(table)
            self._error(23, line_num, msg)
            raise ValueError(msg)

        year = month = day = None

        try:
            year = int(tokens[0])
        except ValueError:
            msg = '#{}.Date year contains invalid characters'.format(table)
            self._error(24, line_num, msg)
        try:
            month = int(tokens[1])
        except ValueError:
            msg = '#{}.Date month contains invalid characters'.format(table)
            self._error(24, line_num, msg)
        try:
            day = int(tokens[2])
        except ValueError:
            msg = '#{}.Date year contains invalid characters'.format(table)
            self._error(24, line_num, msg)

        present_year = datetime.now().year
        if year is not None and year not in range(1940, present_year + 1):
            msg = '#{}.Date year is not within allowable range' \
                  '[1940]-[PRESENT]'.format(table)
            self._warning(1000, line_num, msg)
        if month is not None and month not in range(1, 12 + 1):
            msg = '#{}.Date month is not within allowable range' \
                  '[01]-[12]'.format(table)
            self._warning(1000, line_num, msg)
        if day is not None and day not in range(1, 31 + 1):
            msg = '#{}.Date day is not within allowable range' \
                  '[01]-[31]'.format(table)
            self._warning(1000, line_num, msg)

        if None in [year, month, day]:
            raise ValueError('')
        else:
            return datetime.strptime(datestamp, '%Y-%m-%d').date()

    def parse_utcoffset(self, table, utcoffset, line_num):
        """
        Validates the raw string <utcoffset>, converting it to the expected
        format defined by the regular expression (+|-)\\d\\d:\\d\\d:\\d\\d if
        possible. Returns the converted value or else raises a ValueError.

        The other parameters are used for error reporting.

        :param table: Name of table the value was found under.
        :param utcoffset: String value taken from a UTCOffset column.
        :param line_num: Line number where the value was found.
        :returns: The value converted to expected UTCOffset format.
        """

        separators = re.findall(r'[^-\+\w\d]', utcoffset)
        bad_seps = set(separators) - set(':')

        for separator in bad_seps:
            msg = '#{}.UTCOffset separator \'{}\' corrected to \':\'' \
                  ' (colon)'.format(table, separator)
            self._warning(1000, line_num, msg)
            utcoffset = utcoffset.replace(separator, ':')

        sign = r'(\+|-|\+-)?'
        delim = r'[^-\+\w\d]'
        mandatory_place = r'([\d]{1,2})'
        optional_place = '(' + delim + r'([\d]{0,2}))?'

        template = '^{sign}{mandatory}{optional}{optional}$' \
                   .format(sign=sign, mandatory=mandatory_place,
                           optional=optional_place)
        match = re.findall(template, utcoffset)

        if len(match) == 1:
            sign, hour, _, minute, _, second = match[0]

            if len(hour) < 2:
                msg = '#{}.UTCOffset hour should be 2 digits long' \
                      .format(table)
                self._warning(1000, line_num, msg)
                hour = hour.rjust(2, '0')

            if not minute:
                msg = 'Missing #{}.UTCOffset minute, defaulting to 00' \
                      .format(table)
                self._warning(1000, line_num, msg)
                minute = '00'
            elif len(minute) < 2:
                msg = '#{}.UTCOffset minute should be 2 digits long' \
                      .format(table)
                self._warning(1000, line_num, msg)
                minute = minute.rjust(2, '0')

            if not second:
                msg = 'Missing #{}.UTCOffset second, defaulting to 00' \
                      .format(table)
                self._warning(1000, line_num, msg)
                second = '00'
            elif len(second) < 2:
                msg = '#{}.UTCOffset second should be 2 digits long' \
                      .format(table)
                self._warning(1000, line_num, msg)
                second = second.rjust(2, '0')

            if not sign:
                msg = 'Missing sign in #{}.UTCOffset, default to +' \
                      .format(table)
                self._warning(1000, line_num, msg)
                sign = '+'
            elif sign == '+-':
                msg = 'Invalid sign {} in #{}.UTCOffset, correcting to {}' \
                      .format(sign, table, '-')
                self._warning(1000, line_num, msg)
                sign = '-'

            try:
                magnitude = time(int(hour), int(minute), int(second))
                return '{}{}'.format(sign, magnitude)
            except (ValueError, TypeError) as err:
                msg = 'Improperly formatted #{}.UTCOffset {}: {}' \
                      .format(table, str(err))
                self._error(24, line_num, msg)
                raise ValueError(msg)

        template = '^{sign}[0]+{delim}?[0]*{delim}?[0]*' \
                   .format(sign=sign, delim=delim)
        match = re.findall(template, utcoffset)

        if len(match) == 1:
            msg = '{}.UTCOffset is a series of zeroes, correcting to' \
                  ' +00:00:00'.format(table)
            self._warning(23, line_num, msg)

            return '+00:00:00'

        msg = 'Improperly formatted #{}.UTCOffset {}' \
              .format(table, utcoffset)
        self._error(24, line_num, msg)
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
                          if table.rstrip('0123456789_') in DOMAINS['Common']]

        if len(present_tables) == 0:
            msg = 'No core metadata tables found. Not an Extended CSV file'
            self._error(161, None, msg)
        else:
            for missing in missing_tables:
                msg = 'Missing required table: {}'.format(missing)
                self._error(1, None, msg)

        if self.errors:
            msg = 'Not an Extended CSV file'
            raise MetadataValidationError(msg, self.errors)
        else:
            LOGGER.debug('No missing metadata tables.')

        for table_type, schema in DOMAINS['Common'].items():
            count = self.table_count(table_type)
            schema = DOMAINS['Common'][table_type]

            lower, upper = parse_integer_range(str(schema['occurrences']))
            if count < lower:
                msg = 'At least {} occurrencess of table #{} are required' \
                      .format(lower, table_type)
                line = self.line_num(table_type + '_' + str(count))
                self._error(1000, line, msg)
            elif count > upper:
                msg = 'Cannot have more than {} occurrences of #{}' \
                      .format(upper, table_type)
                line = self.line_num(table_type + '_' + str(upper + 1))
                self._error(26, line, msg)

        for table, definitions in DOMAINS['Common'].items():
            required = definitions.get('required', ())
            optional = definitions.get('optional', ())
            provided = self.extcsv[table].keys()

            required_case_map = {key.lower(): key for key in required}
            optional_case_map = {key.lower(): key for key in optional}
            provided_case_map = {key.lower(): key for key in provided}

            missing_fields = [field for field in required
                              if field not in provided]
            excess_fields = [field for field in provided
                             if field.lower() not in required_case_map]

            start_line = self.line_num(table)
            fields_line = start_line + 1
            values_line = fields_line + 1

            if len(missing_fields) == 0:
                LOGGER.debug('No missing fields in table {}'.format(table))
            for missing in missing_fields:
                match_insensitive = provided_case_map.get(missing.lower(),
                                                          None)
                if match_insensitive:
                    msg = 'Capitalization of #{} field {} corrected to' \
                          ' {}'.format(table, missing, match_insensitive)
                    self._warning(1000, fields_line, msg)

                    self.extcsv[table][missing] = \
                        self.extcsv[table].pop(match_insensitive)
                else:
                    msg = 'Missing required #{} field {}' \
                          .format(table, missing)
                    self._error(3, fields_line, msg)

            for field in excess_fields:
                match_insensitive = optional_case_map.get(field.lower(), None)

                if match_insensitive:
                    msg = 'Found optional field #{}.{}'.format(table, field)
                    LOGGER.info(msg)

                    if field != match_insensitive:
                        msg = 'Capitalization of #{} field {} corrected to' \
                              ' {}'.format(table, field, match_insensitive)
                        self._warning(1000, fields_line, msg)

                        self.extcsv[table][match_insensitive] = \
                            self.extcsv[table].pop(field)
                else:
                    msg = 'Field name {}.{} is not from approved list' \
                          .format(table, field)
                    line = self.line_num(table) + 1
                    self._warning(4, line, msg)
                    del self.extcsv[table][field]

            num_rows = 0
            for field in required:
                column = self.extcsv[table][field]
                num_rows = len(column)

                for line, value in enumerate(column, values_line):
                    if not value:
                        msg = 'Required value #{}.{} is empty'.format(table,
                                                                      field)
                        self._error(5, values_line, msg)
                        break

            occurrence_range = str(definitions['occurrences'])
            lower, upper = parse_integer_range(occurrence_range)
            if num_rows == 0:
                if 'required' in definitions:
                    msg = 'Required table #{} is empty'.format(table)
                    self._error(27, start_line, msg)
                else:
                    msg = 'Optional table #{} is empty'.format(table)
                    self._warning(27.5, start_line, msg)
            elif not lower <= num_rows <= upper:
                msg = 'Incorrectly formatted table: #{}. Table must contain' \
                      ' {} lines'.format(table, occurrence_range)
                self._warning(27, start_line, msg)

        for table in present_tables:
            table_type = table.rstrip('0123456789_')
            body = self.extcsv[table]

            start_line = self.line_num(table)
            values_line = start_line + 2

            for field, column in body.items():
                converted = [self.typecast_value(table, field, val, line)
                             for line, val in enumerate(column, values_line)]

                if DOMAINS['Common'][table_type]['rows'] == 1:
                    self.extcsv[table][field] = converted[0]
                else:
                    self.extcsv[table][field] = converted

        if len(self.errors) == 0:
            LOGGER.debug('All tables in file validated.')
        else:
            raise MetadataValidationError('Invalid metadata', self.errors)

    def validate_dataset_tables(self):
        tables = DOMAINS['Datasets']
        curr_dict = tables
        fields_line = self.line_num('CONTENT') + 1

        for field in ['Category', 'Level', 'Form']:
            key = self.extcsv['CONTENT'][field]

            if key in curr_dict:
                curr_dict = curr_dict[key]
            else:
                field_name = '#CONTENT.{}'.format(field.capitalize())
                msg = 'Cannot assess dataset table schema:' \
                      ' {} unknown'.format(field_name)
                self._warning(56, fields_line, msg)
                return False

        if 1 in curr_dict.keys():
            version = self.determine_version(curr_dict)
            LOGGER.info('Identified version as {}'.format(version))

            curr_dict = curr_dict[version]

        if self.check_tables(curr_dict):
            LOGGER.debug('All dataset tables in file validated')
        else:
            raise MetadataValidationError('Dataset tables failed validation',
                                          self.errors)

    def determine_version(self, schema):
        """
        Attempt to determine which of multiple possible table definitions
        contained in <schema> fits the instance's Extended CSV file best,
        based on which required or optional tables are present.

        Returns the best-fitting version, or raises an error if there is
        not enough information.

        :param schema: Dictionary with nested dictionaries of
                       table definitions as values.
        """

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
            self._error(108, None, msg)
            raise NonStandardDataError(self.errors)
        else:
            for version in versions:
                if rating(version) == best_match:
                    return version

    def check_tables(self, schema):
        success = True

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
            self._error(1, None, msg)
            success = False

        for table_type in schema.keys():
            if table_type not in self._table_count:
                continue
            count = self.table_count(table_type)

            lower, upper = parse_integer_range(
                str(schema[table_type]['occurrences']))
            if table_type in required_tables and count < lower:
                msg = 'At least {} occurrencess of table #{} are required' \
                      .format(lower, table_type)
                line = self.line_num(table_type + '_' + str(count))
                self._error(1000, line, msg)
                success = False
            if count > upper:
                msg = 'Cannot have more than {} occurrences of #{}' \
                      .format(upper, table_type)
                line = self.line_num(table_type + '_' + str(upper + 1))
                self._error(26, line, msg)
                success = False

        for table in present_tables:
            table_type = table.rstrip('0123456789_')

            required = schema[table_type].get('required', ())
            optional = schema[table_type].get('optional', ())
            provided = self.extcsv[table].keys()

            required_case_map = {key.lower(): key for key in required}
            optional_case_map = {key.lower(): key for key in optional}
            provided_case_map = {key.lower(): key for key in provided}

            missing_fields = [field for field in required
                              if field not in provided]
            extra_fields = [field for field in provided
                            if field.lower() not in required_case_map]

            start_line = self.line_num(table)
            fields_line = start_line + 1
            values_line = fields_line + 1

            arbitrary_column = next(iter(self.extcsv[table].values()))
            num_rows = len(arbitrary_column)
            null_value = [''] * num_rows

            for field in missing_fields:
                match_insensitive = provided_case_map.get(field.lower(), None)

                if match_insensitive:
                    msg = 'Capitalization in #{} field {} corrected to' \
                          ' {}'.format(table, field, match_insensitive)
                    self._warning(1000, fields_line, msg)

                    self.extcsv[table][field] = \
                        self.extcsv[table].pop(match_insensitive)
                else:
                    msg = 'Missing required field {}.{}'.format(table, field)
                    self._error(3, fields_line, msg)
                    self.extcsv[table][field] = null_value
                    success = False
            if len(missing_fields) > 0:
                LOGGER.info('Filled missing fields with null string values')

            for field in extra_fields:
                match_insensitive = optional_case_map.get(field.lower(), None)

                if match_insensitive:
                    LOGGER.info('Found optional field #{}.{}'
                                .format(table, match_insensitive))

                    if field != match_insensitive:
                        msg = 'Capitalization in #{} field {} corrected to' \
                              ' {}'.format(table, field, match_insensitive)
                        self._warning(1000, fields_line, msg)

                        self.extcsv[table][match_insensitive] = \
                            self.extcsv[table].pop(field)
                else:
                    msg = 'Removing excess column #{}.{}' \
                          .format(field, table)
                    self._warning(4, fields_line, msg)
                    del self.extcsv[table][field]

            for field in required:
                column = self.extcsv[table][field]
                for line, value in enumerate(column, values_line):
                    if not value:
                        msg = 'Required value #{}.{} is empty' \
                              .format(table, field)
                        self._error(5, line, msg)
                        success = False
                        break

            table_height_range = str(schema[table_type]['rows'])
            lower, upper = parse_integer_range(table_height_range)
            if num_rows == 0:
                if 'required' in schema[table_type]:
                    msg = 'Required table #{} is empty'.format(table)
                    self._error(27, start_line, msg)
                    success = False
                else:
                    msg = 'Optional table #{} is empty'.format(table)
                    self._warning(27.5, start_line, msg)
            elif not lower <= num_rows <= upper:
                msg = 'Incorrectly formatted table: #{}. Table must contain' \
                      ' {} lines'.format(table, table_height_range)

            LOGGER.debug('Successfully validated table {}'.format(table))

        for table in extra_tables:
            table_type = table.rstrip('0123456789_')
            if table_type not in optional_tables:
                msg = 'Excess table {} does not belong in {} file: removing' \
                      .format(table, dataset)
                LOGGER.warning(msg)
                self._warning(2, None, msg)
                del self.extcsv[table]

        for table in optional:
            if table not in self.extcsv:
                LOGGER.warning('Optional table {} is not in file.'.format(
                               table))

        for table in present_tables:
            table_type = table.rstrip('0123456789_')
            body = self.extcsv[table]

            start_line = self.line_num(table)
            values_line = start_line + 2

            for field, column in body.items():
                converted = [self.typecast_value(table, field, val, line)
                             for line, val in enumerate(column, values_line)]

                if schema[table_type]['rows'] == 1:
                    self.extcsv[table][field] = converted[0]
                else:
                    self.extcsv[table][field] = converted

        return success


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
