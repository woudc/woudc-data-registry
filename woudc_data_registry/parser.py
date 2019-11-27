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

import re
import sys

import csv
import json
import yaml

import jsonschema
import logging

from io import StringIO
from datetime import datetime, time
from collections import OrderedDict

from woudc_data_registry import config
from woudc_data_registry.util import parse_integer_range


LOGGER = logging.getLogger(__name__)

with open(config.WDR_TABLE_SCHEMA) as table_schema_file:
    table_schema = json.load(table_schema_file)
with open(config.WDR_TABLE_CONFIG) as table_definitions:
    DOMAINS = yaml.safe_load(table_definitions)

try:
    jsonschema.validate(DOMAINS, table_schema)
except jsonschema.SchemaError as err:
    LOGGER.critical('Failed to read table definition schema:'
                    ' cannot process incoming files')
    raise err
except jsonschema.ValidationError as err:
    LOGGER.critical('Failed to read table definition file:'
                    ' cannot process incoming files')
    raise err


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
                success = False

        if not success:
            raise NonStandardDataError(self.errors)

    def line_num(self, table):
        """
        Returns the line in the source file at which <table> started.
        If there is no table in the file named <table>, returns None instead.
        """

        return self._line_num.get(table, None)

    def table_count(self, table_type):
        """
        Returns the number of tables named <table_type> in the source file.
        """

        return self._table_count.get(table_type, 0)

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

    def remove_table(self, table_name):
        """
        Remove a table from the memory of this Extended CSV instance.
        Does not alter the source file in any way.

        :param table_name: Name of the table to delete.
        """

        table_type = table_name.rstrip('0123456789_')

        self.extcsv.pop(table_name)
        self.line_num.pop(table_name)

        if self.table_count[table_type] > 1:
            self.table_count[table_type] -= 1
        else:
            self.table_count.pop(table_type)

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

    def collimate_tables(self, tables, schema):
        """
        Convert the lists of raw strings in all the tables in <tables>
        into processed values of an appropriate type.

        Ensure that all one-row tables have their single value reported
        for each field rather than a list containing the one value.

        Assumes that all tables in <tables> are demonstratedly valid.

        :param tables: List of tables in which to process columns.
        :param schema: A series of table definitions for the input file.
        """

        for table_name in tables:
            table_type = table_name.rstrip('0123456789_')
            body = self.extcsv[table_name]

            table_valueline = self.line_num(table_name) + 2

            for field, column in body.items():
                converted = [
                    self.typecast_value(table_name, field, val, line)
                    for line, val in enumerate(column, table_valueline)
                ]

                if schema[table_type]['rows'] == 1:
                    self.extcsv[table_name][field] = converted[0]
                else:
                    self.extcsv[table_name][field] = converted

    def check_table_occurrences(self, schema):
        """
        Validate the number of occurrences of each table type in <schema>
        against the expected range of occurrences for that type.
        Returns True iff all tables occur an acceptable number of times.

        :param schema: A series of table definitions for the input file.
        :returns: Whether all tables are within the expected occurrence range.
        """

        success = True

        for table_type in schema.keys():
            count = self.table_count(table_type)

            occurrence_range = str(schema[table_type]['occurrences'])
            lower, upper = parse_integer_range(occurrence_range)

            if count < lower:
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

        return success

    def check_table_height(self, table, definition, num_rows):
        """
        Validate the number of rows in the table named <table> against the
        expected range of rows assuming the table has <num_rows> rows.
        Returns True iff the table has an acceptable number of rows.

        :param table: Name of a table in the input file.
        :param definition: Schema definition of <table>.
        :param num_rows: The number of rows in <table>.
        :returns: Whether all tables are in the expected height range.
        """

        height_range = str(definition['rows'])
        lower, upper = parse_integer_range(height_range)

        occurrence_range = str(definition['occurrences'])
        is_required, _ = parse_integer_range(occurrence_range)

        table_startline = self.line_num(table)
        success = True

        if num_rows == 0:
            if is_required:
                msg = 'Required table #{} is empty'.format(table)
                self._error(27, table_startline, msg)
                success = False
            else:
                msg = 'Optional table #{} is empty'.format(table)
                self._warning(27.5, table_startline, msg)
        elif not lower <= num_rows <= upper:
            msg = 'Incorrectly formatted table: #{}. Table must contain' \
                  ' {} lines'.format(table, height_range)
            self._warning(27, table_startline, msg)

        return success

    def check_field_validity(self, table, definition):
        """
        Validates the fields of the table named <table>, ensuring that
        all required fields are present, correcting capitalizations,
        creating empty columns for any optional fields that were not
        provided, and removing any unrecognized fields.

        Returns True if the table's fields satisfy its definition.

        :param table: Name of a table in the input file.
        :param definition: Schema definition for the table.
        :returns: Whether fields satisfy the table's definition.
        """

        success = True

        required = definition.get('required_fields', ())
        optional = definition.get('optional_fields', ())
        provided = self.extcsv[table].keys()

        required_case_map = {key.lower(): key for key in required}
        optional_case_map = {key.lower(): key for key in optional}
        provided_case_map = {key.lower(): key for key in provided}

        missing_fields = [field for field in required
                          if field not in provided]
        extra_fields = [field for field in provided
                        if field.lower() not in required_case_map]

        table_fieldline = self.line_num(table) + 1
        table_valueline = table_fieldline + 1

        arbitrary_column = next(iter(self.extcsv[table].values()))
        num_rows = len(arbitrary_column)
        null_value = [''] * num_rows

        # Attempt to find a match for all required missing fields.
        for missing in missing_fields:
            match_insensitive = provided_case_map.get(missing.lower(), None)
            if match_insensitive:
                msg = 'Capitalization in #{} field {} corrected to {}' \
                      .format(table, missing, match_insensitive)
                self._warning(1000, table_fieldline, msg)

                self.extcsv[table][missing] = \
                    self.extcsv[table].pop(match_insensitive)
            else:
                msg = 'Missing required field {}.{}'.format(table, missing)
                self._error(3, table_fieldline, msg)
                self.extcsv[table][missing] = null_value
                success = False

        if len(missing_fields) == 0:
            LOGGER.debug('No missing fields in table {}'.format(table))
        else:
            LOGGER.info('Filled missing fields with null string values')

        # Assess whether non-required fields are optional fields or
        # excess ones that are not part of the table's schema.
        for extra in extra_fields:
            match_insensitive = optional_case_map.get(extra.lower(), None)

            if match_insensitive:
                LOGGER.info('Found optional field #{}.{}'
                            .format(table, extra))

                if extra != match_insensitive:
                    msg = 'Capitalization in #{} field {} corrected to' \
                          ' {}'.format(table, extra, match_insensitive)
                    self._warning(1000, table_fieldline, msg)

                    self.extcsv[table][match_insensitive] = \
                        self.extcsv[table].pop(extra)
            else:
                msg = 'Unknown column {} in table #{}'.format(extra, table)
                self._warning(4, table_fieldline, msg)
                del self.extcsv[table][extra]

        # Check that no required fields have empty values.
        for field in required:
            column = self.extcsv[table][field]

            if None in column:
                line = table_valueline + column.index(None)
                msg = 'Required value #{}.{} is empty'.format(table, field)
                self._error(5, line, msg)
                success = False

        return success

    def validate_metadata_tables(self):
        """validate core metadata tables and fields"""

        schema = DOMAINS['Common']
        success = True

        missing_tables = [table for table in schema
                          if table not in self.extcsv.keys()]
        present_tables = [table for table in self.extcsv.keys()
                          if table.rstrip('0123456789_') in schema]

        if len(present_tables) == 0:
            msg = 'No core metadata tables found. Not an Extended CSV file'
            self._error(161, None, msg)
            raise NonStandardDataError(self.errors)
        elif len(missing_tables) > 0:
            for missing in missing_tables:
                msg = 'Missing required table: {}'.format(missing)
                self._error(1, None, msg)

            msg = 'Not an Extended CSV file'
            raise MetadataValidationError(msg, self.errors)
        else:
            LOGGER.debug('No missing metadata tables')

        success &= self.check_table_occurrences(schema)

        for table in present_tables:
            table_type = table.rstrip('0123456789_')
            definition = schema[table_type]
            body = self.extcsv[table]

            arbitrary_column = next(iter(body.values()))
            num_rows = len(arbitrary_column)

            success &= self.check_field_validity(table, definition)
            success &= self.check_table_height(table, definition, num_rows)

            for field in definition.get('optional_fields', []):
                if field not in self.extcsv[table]:
                    self.extcsv[table][field] = [''] * num_rows

        if success:
            self.collimate_tables(present_tables, schema)
            LOGGER.debug('All tables in file validated.')
        else:
            raise MetadataValidationError('Invalid metadata', self.errors)

    def validate_dataset_tables(self):
        tables = DOMAINS['Datasets']
        curr_dict = tables
        fields_line = self.line_num('CONTENT') + 1

        for field in [('Category', str), ('Level', float), ('Form', int)]:
            field_name, type_converter = field
            key = str(type_converter(self.extcsv['CONTENT'][field_name]))

            if key in curr_dict:
                curr_dict = curr_dict[key]
            else:
                field_name = '#CONTENT.{}'.format(field_name.capitalize())
                msg = 'Cannot assess dataset table schema:' \
                      ' {} unknown'.format(field_name)
                self._warning(56, fields_line, msg)
                return False

        if '1' in curr_dict.keys():
            version = self.determine_version(curr_dict)
            LOGGER.info('Identified version as {}'.format(version))

            curr_dict = curr_dict[version]

        if self.check_dataset(curr_dict):
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

    def check_dataset(self, schema):
        success = True
        observations_table = schema.pop('data_table')

        required_tables = [name for name, body in schema.items()
                           if 'required_fields' in body]
        optional_tables = [name for name, body in schema.items()
                           if 'required_fields' not in body]

        missing_tables = [table for table in required_tables
                          if table not in self.extcsv.keys()]
        present_tables = [table for table in self.extcsv.keys()
                          if table.rstrip('0123456789_') in schema]

        required_tables.extend(DOMAINS['Common'].keys())
        extra_tables = [table for table in self.extcsv.keys()
                        if table.rstrip('0123456789_') not in required_tables]

        dataset = self.extcsv['CONTENT']['Category']
        for missing in missing_tables:
            msg = 'Missing required table(s) {} for category {}' \
                  .format(dataset, ', '.join(missing_tables))
            self._error(1, None, msg)
            success = False

        success |= self.check_table_occurrences(schema)

        for table in present_tables:
            table_type = table.rstrip('0123456789_')
            definition = schema[table_type]
            body = self.extcsv[table]

            arbitrary_column = next(iter(body.values()))
            num_rows = len(arbitrary_column)

            success |= self.check_field_validity(table, definition)
            success |= self.check_table_height(table, definition, num_rows)

            LOGGER.debug('Finished validating table {}'.format(table))

        for table in extra_tables:
            table_type = table.rstrip('0123456789_')
            if table_type not in optional_tables:
                msg = 'Excess table {} does not belong in {} file: removing' \
                      .format(table, dataset)
                LOGGER.warning(msg)
                self._warning(2, None, msg)
                del self.extcsv[table]

        for table in optional_tables:
            if table not in self.extcsv:
                LOGGER.warning('Optional table {} is not in file.'.format(
                               table))

        for i in range(1, self.table_count(observations_table) + 1):
            table_name = observations_table + '_' + str(i) \
                if i > 1 else observations_table
            arbitrary_column = next(iter(self.extcsv[table_name].values()))

            self.number_of_observations += len(arbitrary_column)

        self.collimate_tables(present_tables, schema)

        schema['data_table'] = observations_table
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
        ecsv.validate_metadata_tables()
    except MetadataValidationError as err:
        print(err.errors)
