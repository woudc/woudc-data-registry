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

from datetime import date, datetime, time
import os
import unittest

from woudc_data_registry import parser, processing, registry, search, util
from woudc_data_registry.parser import DOMAINS


def resolve_test_data_path(test_data_file):
    """
    helper function to ensure filepath is valid
    for different testing context (setuptools, directly, etc.)
    """

    if os.path.exists(test_data_file):
        return test_data_file
    else:
        path = os.path.join('woudc_data_registry', 'tests', test_data_file)
        if os.path.exists(path):
            return path


class ParserTest(unittest.TestCase):
    """Test suite for parser.py"""

    def test_get_value_type(self):
        """test value typing"""

        dummy = parser.ExtendedCSV('')

        self.assertIsNone(dummy.typecast_value('Dummy', 'TEst', '', 0))
        self.assertIsInstance(
            dummy.typecast_value('Dummy', 'TEST', 'foo', 0), str)
        self.assertIsInstance(
            dummy.typecast_value('Dummy', 'test', '1', 0), int)
        self.assertIsInstance(
            dummy.typecast_value('Dummy', 'test', '022', 0), str)
        self.assertIsInstance(
            dummy.typecast_value('Dummy', 'test', '1.0', 0), float)
        self.assertIsInstance(
            dummy.typecast_value('Dummy', 'test', '1.0-1', 0), str)
        self.assertIsInstance(
            dummy.typecast_value('Dummy', 'date', '2011-11-11', 0), date)
        self.assertIsInstance(
            dummy.typecast_value('Dummy', 'time', '11:11:11', 0), time)
        self.assertIsInstance(
            dummy.typecast_value('Dummy', 'utcoffset', '+00:00:00', 0), str)

        bad_input = 'a generic string'
        self.assertEquals(
            dummy.typecast_value('Dummy', 'date', bad_input, 0), bad_input)
        self.assertEquals(
            dummy.typecast_value('Dummy', 'time', bad_input, 0), bad_input)
        self.assertEquals(
            dummy.typecast_value('Dum', 'utcoffset', bad_input, 0), bad_input)

    def test_submissions(self):
        """ Test parsing of previously submitted Extended CSV files """

        # Error-free file
        contents = util.read_file(resolve_test_data_path(
            'data/20040709.ECC.2Z.2ZL1.NOAA-CMDL.csv'))

        ecsv = parser.ExtendedCSV(contents)
        ecsv.validate_metadata_tables()

        self.assertEqual('20040709.ECC.2Z.2ZL1.NOAA-CMDL.csv',
                         ecsv.gen_woudc_filename())

        # Error-free file with a space in its instrument name
        contents = util.read_file(resolve_test_data_path(
            'data/ecsv-space-in-instrument-name.csv'))
        ecsv = parser.ExtendedCSV(contents)
        ecsv.validate_metadata_tables()

        self.assertEqual('20111101.Brewer-foo.MKIII.201.RMDA.csv',
                         ecsv.gen_woudc_filename())
        self.assertTrue(set(DOMAINS['Common'].keys()).issubset(
                        set(ecsv.extcsv.keys())))

        # Error-free file with special, non-ASCII characters
        contents = util.read_file(resolve_test_data_path(
            'data/Brewer229_Daily_SEP2016.493'))

        ecsv = parser.ExtendedCSV(contents)
        ecsv.validate_metadata_tables()

        self.assertTrue(set(DOMAINS['Common'].keys()).issubset(
                        set(ecsv.extcsv.keys())))
        self.assertEqual(ecsv.extcsv['PLATFORM']['Name'], 'Río Gallegos')

    def test_non_extcsv(self):
        """ Test that various non-extcsv text files fail to parse """

        # Text file is not in Extended CSV format
        contents = util.read_file(resolve_test_data_path(
            'data/not-an-ecsv.dat'))

        with self.assertRaises(parser.NonStandardDataError):
            ecsv = parser.ExtendedCSV(contents)
            ecsv.validate_metadata_tables()

        # Text file not in Extended CSV format, featuring non-ASCII characters
        contents = util.read_file(resolve_test_data_path(
            'data/euc-jp.dat'))

        with self.assertRaises(parser.NonStandardDataError):
             ecsv = parser.ExtendedCSV(contents)
             ecsv.validate_metadata_tables()

    def test_missing_required_table(self):
        """ Test that files with missing required tables fail to parse """

        contents = util.read_file(resolve_test_data_path(
            'data/ecsv-missing-location-table.csv'))

        ecsv = parser.ExtendedCSV(contents)
        self.assertIsInstance(ecsv, parser.ExtendedCSV)

        with self.assertRaises(parser.MetadataValidationError):
            ecsv.validate_metadata_tables()

    def test_missing_required_value(self):
        """ Test that files with missing required values fail to parse """

        # File contains empty/null value for required field
        contents = util.read_file(resolve_test_data_path(
            'data/ecsv-missing-location-latitude.csv'))

        ecsv = parser.ExtendedCSV(contents)
        self.assertIsInstance(ecsv, parser.ExtendedCSV)

        with self.assertRaises(parser.MetadataValidationError):
            ecsv.validate_metadata_tables()

        # Required column is entirely missing in the table
        contents = util.read_file(resolve_test_data_path(
            'data/ecsv-missing-instrument-number.csv'))

        with self.assertRaises(parser.MetadataValidationError):
            ecsv = parser.ExtendedCSV(contents)
            ecsv.validate_metadata_tables()

    def test_missing_optional_table(self):
        """ Test that files with missing optional tables parse successfully """

        contents = util.read_file(resolve_test_data_path(
            'data/ecsv-missing-monthly-table.csv'))

        ecsv = parser.ExtendedCSV(contents)
        ecsv.validate_metadata_tables()

        ecsv.validate_dataset_tables()
        self.assertNotIn('MONTHLY', ecsv.extcsv)
        self.assertTrue(set(DOMAINS['Common']).issubset(
                        set(ecsv.extcsv.keys())))

    def test_missing_optional_value(self):
        """ Test that files with missing optional values parse successfully """

        # File contains empty/null value for optional LOCATION.Height
        contents = util.read_file(resolve_test_data_path(
            'data/ecsv-missing-location-height.csv'))

        ecsv = parser.ExtendedCSV(contents)
        ecsv.validate_metadata_tables()

        self.assertIsNone(ecsv.extcsv['LOCATION']['Height'])

        # File missing whole optional column - PLATFORM.GAW_ID
        contents = util.read_file(resolve_test_data_path(
            'data/ecsv-missing-platform-gawid.csv'))

        ecsv = parser.ExtendedCSV(contents)
        ecsv.validate_metadata_tables()

        self.assertIn('GAW_ID', ecsv.extcsv['PLATFORM'])
        self.assertIsNone(ecsv.extcsv['PLATFORM']['GAW_ID'])


class ProcessingTest(unittest.TestCase):
    """Test suite for processing.py"""

    def test_process(self):
        """test value typing"""

        registry_ = registry.Registry()
        elastic = search.SearchIndex()
        p = processing.Process(registry_, elastic)

        self.assertIsInstance(p.process_start, datetime)
        self.assertIsNone(p.process_end)

        result = p.validate(resolve_test_data_path(
            'data/wmo_acronym_vertical_sm.jpg'))
        self.assertFalse(result)

        result = p.validate(resolve_test_data_path(
            'data/euc-jp.dat'))
        self.assertFalse(result)


class UtilTest(unittest.TestCase):
    """Test suite for util.py"""

    def test_read_file(self):
        """test reading files"""

        contents = util.read_file(resolve_test_data_path(
            'data/20040709.ECC.2Z.2ZL1.NOAA-CMDL.csv'))

        self.assertIsInstance(contents, str)

        contents = util.read_file(resolve_test_data_path(
            'data/wmo_acronym_vertical_sm.jpg'))

        self.assertIsInstance(contents, str)

        with self.assertRaises(FileNotFoundError):
            contents = util.read_file('404file.dat')

    def test_is_binary_string(self):
        """test if the string is binary"""

        self.assertFalse(util.is_binary_string('foo'))
        self.assertFalse(util.is_binary_string(b'foo'))

    def test_is_text_file(self):
        """test if file is text-based"""

        res = resolve_test_data_path('data/20040709.ECC.2Z.2ZL1.NOAA-CMDL.csv')
        self.assertTrue(util.is_text_file(res))

        res = resolve_test_data_path('data/wmo_acronym_vertical_sm.jpg')
        self.assertFalse(util.is_text_file(res))

    def test_point2geojsongeometry(self):
        """test point GeoJSON geometry creation"""

        point = util.point2geojsongeometry(-75, 45)
        self.assertIsInstance(point, dict)
        self.assertEqual(point['type'], 'Point')
        self.assertIsInstance(point['coordinates'], list)
        self.assertEqual(len(point['coordinates']), 2)
        self.assertEqual(point['coordinates'][0], -75)
        self.assertEqual(point['coordinates'][1], 45)

        point = util.point2geojsongeometry(-75, 45, 333)
        self.assertEqual(len(point['coordinates']), 3)
        self.assertEqual(point['coordinates'][0], -75)
        self.assertEqual(point['coordinates'][1], 45)
        self.assertEqual(point['coordinates'][2], 333)

        point = util.point2geojsongeometry(-75, 45, 0)
        self.assertEqual(len(point['coordinates']), 2)
        self.assertEqual(point['coordinates'][0], -75)
        self.assertEqual(point['coordinates'][1], 45)

    def test_str2bool(self):
        """test boolean evaluation"""

        self.assertEqual(util.str2bool(True), True)
        self.assertEqual(util.str2bool(False), False)
        self.assertEqual(util.str2bool('1'), True)
        self.assertEqual(util.str2bool('0'), False)
        self.assertEqual(util.str2bool('true'), True)
        self.assertEqual(util.str2bool('false'), False)

    def test_json_serial(self):
        """test JSON serialization"""

        value = datetime.now()

        self.assertIsInstance(util.json_serial(value), str)

        with self.assertRaises(TypeError):
            util.json_serial('non_datetime_value')

    def test_is_plural(self):
        """test plural evaluation"""

        self.assertTrue(util.is_plural(0))
        self.assertFalse(util.is_plural(1))
        self.assertTrue(util.is_plural(2))


if __name__ == '__main__':
    unittest.main()
