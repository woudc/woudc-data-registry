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

from datetime import date, datetime, time
import os
import unittest

from woudc_data_registry import parser, processing, util
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

        self.assertIsNone(parser._get_value_type('TEst', ''))
        self.assertIsInstance(parser._get_value_type('TEST', 'foo'), str)
        self.assertIsInstance(parser._get_value_type('test', '1'), int)
        self.assertIsInstance(parser._get_value_type('test', '022'), str)
        self.assertIsInstance(parser._get_value_type('test', '1.0'), float)
        self.assertIsInstance(parser._get_value_type('test', '1.0-1'), str)
        self.assertIsInstance(parser._get_value_type('date', '2011-11-11'),
                              date)
        self.assertIsInstance(parser._get_value_type('time', '11:11:11'), time)

    def test_ecsv(self):
        """test Extended CSV handling"""

        # good file
        contents = util.read_file(resolve_test_data_path(
            'data/20040709.ECC.2Z.2ZL1.NOAA-CMDL.csv'))

        ecsv = parser.ExtendedCSV(contents)
        self.assertIsInstance(ecsv, parser.ExtendedCSV)

        self.assertEqual(DOMAINS['metadata_tables'].keys(), ecsv.extcsv.keys())
        ecsv.validate_metadata()

        # good file, test special characters
        contents = util.read_file(resolve_test_data_path(
            'data/Brewer229_Daily_SEP2016.493'))

        ecsv = parser.ExtendedCSV(contents)
        self.assertIsInstance(ecsv, parser.ExtendedCSV)

        self.assertEqual(DOMAINS['metadata_tables'].keys(), ecsv.extcsv.keys())
        ecsv.validate_metadata()

        self.assertEqual(ecsv.extcsv['PLATFORM']['Name'], 'Río Gallegos')

        # bad file (not an ecsv)
        contents = util.read_file(resolve_test_data_path(
            'data/not-an-ecsv.dat'))

        ecsv = parser.ExtendedCSV(contents)
        self.assertIsInstance(ecsv, parser.ExtendedCSV)

        with self.assertRaises(parser.NonStandardDataError):
            ecsv.validate_metadata()

        # bad file (missing table)
        contents = util.read_file(resolve_test_data_path(
            'data/ecsv-missing-location-table.csv'))

        ecsv = parser.ExtendedCSV(contents)
        self.assertIsInstance(ecsv, parser.ExtendedCSV)

        with self.assertRaises(parser.MetadataValidationError):
            ecsv.validate_metadata()

        # bad file (missing data - LOCATION.Height)
        contents = util.read_file(resolve_test_data_path(
            'data/ecsv-missing-location-height.csv'))

        ecsv = parser.ExtendedCSV(contents)
        self.assertIsInstance(ecsv, parser.ExtendedCSV)

        self.assertEqual(DOMAINS['metadata_tables'].keys(), ecsv.extcsv.keys())

        with self.assertRaises(parser.MetadataValidationError):
            ecsv.validate_metadata()

        # bad file (invalid data - CONTENT.Category)
        contents = util.read_file(resolve_test_data_path(
            'data/ecsv-invalid-content-category.csv'))

        ecsv = parser.ExtendedCSV(contents)
        self.assertIsInstance(ecsv, parser.ExtendedCSV)

        self.assertEqual(DOMAINS['metadata_tables'].keys(), ecsv.extcsv.keys())

        with self.assertRaises(parser.MetadataValidationError):
            ecsv.validate_metadata()

        # bad file (invalid location latitude)
        contents = util.read_file(resolve_test_data_path(
            'data/ecsv-invalid-location-latitude.csv'))

        ecsv = parser.ExtendedCSV(contents)
        self.assertIsInstance(ecsv, parser.ExtendedCSV)

        with self.assertRaises(parser.MetadataValidationError):
            ecsv.validate_metadata()

        # bad file (invalid location longitude)
        contents = util.read_file(resolve_test_data_path(
            'data/ecsv-invalid-location-longitude.csv'))

        ecsv = parser.ExtendedCSV(contents)
        self.assertIsInstance(ecsv, parser.ExtendedCSV)

        with self.assertRaises(parser.MetadataValidationError):
            ecsv.validate_metadata()


class ProcessingTest(unittest.TestCase):
    """Test suite for processing.py"""

    def test_process(self):
        """test value typing"""

        p = processing.Process()
        self.assertIsNone(p.status)
        self.assertIsNone(p.code)
        self.assertIsNone(p.message)
        self.assertIsInstance(p.process_start, datetime)
        self.assertIsNone(p.process_end)

        result = p.process_data(resolve_test_data_path(
            'data/wmo_acronym_vertical_sm.jpg'))

        self.assertFalse(result)
        self.assertEqual(p.status, 'failed')
        self.assertEqual(p.code, 'NonStandardDataError')
        self.assertEqual(p.message, 'binary file detected')

        result = p.process_data(resolve_test_data_path(
            'data/euc-jp.dat'))

        self.assertFalse(result)
        self.assertEqual(p.status, 'failed')
        self.assertEqual(p.code, 'NonStandardDataError')
        # self.assertEqual(p.message, 'binary file detected')


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

    def test_point2ewkt(self):
        """test point WKT creation"""

        point = util.point2ewkt(-75, 45)
        self.assertEqual(point, 'SRID=4326;POINT(-75 45)')

        point = util.point2ewkt(-75, 45, 333)
        self.assertEqual(point, 'SRID=4326;POINTZ(-75 45 333)')

        point = util.point2ewkt(-75, 45, srid=4269)
        self.assertEqual(point, 'SRID=4269;POINT(-75 45)')

        point = util.point2ewkt(-75, 45, 111, srid=4269)
        self.assertEqual(point, 'SRID=4269;POINTZ(-75 45 111)')

        point = util.point2ewkt(-75, 45, 0, srid=4269)
        self.assertEqual(point, 'SRID=4269;POINT(-75 45)')

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


if __name__ == '__main__':
    unittest.main()
