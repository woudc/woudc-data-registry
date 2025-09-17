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
# Copyright (c) 2024 Government of Canada
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

import json
import unittest
import uuid
from datetime import datetime
from unittest.mock import Mock, mock_open, patch

import requests

from woudc_data_registry.dobson_corrections import (custom_day_of_year,
                                                    fix_line_commas,
                                                    get_correct_factor,
                                                    parse_csv, parse_dat)
from woudc_data_registry.notification import pubsub


class DummyRecord:
    def __init__(self, x, y, z, published_datetime, timestamp_date,
                 timestamp_time, timestamp_utcoffset, data_record_id,
                 dataset_id, url, publish_filepath):
        self.x = x
        self.y = y
        self.z = z
        self.published_datetime = published_datetime
        self.timestamp_date = timestamp_date
        self.timestamp_time = timestamp_time
        self.timestamp_utcoffset = timestamp_utcoffset
        self.timestamp_utc = datetime.fromisoformat(
            f"{timestamp_date}T{timestamp_time}{timestamp_utcoffset}")
        self.data_record_id = data_record_id
        self.dataset_id = dataset_id
        self.url = url
        self.publish_filepath = publish_filepath
        self.content_category = 'TotalOzone'


class TestGenerateGeoJsonPayload(unittest.TestCase):
    def setUp(self):
        self.geojson_template = {
            "geometry": {"type": "Point", "coordinates": [1, 2, 3]},
            "properties": {
                "integrity": {}
            },
            "links": [{"href": ""}],
            "id": None
        }
        self.info = {
            "file1": {
                "record": DummyRecord(
                    x=1, y=2, z=3,
                    published_datetime=datetime.fromisoformat(
                        "2024-06-04T12:34:56+00:00"),
                    timestamp_date="2024-06-04",
                    timestamp_time="12:00:00",
                    timestamp_utcoffset="+00:00",
                    data_record_id="DATA123",
                    dataset_id="DSID123",
                    url="http://example.com",
                    publish_filepath=(
                        "/data/web/woudc-archive/Archive-NewFormat"
                        "/TotalOzone_1.0_1"
                        "/stn001/dobson/1959"
                        "/19590101.Dobson.Beck.053.UNKNOWN.csv")
                ),
                "status_code": 200,
                "message": "OK"
            }
        }

    @patch(
        "woudc_data_registry.util.open",
        new_callable=mock_open,
        read_data=(
            '{"geometry": {"type": "Point", "coordinates": []}, '
            '"properties": {"integrity": {}}, '
            '"links": [{"href": ""}], "id": null}'
        )
    )
    @patch("json.load")
    @patch("uuid.uuid4")
    def test_basic_payload(self, mock_uuid, mock_json_load, mock_file_open):
        mock_json_load.return_value = self.geojson_template.copy()
        mock_uuid.return_value = uuid.UUID("12345678123456781234567812345678")

        payload = pubsub.generate_geojson_payload(self.info)
        self.assertEqual(len(payload), 1)
        # Parse the JSON payload string
        notif = json.loads(payload[0]['payload'])

        self.assertEqual(notif["geometry"]["coordinates"], [1, 2, 3])
        self.assertEqual(
            notif["properties"]["pubtime"], "2024-06-04T12:34:56Z"
        )
        self.assertEqual(
            notif["properties"]["datetime"], "2024-06-04T12:00:00Z"
        )
        self.assertEqual(notif["properties"]["data_id"], "DATA123")
        self.assertEqual(
            notif["properties"]["metadata_id"],
            "urn:wmo:md:org-woudc:totalozone"
        )
        self.assertEqual(notif["links"][0]["href"], "http://example.com")
        self.assertEqual(
            str(notif["id"]), "12345678-1234-5678-1234-567812345678"
        )

    @patch.object(pubsub, 'LOGGER')
    @patch("hashlib.sha512")
    @patch("uuid.uuid4")
    @patch("json.load")
    @patch("builtins.open")
    @patch("woudc_data_registry.config", autospec=True)
    def test_none_x_or_y(self, mock_config, mock_file_open, mock_json_load,
                         mock_uuid, mock_sha512, mock_logger):
        # Mock the hash operations
        mock_hash_obj = Mock()
        mock_hash_obj.digest.return_value = b'mock_hash_digest_bytes'
        mock_sha512.return_value = mock_hash_obj

        # Set x to None to trigger geometry=null and error log
        record = DummyRecord(
            x=None, y=2, z=3,
            published_datetime=datetime.fromisoformat(
                "2024-06-04T12:34:56+00:00"),
            timestamp_date="2024-06-04", timestamp_time="12:00:00",
            timestamp_utcoffset="+00:00",
            data_record_id="DATA456", dataset_id="DSID456",
            url="http://example.com/2",
            publish_filepath=(
                        "/data/web/woudc-archive/Archive-NewFormat"
                        "/TotalOzone_1.0_1"
                        "/stn001/dobson/1959"
                        "/19590101.Dobson.Beck.053.UNKNOWN.csv")
        )
        info = {
            "file2": {
                "record": record,
                "status_code": 200,
                "message": "OK"
            }
        }

        # Simple file mocks - content doesn't matter now since we're
        # mocking the hash
        template_file = mock_open(read_data=(
            '{"geometry": {"type": "Point", "coordinates": []}, '
            '"properties": {"integrity": {}}, "links": [{"href": ""}], '
            '"id": null}')).return_value
        data_file = mock_open(read_data=b"any content").return_value
        mock_file_open.side_effect = [template_file, data_file]

        mock_json_load.return_value = self.geojson_template.copy()
        mock_uuid.return_value = uuid.UUID("12345678123456781234567812345678")

        payload = pubsub.generate_geojson_payload(info)
        notif = json.loads(payload[0]['payload'])
        self.assertIsNone(notif["geometry"])
        # Optionally, check that LOGGER.error was called
        mock_logger.error.assert_called_once_with('x or y is None')

        # Verify the hash was computed
        mock_sha512.assert_called_once()
        self.assertIn("integrity", notif["properties"])


class TestGetHTTPHeadResponse(unittest.TestCase):

    @patch('requests.head')
    def test_successful_response(self, mock_head):
        """Test success handling of requests.head"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.raise_for_status.return_value = None
        mock_head.return_value = mock_response

        response = requests.head(
            'https://woudc.org/archive/Archive-NewFormat/',
            timeout=5
        )
        self.assertEqual(response.status_code, 200)

    @patch('requests.head')
    def test_error_response(self, mock_head):
        """Test error/404 handling of requests.head"""
        mock_head.side_effect = requests.exceptions.RequestException(
            "Network error"
        )
        with self.assertRaises(requests.exceptions.RequestException):
            response = requests.head(
                'http://nonexistent.example.com',
                timeout=5
            )
            self.assertEqual(response.status_code, '404')


class TestDobsonCorrections(unittest.TestCase):
    """Test cases for Dobson corrections."""

    def test_parse_csv(self):
        """
        Test that the parse_csv function correctly reads a CSV file
        and returns the expected dictionary.
        """
        file_path = (
            'data/general/ecsv-comments.csv'
        )
        dictionary = parse_csv(file_path)
        self.assertIsInstance(dictionary, dict,
                              "Expected a dictionary from parse_csv.")
        self.assertEqual(dictionary.get('CONTENT')[1][1], 'OzoneSonde')
        self.assertEqual(dictionary.get('DATA_GENERATION')[1][1], 'NOAA-CMDL')
        self.assertEqual(dictionary.get('PLATFORM')[1][1], '440')
        self.assertEqual(dictionary.get('INSTRUMENT')[1][1], '2Z')
        self.assertEqual(dictionary.get('LOCATION')[1][1], '-66.57')
        self.assertEqual(dictionary.get('TIMESTAMP')[1][1], '2004-07-09')

        double_spaced_file_path = (
            'data/general/ecsv-double-spaced.csv'
        )
        dictionary2 = parse_csv(double_spaced_file_path)
        self.assertIsInstance(dictionary, dict,
                              "Expected a dictionary from parse_csv.")
        self.assertEqual(dictionary2.get('CONTENT')[1][1], 'TotalOzone')
        self.assertEqual(dictionary2.get('DATA_GENERATION')[1][1], 'RMDA')
        self.assertEqual(dictionary2.get('PLATFORM')[1][1], '002')
        self.assertEqual(dictionary2.get('INSTRUMENT')[1][1], 'MKIII')
        self.assertEqual(dictionary2.get('LOCATION')[1][1], '95.520')
        self.assertEqual(dictionary2.get('TIMESTAMP')[1][1], '2011-11-01')
        self.assertEqual(dictionary2.get('DAILY')[1][0], '2011-11-01')

        file_path_with_no_data = './not_real_file.csv'
        dictionary3 = parse_csv(file_path_with_no_data)
        self.assertEqual(dictionary3, {})

    def test_parse_csv_invalid_file(self):
        """
        Test that the parse_csv function raises a ValueError
        when given an invalid file path.
        """
        empty_file_path = (
            'data/general/pass_and_fail/'
            'KW160914.CSV'
        )
        dictionary = parse_csv(empty_file_path)
        self.assertIsInstance(dictionary, dict,
                              "Expected a dictionary from parse_csv.")
        self.assertEqual(len(dictionary), 0,
                         "Expected an empty dictionary for an invalid file.")
        self.assertEqual(dictionary, {})

        error_file_path = (
            'data/general/'
            'euc-jp.dat'
        )
        dictionary2 = parse_csv(error_file_path)
        self.assertIsInstance(dictionary2, dict,
                              "Expected a dictionary from parse_csv.")
        self.assertEqual(len(dictionary2), 0,
                         "Expected an empty dictionary for an invalid file.")
        self.assertEqual(dictionary2, {})

    def test_custom_doy_february(self):
        """Test custom_day_of_year for February dates."""
        with self.assertRaises(ValueError):
            datetime(2023, 2, 29)

        date = datetime(2024, 2, 29)  # Leap year
        self.assertEqual(custom_day_of_year(date), 60)

    def test_custom_doy_march(self):
        """Test custom_day_of_year for March dates."""
        date = datetime(2023, 3, 1)  # Non-leap year
        self.assertEqual(custom_day_of_year(date), 61)

        date = datetime(2024, 3, 1)
        self.assertEqual(custom_day_of_year(date), 61)

    def test_custom_doy_december(self):
        """Test custom_day_of_year for December dates."""
        date = datetime(2023, 12, 31)
        self.assertEqual(custom_day_of_year(date), 366)  # Normally 365 + 1

        date = datetime(2024, 12, 31)
        self.assertEqual(custom_day_of_year(date), 366)

    def test_fix_line_commas(self):
        """Test that the fix_line_commas function replaces commas correctly."""
        line = "2010-11-01,9,ZS,342.6,2.5,16.2,"
        fixed_line = fix_line_commas(line, 10)
        self.assertEqual(fixed_line.count(','), 10)

        # Test with no commas
        line_no_commas = "2010-11-01,9,ZS,342.6,2.5,16.2,19.5,18.2,8,3.6,-4.0"
        fixed_line_no_commas = fix_line_commas(line_no_commas, 10)
        self.assertEqual(fixed_line_no_commas.count(','), 10)

    def test_parse_dat_file(self):
        """
        Test that the parse_dat_file function reads a .dat file correctly.
        """
        file_path = (
            'data/dobsonCorrections'
            '/1_Leopoldville_Kinshasa_TEMISfilename_Brazzaville_Kinshasa_'
            'teff_abscoef.dat'
        )
        dictionary = parse_dat(file_path)
        self.assertIsInstance(dictionary, dict,
                              "Expected a dictionary from parse_csv.")
        self.assertEqual(
            dictionary.get('1'),
            ['-48.0852', '1.4224', '1.0068', '0.4513', '1.0171'])
        self.assertEqual(
            dictionary.get('2'),
            ['-48.1581', '1.4223', '1.0068', '0.4512', '1.0172'])
        self.assertEqual(
            dictionary.get('366'),
            ['-47.9949', '1.4225', '1.0067', '0.4513', '1.0170']
        )

    def test_correct_factor(self):
        """
        Test the get_correct_factor function to return the right
        correction factor.
        """
        file_path = (
            'data/dobsonCorrections'
            '/1_Leopoldville_Kinshasa_TEMISfilename_Brazzaville_Kinshasa_'
            'teff_abscoef.dat'
        )
        dictionary = parse_dat(file_path)
        self.assertEqual(
            get_correct_factor(dictionary, 'AD', '1'),
            (1.0068, -48.0852)
        )
        self.assertEqual(
            get_correct_factor(dictionary, 'CD', '1'),
            (1.0171, -48.0852)
        )
        self.assertEqual(
            get_correct_factor(dictionary, '', '1'),
            'Unknown Wavelength'
        )


if __name__ == '__main__':
    unittest.main()
