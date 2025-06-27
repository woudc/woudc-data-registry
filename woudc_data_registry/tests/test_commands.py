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

import unittest
import os
import subprocess
import uuid
from click.testing import CliRunner


from unittest.mock import patch, mock_open
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from woudc_data_registry.models import DataRecord
from woudc_data_registry import config
from woudc_data_registry.util import generate_geojson_payload
from woudc_data_registry.controller import gather, delete_record
from woudc_data_registry.dobson_corrections import number_of_lines_till_end

"""
You need to set up a test environment for your tests. So setup and populate a
database and directory with files that have been ingested.

Change WDR_DB_NAME and WDR_SEARCH_INDEX for testing perposes.
"""


class TestBasicDeletion(unittest.TestCase):
    """Test case for basic functionality of deleting a record."""

    def test_01_file_deletion(self):
        """Run bash commands and verify the outcome."""

        # Bash commands to run
        commands = [
            'woudc-data-registry data ingest '
            './woudc_data_registry/tests/data/totalozone/'
            'totalozone-correct.csv',
            'rm ' + config.WDR_FILE_TRASH + '/totalozone-correct.csv'
        ]

        runner = CliRunner()

        engine = create_engine(config.WDR_DATABASE_URL,
                               echo=config.WDR_DB_DEBUG)
        Session = sessionmaker(bind=engine, expire_on_commit=False)
        session = Session()

        filenames_OG = [
            file for file in os.listdir(config.WDR_FILE_TRASH)
            if os.path.isfile(os.path.join(config.WDR_FILE_TRASH, file))
        ]

        file_count_OG = len(filenames_OG)

        result_OG = session.query(DataRecord.output_filepath).all()
        result_list_OG = [row[0] for row in result_OG]
        row_count_OG = len(result_list_OG)
        print(result_list_OG)

        # Ingesting the File
        subprocess.run(commands[0], shell=True, check=True)

        result = session.query(DataRecord.output_filepath).all()
        result_list = [row[0] for row in result]
        row_count = len(result_list)

        output_filepath = (
            config.WDR_WAF_BASEDIR +
            '/Archive-NewFormat/TotalOzone_1.0_1/stn077/brewer/2010/'
            'totalozone-correct.csv'
        )

        self.assertEqual(row_count, row_count_OG + 1)
        self.assertTrue(output_filepath in result_list)

        # Deleting the File
        result = runner.invoke(
            delete_record,
            [
                config.WDR_WAF_BASEDIR +
                '/Archive-NewFormat/TotalOzone_1.0_1/stn077/brewer/2010/'
                'totalozone-correct.csv'
            ]
        )

        assert result.exit_code == 0

        filenames_01 = [
            file for file in os.listdir(config.WDR_FILE_TRASH)
            if os.path.isfile(os.path.join(config.WDR_FILE_TRASH, file))
        ]
        file_count_01 = len(filenames_01)

        result2 = session.query(DataRecord.output_filepath).all()
        result_list2 = [row[0] for row in result2]
        row_count2 = len(result_list2)

        self.assertEqual(file_count_01, file_count_OG + 1)
        self.assertEqual(row_count2, row_count_OG)
        self.assertEqual(result_list2, result_list_OG)
        self.assertFalse(commands[0].split('/')[-1] in result_list2)

        subprocess.run(commands[1], shell=True, check=True)

        session.close()

    def test_02_absent_file_deletion(self):
        """
        Run bash commands and verify the outcome where the file
        path does not exist.
        """
        runner = CliRunner()

        # Deleting the File
        result = runner.invoke(
            delete_record,
            [
                config.WDR_WAF_BASEDIR +
                '/Archive-NewFormat/TotalOzone_1.0_1/stn077/brewer/2010/'
                'totalozone-correct.csv'
            ]
        )

        # Check that it failed (non-zero exit code)
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("does not exist", result.output)

    def test_03_absent_file_DB_deletion(self):
        """
        Run bash commands and verify the outcome where the file path
        exists but the row does not.
        """
        commands = [
            'cp ./woudc_data_registry/tests/data/totalozone/'
            'totalozone-correct.csv '
            + config.WDR_WAF_BASEDIR + '/Archive-NewFormat'
            '/TotalOzone_1.0_1/stn077/brewer/2010',
            'rm ' + config.WDR_WAF_BASEDIR + '/Archive-NewFormat'
            '/TotalOzone_1.0_1/stn077/brewer/2010/totalozone-correct.csv'
        ]
        # Get information
        engine = create_engine(config.WDR_DATABASE_URL,
                               echo=config.WDR_DB_DEBUG)
        Session = sessionmaker(bind=engine, expire_on_commit=False)
        session = Session()

        filenames_OG = [
            file for file in os.listdir(config.WDR_FILE_TRASH)
            if os.path.isfile(os.path.join(config.WDR_FILE_TRASH, file))
        ]
        file_count_OG = len(filenames_OG)

        result_OG = session.query(DataRecord.output_filepath).all()
        result_list_OG = [row[0] for row in result_OG]
        row_count_OG = len(result_list_OG)

        # Copy the file to the WAF so the path exists
        # but the file is not in the DB
        subprocess.run(commands[0], shell=True, check=True)

        filenames_01 = [
            file for file in os.listdir(config.WDR_FILE_TRASH)
            if os.path.isfile(os.path.join(config.WDR_FILE_TRASH, file))
        ]
        file_count_01 = len(filenames_OG)

        result_01 = session.query(DataRecord.output_filepath).all()
        result_list_01 = [row[0] for row in result_01]
        row_count_01 = len(result_list_01)

        self.assertEqual(filenames_OG, filenames_01)
        self.assertEqual(file_count_OG, file_count_01)

        self.assertEqual(result_list_OG, result_list_01)
        self.assertEqual(row_count_OG, row_count_01)

        subprocess.run(commands[1], shell=True, check=True)


class DummyRecord:
    def __init__(self, x, y, z, published_datetime, timestamp_date,
                 timestamp_time, timestamp_utcoffset, data_record_id,
                 dataset_id, url):
        self.x = x
        self.y = y
        self.z = z
        self.published_datetime = published_datetime
        self.timestamp_date = timestamp_date
        self.timestamp_time = timestamp_time
        self.timestamp_utcoffset = timestamp_utcoffset
        self.data_record_id = data_record_id
        self.dataset_id = dataset_id
        self.url = url


class TestGenerateGeoJsonPayload(unittest.TestCase):
    def setUp(self):
        self.geojson_template = {
            "geometry": {"type": "Point", "coordinates": []},
            "properties": {},
            "links": [{"href": ""}],
            "id": None
        }
        self.info = {
            "file1": {
                "record": DummyRecord(
                    x=1, y=2, z=3,
                    published_datetime="2024-06-04T12:34:56Z",
                    timestamp_date="2024-06-04",
                    timestamp_time="12:00:00",
                    timestamp_utcoffset="+00:00",
                    data_record_id="DATA123",
                    dataset_id="DSID123",
                    url="http://example.com"
                ),
                "status_code": 200,
                "message": "OK"
            }
        }

    @patch("woudc_data_registry.config.WDR_NOTIFICATION_MESSAGE",
           "dummy_path.json")
    @patch(
        "woudc_data_registry.util.open",
        new_callable=mock_open,
        read_data=(
            '{"geometry": {"type": "Point", "coordinates": []}, '
            '"properties": {}, "links": [{"href": ""}], "id": null}'
        )
    )
    @patch("woudc_data_registry.util.json.load")
    @patch("woudc_data_registry.util.uuid.uuid4")
    def test_basic_payload(self, mock_uuid, mock_json_load, mock_file_open):
        mock_json_load.return_value = self.geojson_template.copy()
        mock_uuid.return_value = uuid.UUID("12345678123456781234567812345678")

        payload = generate_geojson_payload(self.info)
        self.assertEqual(len(payload), 1)
        notif = payload[0]
        self.assertEqual(notif["geometry"]["coordinates"], [1, 2, 3])
        self.assertEqual(
            notif["properties"]["pubtime"], "2024-06-04T12:34:56Z"
        )
        self.assertEqual(
            notif["properties"]["datetime"], "2024-06-04T12:00:00+00:00"
        )
        self.assertEqual(notif["properties"]["data_id"], "DATA123")
        self.assertEqual(
            notif["properties"]["metadata_id"], "urn:wmo:md:org-woudc:DSID123"
        )
        self.assertEqual(notif["links"][0]["href"], "http://example.com")
        self.assertEqual(
            str(notif["id"]), "12345678-1234-5678-1234-567812345678"
        )

    @patch("woudc_data_registry.util.LOGGER")
    @patch("woudc_data_registry.util.uuid.uuid4")
    @patch("woudc_data_registry.util.json.load")
    @patch(
        "woudc_data_registry.util.open",
        new_callable=mock_open,
        read_data='{}'
    )
    @patch("woudc_data_registry.config", autospec=True)
    def test_none_x_or_y(self, mock_config, mock_file_open, mock_json_load,
                         mock_uuid, mock_logger):
        # Set x to None to trigger geometry=null and error log
        record = DummyRecord(
            x=None, y=2, z=3,
            published_datetime="2024-06-04T12:34:56Z",
            timestamp_date="2024-06-04", timestamp_time="12:00:00",
            timestamp_utcoffset="+00:00",
            data_record_id="DATA456", dataset_id="DSID456",
            url="http://example.com/2"
        )
        info = {
            "file2": {
                "record": record,
                "status_code": 200,
                "message": "OK"
            }
        }
        mock_json_load.return_value = self.geojson_template.copy()
        mock_uuid.return_value = uuid.UUID("12345678123456781234567812345678")

        payload = generate_geojson_payload(info)
        notif = payload[0]
        self.assertIsNone(notif["geometry"])
        # Optionally, check that LOGGER.error was called
        mock_logger.error.assert_called_once_with('x or y is None')


class TestGathering(unittest.TestCase):
    def testconnection(self):
        """Test connection to the ftp account."""
        commands = [
            'rm -r yyyy-mm-dd'
            ]

        runner = CliRunner()
        result = runner.invoke(gather, ['yyyy-mm-dd'])

        assert result.exit_code == 0

        folders = [
            name for name in os.listdir('yyyy-mm-dd')
            if os.path.isdir(os.path.join('yyyy-mm-dd', name))
        ]

        folder_files = {
            folder: [
                file for file in os.listdir(os.path.join('yyyy-mm-dd', folder))
                if os.path.isfile(os.path.join('yyyy-mm-dd', folder, file))
            ]
            for folder in os.listdir('yyyy-mm-dd')
            if os.path.isdir(os.path.join('yyyy-mm-dd', folder))
        }

        shadoz_files = folder_files.get('shadoz', [])

        # Edit this accordingly
        self.assertGreater(len(folders), 0, "No folders found in yyyy-mm-dd")
        self.assertIn('shadoz', folders,
                      "shadoz folder not found in yyyy-mm-dd")

        self.assertGreater(len(shadoz_files), 0, "No files in yyyy-mm-dd")
        self.assertEqual(len(shadoz_files), 1659)

        subprocess.run(commands[0], shell=True, check=True,
                       stdout=subprocess.PIPE, text=True)


class TestDobsonCorrections(unittest.TestCase):
    """Test cases for Dobson corrections."""

    def test_number_of_lines_till_end(self):
        """
        Test that this function returns the correct number of lines
        in the corrected Dobson file.
        """
        num_lines = number_of_lines_till_end(
            './woudc_data_registry/tests/data/totalozoneobs/'
            'totalozoneobs-correct.csv'
        )
        self.assertEqual(num_lines, 3,
                         "Expected 1 line in the corrected Dobson file, "
                         f"got {num_lines} instead.")
        
        num_lines2 = number_of_lines_till_end(
            './woudc_data_registry/tests/data/general/ecsv-comments.csv'
        )
        self.assertEqual(num_lines2, 3,
                         "Expected 3 lines in the ecsv-comments file, "
                         f"got {num_lines2} instead.")

        num_lines3 = number_of_lines_till_end(
            './woudc_data_registry/tests/data/general/pass_and_fail/LT160223.CSV'
        )
        self.assertEqual(num_lines3, 2,
                         "Expected 1 line in the LT160223.CSV file, "
                         f"got {num_lines3} instead.")


if __name__ == '__main__':
    unittest.main()
