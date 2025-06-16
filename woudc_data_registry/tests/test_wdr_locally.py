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
from click.testing import CliRunner

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from woudc_data_registry.models import DataRecord
from woudc_data_registry import config
from woudc_data_registry.controller import delete_record

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

        result_OG = session.query(DataRecord.publish_filepath).all()
        result_list_OG = [row[0] for row in result_OG]
        row_count_OG = len(result_list_OG)

        # Ingesting the File
        subprocess.run(commands[0], shell=True, check=True)

        result = session.query(DataRecord.publish_filepath).all()
        result_list = [row[0] for row in result]
        row_count = len(result_list)

        publish_filepath = (
            config.WDR_WAF_BASEDIR +
            '/Archive-NewFormat/TotalOzone_1.0_1/stn077/brewer/2010/'
            'totalozone-correct.csv'
        )

        self.assertEqual(row_count, row_count_OG + 1)
        self.assertTrue(publish_filepath in result_list)

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

        result2 = session.query(DataRecord.publish_filepath).all()
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

        result_OG = session.query(DataRecord.publish_filepath).all()
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

        result_01 = session.query(DataRecord.publish_filepath).all()
        result_list_01 = [row[0] for row in result_01]
        row_count_01 = len(result_list_01)

        self.assertEqual(filenames_OG, filenames_01)
        self.assertEqual(file_count_OG, file_count_01)

        self.assertEqual(result_list_OG, result_list_01)
        self.assertEqual(row_count_OG, row_count_01)

        subprocess.run(commands[1], shell=True, check=True)


if __name__ == '__main__':
    unittest.main()
