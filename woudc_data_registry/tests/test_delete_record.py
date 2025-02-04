import unittest
import os
import subprocess
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from woudc_data_registry.models import DataRecord
from woudc_data_registry import config

"""
You need to set up a test environment for your tests. So setup and populate a
database and directory with files that have been ingested.

Change WDR_DB_NAME and WDR_SEARCH_INDEX for testing perposes.
"""


class TestBasicDeletion(unittest.TestCase):
    """Test case for basic functionality of deleting a record."""
    # I need to run 2 bash commands and then do some checks

    def test_01_file_deletion(self):
        """Run bash commands and verify the outcome."""

        # Bash commands to run
        commands = [
            'woudc-data-registry data ingest '
            './data/totalozone/totalozone-correct.csv',
            'woudc-data-registry data delete-record '
            + config.WDR_WAF_BASEDIR + '/Archive-NewFormat'
            '/TotalOzone_1.0_1/stn077/brewer/2010/totalozone-correct.csv',
            'rm ' + config.WDR_FILE_TRASH + '/totalozone-correct.csv'
        ]

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

        self.assertEqual(row_count, row_count_OG + 1)
        self.assertTrue(commands[1].split(' ')[-1] in result_list)

        # Deleting the File
        subprocess.run(commands[1], shell=True, check=True)

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

        subprocess.run(commands[2], shell=True, check=True)

        session.close()

    def test_02_absent_file_deletion(self):
        """
        Run bash commands and verify the outcome where the file
        path does not exist.
        """

        # Bash commands to run
        commands = [
            'woudc-data-registry data delete-record '
            + config.WDR_WAF_BASEDIR + '/Archive-NewFormat'
            '/TotalOzone_1.0_1/stn077/brewer/2010/totalozone-correct.csv'
        ]

        # Deleting the File
        with self.assertRaises(subprocess.CalledProcessError) as context:
            subprocess.run(commands[0], shell=True, check=True)

        # Optional: Verify the error message or exit code
        self.assertEqual(context.exception.returncode, 2)
        self.assertIn("woudc-data-registry", context.exception.cmd)

    def test_03_absent_file_DB_deletion(self):
        """
        Run bash commands and verify the outcome where the file path
        exists but the row does not.
        """
        commands = [
            'cp ./data/totalozone/totalozone-correct.csv '
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


if __name__ == '__main__':
    unittest.main()
