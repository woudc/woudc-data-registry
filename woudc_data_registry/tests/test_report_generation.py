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
# Copyright (c) 2025 Government of Canada
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

"""
Test suite for for data report related functions
"""

import csv
import pathlib
import os
import unittest

from datetime import datetime
from woudc_extcsv import (ExtendedCSV, MetadataValidationError,
                          NonStandardDataError)

from woudc_data_registry import models, report, util


SANDBOX_DIR = '/tmp/woudc-data-registry'


def dummy_extcsv(source):
    """
    Returns a woudc-extcsv ExtendedCSV instace built from the filepath <source>
    with dummy output settings (no logs or reports).
    """

    with report.OperatorReport() as error_bank:
        return ExtendedCSV(source, error_bank)


def resolve_test_data_path(test_data_file):
    """
    helper function to ensure filepath is valid
    for different testing context (setuptools, directly, etc.)

    :param test_data_file: Relative path to an input file.
    :returns: Full path to the input file.
    """

    if os.path.exists(test_data_file):
        return test_data_file
    else:
        path = os.path.join('woudc_data_registry', 'tests', test_data_file)
        if os.path.exists(path):
            return path


def clear_sandbox():
    """
    Clean up report generation tests by deleting any files in the
    sandbox directory.
    """

    for filename in os.listdir(SANDBOX_DIR):
        fullpath = os.path.join(SANDBOX_DIR, filename)
        os.remove(fullpath)


class SandboxTestSuite(unittest.TestCase):
    """Superclass for test classes that write temporary files to a sandbox"""

    @classmethod
    def setUpClass(cls):
        os.mkdir(SANDBOX_DIR)

    @classmethod
    def tearDownClass(cls):
        os.rmdir(SANDBOX_DIR)

    def tearDown(self):
        clear_sandbox()


class OperatorReportTest(SandboxTestSuite):
    """Test suite for OperatorReport, error severity, and file format"""

    def test_operator_report_output_location(self):
        """Test that operator reports write a file in the working directory"""

        with report.OperatorReport(SANDBOX_DIR) as op_report:
            report_filepath = op_report.filepath()
            self.assertIsNotNone(report_filepath)
            assert report_filepath is not None
            operator_path = pathlib.Path(report_filepath)
            self.assertEqual(str(operator_path.parent), SANDBOX_DIR)

    def test_uses_error_definition(self):
        """Test that error/warning feedback responds to input files"""

        # The two error files below have different error types for error 1.
        all_errors = resolve_test_data_path('config/errors.csv')

        with report.OperatorReport(SANDBOX_DIR) as op_report:
            op_report.read_error_definitions(all_errors)

            self.assertTrue(op_report.has_error_definition(245))
            _, success = op_report.add_message(245, table="flight_summary")
            self.assertFalse(success)

            self.assertTrue(op_report.has_error_definition(101))
            _, success = op_report.add_message(101)
            self.assertFalse(success)

    def test_passing_operator_report(self):
        """Test that a passing file is written in the operator report"""

        filename = '20080101.Kipp_Zonen.UV-S-E-T.000560.PMOD-WRC.csv'
        infile = str(resolve_test_data_path(f'data/general/{filename}'))
        contents = util.read_file(infile)

        with report.OperatorReport(SANDBOX_DIR) as op_report:
            ecsv = ExtendedCSV(contents, op_report)

            ecsv.validate_metadata_tables()
            ecsv.validate_dataset_tables()
            data_record = models.DataRecord(ecsv)
            data_record.filename = filename  # type: ignore[assignment]

            agency = ecsv.extcsv['DATA_GENERATION']['Agency']

            output_path = os.path.join(SANDBOX_DIR,
                                       'operator-report.csv')

            op_report.add_message(405)  # File passes validation
            op_report.write_passing_file(infile, ecsv, data_record)

        self.assertTrue(os.path.exists(output_path))
        with open(output_path, encoding='utf-8') as output:
            reader = csv.reader(output)
            next(reader)

            report_line = next(reader)
            self.assertEqual(report_line[0], 'P')
            self.assertEqual(report_line[2], '405')
            self.assertIn(agency, report_line)
            self.assertIn(os.path.basename(infile), report_line)

            with self.assertRaises(StopIteration):
                next(reader)

    def test_warning_operator_report(self):
        """Test that file warnings are written in the operator report"""

        filename = 'ecsv-trailing-commas.csv'
        infile = str(resolve_test_data_path(f'data/general/{filename}'))
        contents = util.read_file(infile)

        with report.OperatorReport(SANDBOX_DIR) as op_report:
            ecsv = ExtendedCSV(contents, op_report)

            # Some warnings are encountered during parsing.
            ecsv.validate_metadata_tables()
            ecsv.validate_dataset_tables()
            data_record = models.DataRecord(ecsv)
            data_record.filename = filename  # type: ignore[assignment]

            agency = ecsv.extcsv['DATA_GENERATION']['Agency']

            output_path = os.path.join(SANDBOX_DIR,
                                       'operator-report.csv')

            op_report.add_message(405)  # File passes validation
            op_report.write_passing_file(infile, ecsv, data_record)

        self.assertTrue(os.path.exists(output_path))
        with open(output_path, encoding='utf-8') as output:
            reader = csv.reader(output)
            next(reader)

            expected_warnings = len(ecsv.warnings)
            for _ in range(expected_warnings):
                report_line = next(reader)

                self.assertEqual(report_line[0], 'P')
                self.assertEqual(report_line[1], 'Warning')
                self.assertIn(agency, report_line)
                self.assertIn(os.path.basename(infile), report_line)

            report_line = next(reader)
            self.assertEqual(report_line[0], 'P')
            self.assertEqual(report_line[1], 'Warning')
            self.assertEqual(report_line[2], '405')
            self.assertIn(agency, report_line)
            self.assertIn(os.path.basename(infile), report_line)

            with self.assertRaises(StopIteration):
                next(reader)

    def test_failing_operator_report(self):
        """Test that a failing file is written in the operator report"""

        filename = 'ecsv-missing-instrument-name.csv'
        infile = str(resolve_test_data_path(f'data/general/{filename}'))
        contents = util.read_file(infile)

        ecsv = None
        agency = 'UNKNOWN'

        with report.OperatorReport(SANDBOX_DIR) as op_report:
            try:
                ecsv = ExtendedCSV(contents, op_report)
                ecsv.validate_metadata_tables()
                agency = ecsv.extcsv['DATA_GENERATION']['Agency']

                ecsv.validate_dataset_tables()
                raise AssertionError(f'Parsing of {infile} did not fail')
            except (MetadataValidationError,
                    NonStandardDataError):
                output_path = os.path.join(SANDBOX_DIR, 'run1')

                op_report.add_message(410)
                op_report.write_failing_file(infile, agency, ecsv)

        output_path = os.path.join(SANDBOX_DIR,
                                   'operator-report.csv')

        self.assertIsNotNone(ecsv)
        assert ecsv is not None
        self.assertTrue(os.path.exists(output_path))
        with open(output_path, encoding='utf-8') as output:
            reader = csv.reader(output)
            next(reader)

            warnings = 0
            errors = 0

            expected_warnings = len(ecsv.warnings)
            expected_errors = len(ecsv.errors)
            for _ in range(expected_warnings + expected_errors):
                report_line = next(reader)
                self.assertEqual(report_line[0], 'F')

                if report_line[1] == 'Warning':
                    warnings += 1
                elif report_line[1] == 'Error':
                    errors += 1

            self.assertEqual(warnings, expected_warnings)
            self.assertEqual(errors, expected_errors)

            report_line = next(reader)
            self.assertEqual(report_line[0], 'F')
            self.assertEqual(report_line[1], 'Error')
            self.assertEqual(report_line[2], '410')
            self.assertIn(agency, report_line)
            self.assertIn(os.path.basename(infile), report_line)

            with self.assertRaises(StopIteration):
                next(reader)

    def test_mixed_operator_report(self):
        """
        Test that passing and failing files are written to the operator report
        when a mixture of the two is processed
        """

        infile_root = str(resolve_test_data_path('data/general/pass_and_fail'))

        warnings = {}
        errors = {}

        expected_warnings = {}
        expected_errors = {}

        agency = 'UNKNOWN'

        with report.OperatorReport(SANDBOX_DIR) as op_report:
            for infile in os.listdir(infile_root):
                fullpath = os.path.join(infile_root, infile)

                warnings[fullpath] = 0
                errors[fullpath] = 0

                try:
                    contents = util.read_file(fullpath)
                    ecsv = ExtendedCSV(contents, op_report)
                except (MetadataValidationError,
                        NonStandardDataError) as err:
                    expected_errors[fullpath] = len(err.errors)

                    op_report.add_message(410)
                    op_report.write_failing_file(fullpath, agency)
                    continue

                try:
                    ecsv.validate_metadata_tables()
                    agency = ecsv.extcsv['DATA_GENERATION']['Agency']

                    ecsv.validate_dataset_tables()
                    data_record = models.DataRecord(ecsv)
                    data_record.filename = infile  # type: ignore[assignment]

                    expected_warnings[fullpath] = len(ecsv.warnings)
                    expected_errors[fullpath] = 0
                    op_report.write_passing_file(fullpath, ecsv, data_record)
                except (MetadataValidationError,
                        NonStandardDataError):
                    expected_warnings[fullpath] = len(ecsv.warnings)
                    expected_errors[fullpath] = len(ecsv.errors)

                    op_report.add_message(410)
                    op_report.write_failing_file(fullpath, agency, ecsv)

        output_path = os.path.join(SANDBOX_DIR,
                                   'operator-report.csv')

        self.assertTrue(os.path.exists(output_path))
        with open(output_path, encoding='utf-8') as output:
            reader = csv.reader(output)
            next(reader)

            for line in reader:
                if expected_errors[line[12]] == 0:
                    self.assertEqual(line[0], 'P')
                    self.assertEqual(line[1], 'Warning')
                else:
                    self.assertEqual(line[0], 'F')

                if line[2] == '405':
                    self.assertEqual(expected_errors[line[12]], 0)
                elif line[2] == '410':
                    self.assertGreater(expected_errors[line[12]], 0)
                elif line[1] == 'Warning':
                    warnings[line[12]] += 1
                elif line[1] == 'Error':
                    errors[line[12]] += 1

        self.assertEqual(warnings, expected_warnings)
        self.assertEqual(errors, expected_errors)


class RunReportTest(SandboxTestSuite):
    """Test suite for RunReport, file writing and file format"""

    def test_run_report_output_location(self):
        """Test that run reports write a file in the working directory"""

        run_report = report.RunReport(SANDBOX_DIR)

        filepath = run_report.filepath()
        self.assertIsNotNone(filepath)
        assert filepath is not None
        run_report_path = pathlib.Path(filepath)
        self.assertEqual(str(run_report_path.parent), SANDBOX_DIR)

    def test_passing_run_report(self):
        """Test that a passing file is written to the run report"""

        filename = '20080101.Kipp_Zonen.UV-S-E-T.000560.PMOD-WRC.csv'
        infile = str(resolve_test_data_path(f'data/general/{filename}'))
        contents = util.read_file(infile)

        run_report = report.RunReport(SANDBOX_DIR)
        with report.OperatorReport() as error_bank:
            ecsv = ExtendedCSV(contents, error_bank)

            ecsv.validate_metadata_tables()
            ecsv.validate_dataset_tables()
            data_record = models.DataRecord(ecsv)
            data_record.filename = filename  # type: ignore[assignment]

            agency = ecsv.extcsv['DATA_GENERATION']['Agency']
            output_path = os.path.join(SANDBOX_DIR, 'run_report')

            run_report.write_passing_file(infile, agency)

        self.assertTrue(os.path.exists(output_path))
        with open(output_path, encoding='utf-8') as output:
            lines = output.read().splitlines()
            self.assertEqual(len(lines), 2)

            self.assertEqual(lines[0], agency)
            self.assertEqual(lines[1], f'Pass: {infile}')

    def test_failing_run_report(self):
        """Test that a failing file is written to the run report"""

        filename = 'ecsv-missing-instrument-name.csv'
        infile = str(resolve_test_data_path(f'data/general/{filename}'))
        contents = util.read_file(infile)

        ecsv = None
        # Agency typically filled in with FTP username for failing files.
        agency = 'rmda'

        with report.OperatorReport() as error_bank:
            run_report = report.RunReport(SANDBOX_DIR)
            output_path = os.path.join(SANDBOX_DIR, 'run_report')

            try:
                ecsv = ExtendedCSV(contents, error_bank)
                ecsv.validate_metadata_tables()
                agency = ecsv.extcsv['DATA_GENERATION']['Agency']

                ecsv.validate_dataset_tables()
                raise AssertionError(f'Parsing of {infile} did not fail')
            except (MetadataValidationError,
                    NonStandardDataError):
                run_report.write_failing_file(infile, agency)

        self.assertTrue(os.path.exists(output_path))
        with open(output_path, encoding='utf-8') as output:
            lines = output.read().splitlines()
            self.assertEqual(len(lines), 2)

            self.assertEqual(lines[0], agency)
            self.assertEqual(lines[1], f'Fail: {infile}')

    def test_non_extcsv_run_report(self):
        """Test that an unparseable file is written to the run report"""

        filename = 'not-an-ecsv.dat'
        infile = str(resolve_test_data_path(f'data/general/{filename}'))
        contents = util.read_file(infile)

        agency = 'UNKNOWN'

        with report.OperatorReport() as error_bank:
            run_report = report.RunReport(SANDBOX_DIR)

            try:
                _ = ExtendedCSV(contents, error_bank)
                raise AssertionError(f'Parsing of {infile} did not fail')
            except (MetadataValidationError,
                    NonStandardDataError):
                output_path = os.path.join(SANDBOX_DIR, 'run_report')

                run_report.write_failing_file(infile, agency)

                self.assertTrue(os.path.exists(output_path))
                with open(output_path, encoding='utf-8') as output:
                    lines = output.read().splitlines()
                    self.assertEqual(len(lines), 2)

                    self.assertEqual(lines[0], agency)
                    self.assertEqual(lines[1], f'Fail: {infile}')

    def test_mixed_run_report(self):
        """
        Test that passing and failing files are written to the run report
        when a mixture of the two is processed
        """

        infile_root = str(resolve_test_data_path('data/general/pass_and_fail'))

        agency = 'MSC'

        expected_passes = set()
        expected_fails = set()

        with report.OperatorReport() as error_bank:
            run_report = report.RunReport(SANDBOX_DIR)

            for infile in os.listdir(infile_root):
                fullpath = os.path.join(infile_root, infile)

                try:
                    contents = util.read_file(fullpath)
                    ecsv = ExtendedCSV(contents, error_bank)
                except (MetadataValidationError,
                        NonStandardDataError):
                    expected_fails.add(fullpath)
                    run_report.write_failing_file(fullpath, agency)
                    continue

                try:
                    ecsv.validate_metadata_tables()
                    ecsv.validate_dataset_tables()
                    data_record = models.DataRecord(ecsv)
                    data_record.filename = infile  # type: ignore[assignment]

                    expected_passes.add(fullpath)
                    run_report.write_passing_file(fullpath, agency)
                except (MetadataValidationError,
                        NonStandardDataError):
                    expected_fails.add(fullpath)
                    run_report.write_failing_file(fullpath, agency)

        self.assertEqual(len(expected_passes), 6)
        self.assertEqual(len(expected_fails), 4)

        output_path = os.path.join(SANDBOX_DIR, 'run_report')
        self.assertTrue(os.path.exists(output_path))

        with open(output_path, encoding='utf-8') as output:
            lines = output.read().splitlines()
            self.assertEqual(lines[0], agency)
            self.assertEqual(len(lines),
                             len(expected_passes) + len(expected_fails) + 1)

            for line in lines[1:]:
                if line.startswith('Pass'):
                    target = line[6:].strip()
                    self.assertIn(target, expected_passes)
                elif line.startswith('Fail'):
                    target = line[6:].strip()
                    self.assertIn(target, expected_fails)

    def test_run_report_multiple_agencies(self):
        """Test that files in the run report are grouped by agency"""

        infile_root = str(resolve_test_data_path('data/general/agencies'))

        expected_passes = {}
        expected_fails = {}
        agency_aliases = {
            'msc': 'MSC',
            'casiap': 'CAS-IAP',
            'mlcd-lu': 'MLCD-LU',
            'dwd-mohp': 'DWD-MOHp'
        }

        with report.OperatorReport() as error_bank:
            run_report = report.RunReport(SANDBOX_DIR)

            for dirpath, _, filenames in os.walk(infile_root):
                for infile in filenames:
                    fullpath = os.path.join(dirpath, infile)
                    # Agency inferred from directory name.
                    agency = dirpath.split('/')[-1]

                    try:
                        contents = util.read_file(fullpath)
                        ecsv = ExtendedCSV(contents, error_bank)
                    except (MetadataValidationError,
                            NonStandardDataError):
                        if agency not in expected_passes:
                            expected_passes[agency] = set()
                        if agency not in expected_fails:
                            expected_fails[agency] = set()
                        expected_fails[agency].add(fullpath)
                        run_report.write_failing_file(fullpath, agency)
                        continue

                    try:
                        ecsv.validate_metadata_tables()
                        agency = ecsv.extcsv['DATA_GENERATION']['Agency']

                        if agency not in expected_passes:
                            expected_passes[agency] = set()
                        if agency not in expected_fails:
                            expected_fails[agency] = set()

                        ecsv.validate_dataset_tables()
                        data_record = models.DataRecord(ecsv)
                        data_record.filename = infile  # type: ignore[assignment] # noqa

                        expected_passes[agency].add(fullpath)
                        run_report.write_passing_file(fullpath, agency)
                    except (MetadataValidationError,
                            NonStandardDataError):
                        agency = agency_aliases[agency]
                        if agency not in expected_passes:
                            expected_passes[agency] = set()
                        if agency not in expected_fails:
                            expected_fails[agency] = set()

                        expected_fails[agency].add(fullpath)
                        run_report.write_failing_file(fullpath, agency)

        self.assertEqual(len(expected_passes['CAS-IAP']), 1)
        self.assertEqual(len(expected_passes['DWD-MOHp']), 2)
        self.assertEqual(len(expected_passes['MLCD-LU']), 3)
        self.assertEqual(len(expected_passes['MSC']), 4)

        self.assertEqual(len(expected_fails['CAS-IAP']), 0)
        self.assertEqual(len(expected_fails['DWD-MOHp']), 1)
        self.assertEqual(len(expected_fails['MLCD-LU']), 0)
        self.assertEqual(len(expected_fails['MSC']), 1)

        output_path = os.path.join(SANDBOX_DIR, 'run_report')
        self.assertTrue(os.path.exists(output_path))

        with open(output_path, encoding='utf-8') as output:
            lines = output.read().splitlines()
            curr_agency = None

            for line in lines:
                if line.startswith('Pass'):
                    target = line[6:]
                    self.assertIn(target, expected_passes[curr_agency])
                elif line.startswith('Fail'):
                    target = line[6:]
                    self.assertIn(target, expected_fails[curr_agency])
                elif line.strip() != '':
                    curr_agency = line.strip()
                    self.assertIn(line, agency_aliases.values())


class EmailSummaryTest(SandboxTestSuite):
    """
    Test suite for EmailSummary, output format, and detection of passes,
    fixes, and fails
    """

    def test_email_summary_output_location(self):
        """Test that email summaries write a file in the working directory"""

        email_report = report.EmailSummary(SANDBOX_DIR)

        email_report_path = pathlib.Path(email_report.filepath())
        self.assertEqual(str(email_report_path.parent), SANDBOX_DIR)

    def test_find_operator_report_empty(self):
        """Test that no operator reports are found when none exist"""

        project_root = resolve_test_data_path('data/reports')
        email_report = report.EmailSummary(project_root)

        operator_reports = email_report.find_operator_reports()

        self.assertEqual([], operator_reports)

    def test_find_operator_report_one_run(self):
        """Test that operator reports are found when one exists"""

        project_root = resolve_test_data_path('data/reports/one_pass')
        email_report = report.EmailSummary(project_root)

        operator_reports = email_report.find_operator_reports()
        expected_parent = resolve_test_data_path('data/reports/one_pass/run1')

        self.assertEqual(1, len(operator_reports))
        self.assertIn(expected_parent, operator_reports[0])

    def test_find_operator_report_many_runs(self):
        """
        Test that all operator reports are found when they are spread
        across multiple run directories
        """

        project_root = resolve_test_data_path('data/reports/six_reports')
        email_report = report.EmailSummary(project_root)

        operator_reports = email_report.find_operator_reports()

        self.assertEqual(6, len(operator_reports))

        for run_number in range(1, 6 + 1):
            expected_path = resolve_test_data_path(
                f'data/reports/six_reports/run{run_number}/operator-report-9999-12-31.csv')  # noqa
            self.assertIn(expected_path, set(operator_reports))

    def test_email_summary_single_pass(self):

        """Test email report generation for a single passing file"""

        input_root = resolve_test_data_path('data/reports/one_pass')
        email_report = report.EmailSummary(input_root, SANDBOX_DIR)

        emails = {'MSC': 'placeholder@site.com'}
        email_report.write(emails)

        today = datetime.now().strftime('%Y-%m-%d')
        output_filename = f'failed-files-{today}'
        output_path = os.path.join(SANDBOX_DIR, output_filename)

        self.assertTrue(os.path.exists(output_path))

        with open(output_path, encoding='utf-8') as output:
            lines = output.read().splitlines()
            self.assertEqual(len(lines), 5)

            self.assertEqual(lines[0], 'MSC (placeholder@site.com)')
            self.assertEqual(lines[1], 'Total files received: 1')
            self.assertEqual(lines[2], 'Number of passed files: 1')
            self.assertEqual(lines[3], 'Number of manually repaired files: 0')
            self.assertEqual(lines[4], 'Number of failed files: 0')

    def test_email_summary_single_fail(self):
        """Test email report generation for a single failing file"""

        input_root = resolve_test_data_path('data/reports/one_fail')
        email_report = report.EmailSummary(input_root, SANDBOX_DIR)

        emails = {'MSC': 'placeholder@site.com'}
        email_report.write(emails)

        today = datetime.now().strftime('%Y-%m-%d')
        output_filename = f'failed-files-{today}'
        output_path = os.path.join(SANDBOX_DIR, output_filename)

        self.assertTrue(os.path.exists(output_path))

        with open(output_path, encoding='utf-8') as output:
            lines = output.read().splitlines()
            self.assertEqual(len(lines), 8)

            self.assertEqual(lines[0], 'MSC (placeholder@site.com)')
            self.assertEqual(lines[1], 'Total files received: 1')
            self.assertEqual(lines[2], 'Number of passed files: 0')
            self.assertEqual(lines[3], 'Number of manually repaired files: 0')
            self.assertEqual(lines[4], 'Number of failed files: 1')

            self.assertEqual(lines[5], 'Summary of Failures:')
            self.assertNotIn('.csv', lines[6])
            self.assertEqual(lines[7], 'file1.csv')

    def test_email_summary_one_run_mixed_pass_fail(self):
        """
        Test email report generation with passing and failing files
        all in one operator report
        """

        input_root = resolve_test_data_path('data/reports//pass_and_fail')
        email_report = report.EmailSummary(input_root, SANDBOX_DIR)

        emails = {'MSC': 'placeholder@site.com'}
        email_report.write(emails)

        today = datetime.now().strftime('%Y-%m-%d')
        output_filename = f'failed-files-{today}'
        output_path = os.path.join(SANDBOX_DIR, output_filename)

        self.assertTrue(os.path.exists(output_path))

        with open(output_path, encoding='utf-8') as output:
            lines = output.read().splitlines()
            self.assertEqual(len(lines), 10)

            self.assertEqual(lines[0], 'MSC (placeholder@site.com)')
            self.assertEqual(lines[1], 'Total files received: 5')
            self.assertEqual(lines[2], 'Number of passed files: 2')
            self.assertEqual(lines[3], 'Number of manually repaired files: 0')
            self.assertEqual(lines[4], 'Number of failed files: 3')

            self.assertEqual(lines[5], 'Summary of Failures:')
            self.assertNotIn('.csv', lines[6])
            # Alphabetical order of files: the first one has capital F.
            self.assertEqual(lines[7], 'File5.csv')
            self.assertEqual(lines[8], 'file2.csv')
            self.assertEqual(lines[9], 'file3.csv')

    def test_email_summary_multiple_causes_one_group(self):
        """
        Test email report generation where a single group of files
        experiences multiple error types.
        """

        input_root = resolve_test_data_path('data/reports/multiple_causes')
        email_report = report.EmailSummary(input_root, SANDBOX_DIR)

        emails = {'MSC': 'placeholder@site.com'}
        email_report.write(emails)

        today = datetime.now().strftime('%Y-%m-%d')
        output_filename = f'failed-files-{today}'
        output_path = os.path.join(SANDBOX_DIR, output_filename)

        self.assertTrue(os.path.exists(output_path))

        with open(output_path, encoding='utf-8') as output:
            lines = output.read().splitlines()
            self.assertEqual(len(lines), 12)

            self.assertEqual(lines[0], 'MSC (placeholder@site.com)')
            self.assertEqual(lines[1], 'Total files received: 5')
            self.assertEqual(lines[2], 'Number of passed files: 2')
            self.assertEqual(lines[3], 'Number of manually repaired files: 0')
            self.assertEqual(lines[4], 'Number of failed files: 3')

            self.assertEqual(lines[5], 'Summary of Failures:')
            # Three error descriptions shared by all the files below.
            self.assertNotIn('.csv', lines[6])
            self.assertNotIn('.csv', lines[7])
            self.assertNotIn('.csv', lines[8])
            # Alphabetical order of files: the first one has capital F.
            self.assertEqual(lines[9], 'File5.csv')
            self.assertEqual(lines[10], 'file2.csv')
            self.assertEqual(lines[11], 'file3.csv')

    def test_email_summary_multiple_agencies(self):
        """Test email report generation where input has multiple agencies"""

        input_root = resolve_test_data_path('data/reports/agencies')
        email_report = report.EmailSummary(input_root, SANDBOX_DIR)

        emails = {
            'CAS-IAP': 'casiap@mail.com',
            'DWD-MOHp': 'dwd@mail.com',
            'MLCD-LU': 'mlcd@mail.com',
            'MSC': 'msc@mail.com'
        }
        email_report.write(emails)

        today = datetime.now().strftime('%Y-%m-%d')
        output_filename = f'failed-files-{today}'
        output_path = os.path.join(SANDBOX_DIR, output_filename)

        self.assertTrue(os.path.exists(output_path))

        with open(output_path, encoding='utf-8') as output:
            lines = output.read().splitlines()
            self.assertEqual(len(lines), 29)

            self.assertEqual(lines[0], 'CAS-IAP (casiap@mail.com)')
            self.assertEqual(lines[1], 'Total files received: 1')
            self.assertEqual(lines[2], 'Number of passed files: 1')
            self.assertEqual(lines[3], 'Number of manually repaired files: 0')
            self.assertEqual(lines[4], 'Number of failed files: 0')

            self.assertEqual(lines[6], 'DWD-MOHp (dwd@mail.com)')
            self.assertEqual(lines[7], 'Total files received: 3')
            self.assertEqual(lines[8], 'Number of passed files: 2')
            self.assertEqual(lines[9], 'Number of manually repaired files: 0')
            self.assertEqual(lines[10], 'Number of failed files: 1')

            self.assertEqual(lines[11], 'Summary of Failures:')
            self.assertNotIn('.csv', lines[12])
            self.assertEqual(lines[13], 'file2.csv')

            self.assertEqual(lines[15], 'MLCD-LU (mlcd@mail.com)')
            self.assertEqual(lines[16], 'Total files received: 3')
            self.assertEqual(lines[17], 'Number of passed files: 3')
            self.assertEqual(lines[18],
                             'Number of manually repaired files: 0')
            self.assertEqual(lines[19], 'Number of failed files: 0')

            self.assertEqual(lines[21], 'MSC (msc@mail.com)')
            self.assertEqual(lines[22], 'Total files received: 5')
            self.assertEqual(lines[23], 'Number of passed files: 4')
            self.assertEqual(lines[24],
                             'Number of manually repaired files: 0')
            self.assertEqual(lines[25], 'Number of failed files: 1')

            self.assertEqual(lines[26], 'Summary of Failures:')
            self.assertNotIn('.csv', lines[27])
            self.assertEqual(lines[28], 'file4.csv')

    def test_email_summary_multiple_runs(self):
        """Test email report generation across multiple operator reports"""

        input_root = resolve_test_data_path('data/reports/multiple_runs')
        email_report = report.EmailSummary(input_root, SANDBOX_DIR)

        emails = {
            'CAS-IAP': 'casiap@mail.com',
            'DWD-MOHp': 'dwd@mail.com',
            'MLCD-LU': 'mlcd@mail.com',
            'MSC': 'msc@mail.com'
        }
        email_report.write(emails)

        today = datetime.now().strftime('%Y-%m-%d')
        output_filename = f'failed-files-{today}'
        output_path = os.path.join(SANDBOX_DIR, output_filename)

        self.assertTrue(os.path.exists(output_path))

        with open(output_path, encoding='utf-8') as output:
            lines = output.read().splitlines()
            self.assertEqual(len(lines), 29)

            self.assertEqual(lines[0], 'CAS-IAP (casiap@mail.com)')
            self.assertEqual(lines[1], 'Total files received: 1')
            self.assertEqual(lines[2], 'Number of passed files: 1')
            self.assertEqual(lines[3], 'Number of manually repaired files: 0')
            self.assertEqual(lines[4], 'Number of failed files: 0')

            self.assertEqual(lines[6], 'DWD-MOHp (dwd@mail.com)')
            self.assertEqual(lines[7], 'Total files received: 3')
            self.assertEqual(lines[8], 'Number of passed files: 2')
            self.assertEqual(lines[9], 'Number of manually repaired files: 0')
            self.assertEqual(lines[10], 'Number of failed files: 1')

            self.assertEqual(lines[11], 'Summary of Failures:')
            self.assertNotIn('.csv', lines[12])
            self.assertEqual(lines[13], 'file2.csv')

            self.assertEqual(lines[15], 'MLCD-LU (mlcd@mail.com)')
            self.assertEqual(lines[16], 'Total files received: 3')
            self.assertEqual(lines[17], 'Number of passed files: 3')
            self.assertEqual(lines[18],
                             'Number of manually repaired files: 0')
            self.assertEqual(lines[19], 'Number of failed files: 0')

            self.assertEqual(lines[21], 'MSC (msc@mail.com)')
            self.assertEqual(lines[22], 'Total files received: 5')
            self.assertEqual(lines[23], 'Number of passed files: 4')
            self.assertEqual(lines[24],
                             'Number of manually repaired files: 0')
            self.assertEqual(lines[25], 'Number of failed files: 1')

            self.assertEqual(lines[26], 'Summary of Failures:')
            self.assertNotIn('.csv', lines[27])
            self.assertEqual(lines[28], 'file4.csv')

    def test_email_summary_single_fix(self):
        """
        Test email report generation for a single file that is fixed
        between two operator reports
        """

        input_root = resolve_test_data_path('data/reports/one_fix')
        email_report = report.EmailSummary(input_root, SANDBOX_DIR)

        emails = {'MSC': 'placeholder@mail.com'}
        email_report.write(emails)

        today = datetime.now().strftime('%Y-%m-%d')
        output_filename = f'failed-files-{today}'
        output_path = os.path.join(SANDBOX_DIR, output_filename)

        self.assertTrue(os.path.exists(output_path))

        with open(output_path, encoding='utf-8') as output:
            lines = output.read().splitlines()
            self.assertEqual(len(lines), 8)
            self.assertEqual(lines[0], 'MSC (placeholder@mail.com)')
            self.assertEqual(lines[1], 'Total files received: 1')
            self.assertEqual(lines[2], 'Number of passed files: 0')
            self.assertEqual(lines[3], 'Number of manually repaired files: 1')
            self.assertEqual(lines[4], 'Number of failed files: 0')

            self.assertEqual(lines[5], 'Summary of Fixes:')
            self.assertNotIn('.csv', lines[6])
            self.assertEqual(lines[7], 'file1.csv')

    def test_email_report_mixed_pass_fix(self):
        """
        Test email report generation when some files pass immediately
        and others are fixed between runs.
        """

        input_root = resolve_test_data_path('data/reports/pass_and_fix')
        email_report = report.EmailSummary(input_root, SANDBOX_DIR)

        emails = {'MSC': 'placeholder@mail.com'}
        email_report.write(emails)

        today = datetime.now().strftime('%Y-%m-%d')
        output_filename = f'failed-files-{today}'
        output_path = os.path.join(SANDBOX_DIR, output_filename)

        self.assertTrue(os.path.exists(output_path))

        with open(output_path, encoding='utf-8') as output:
            lines = output.read().splitlines()
            self.assertEqual(len(lines), 11)

            self.assertEqual(lines[0], 'MSC (placeholder@mail.com)')
            self.assertEqual(lines[1], 'Total files received: 9')
            self.assertEqual(lines[2], 'Number of passed files: 5')
            self.assertEqual(lines[3], 'Number of manually repaired files: 4')
            self.assertEqual(lines[4], 'Number of failed files: 0')

            self.assertEqual(lines[5], 'Summary of Fixes:')
            self.assertNotIn('.csv', lines[6])
            self.assertEqual(lines[7], 'File5.csv')
            self.assertEqual(lines[8], 'file2.csv')
            self.assertEqual(lines[9], 'file3.csv')
            self.assertEqual(lines[10], 'file9.csv')

    def test_email_report_mixed_fail_fix(self):
        """
        Test email report generation when some files fail irrecoverably
        and others are fixed between runs
        """

        input_root = resolve_test_data_path('data/reports/fix_and_fail')
        email_report = report.EmailSummary(input_root, SANDBOX_DIR)

        emails = {'MSC': 'placeholder@mail.com'}
        email_report.write(emails)

        today = datetime.now().strftime('%Y-%m-%d')
        output_filename = f'failed-files-{today}'
        output_path = os.path.join(SANDBOX_DIR, output_filename)

        self.assertTrue(os.path.exists(output_path))

        with open(output_path, encoding='utf-8') as output:
            lines = output.read().splitlines()
            self.assertEqual(len(lines), 17)

            self.assertEqual(lines[0], 'MSC (placeholder@mail.com)')
            self.assertEqual(lines[1], 'Total files received: 8')
            self.assertEqual(lines[2], 'Number of passed files: 0')
            self.assertEqual(lines[3], 'Number of manually repaired files: 3')
            self.assertEqual(lines[4], 'Number of failed files: 5')

            self.assertEqual(lines[5], 'Summary of Failures:')
            self.assertNotIn('.csv', lines[6])
            self.assertEqual(lines[7], 'file1.csv')
            self.assertEqual(lines[8], 'file3.csv')
            self.assertEqual(lines[9], 'file4.csv')
            self.assertEqual(lines[10], 'file7.csv')
            self.assertEqual(lines[11], 'file8.csv')

            self.assertEqual(lines[12], 'Summary of Fixes:')
            self.assertNotIn('.csv', lines[13])
            self.assertEqual(lines[14], 'file2.csv')
            self.assertEqual(lines[15], 'file5.csv')
            self.assertEqual(lines[16], 'file6.csv')

    def test_email_summary_fix_but_still_fail(self):
        """
        Test email report generation when files are fixed between runs,
        only to have an irrecoverable error show up.
        """

        input_root = resolve_test_data_path('data/reports/fail_twice')
        email_report = report.EmailSummary(input_root, SANDBOX_DIR)

        emails = {'MSC': 'placeholder@mail.com'}
        email_report.write(emails)

        today = datetime.now().strftime('%Y-%m-%d')
        output_filename = f'failed-files-{today}'
        output_path = os.path.join(SANDBOX_DIR, output_filename)

        self.assertTrue(os.path.exists(output_path))

        with open(output_path, encoding='utf-8') as output:
            lines = output.read().splitlines()
            self.assertEqual(len(lines), 8)

            self.assertEqual(lines[0], 'MSC (placeholder@mail.com)')
            self.assertEqual(lines[1], 'Total files received: 1')
            self.assertEqual(lines[2], 'Number of passed files: 0')
            self.assertEqual(lines[3], 'Number of manually repaired files: 0')
            self.assertEqual(lines[4], 'Number of failed files: 1')

            self.assertEqual(lines[5], 'Summary of Failures:')
            self.assertNotIn('.csv', lines[6])
            self.assertEqual(lines[7], 'file1.csv')

    def test_email_summary_mixed_pass_fix_fail(self):
        """
        Test email report generation when some files pass immediately,
        some fail irrecoverably, and others are fixed between runs.
        """

        input_root = resolve_test_data_path('data/reports/pass_fix_fail')
        email_report = report.EmailSummary(input_root, SANDBOX_DIR)

        emails = {'MSC': 'placeholder@mail.com'}
        email_report.write(emails)

        today = datetime.now().strftime('%Y-%m-%d')
        output_filename = f'failed-files-{today}'
        output_path = os.path.join(SANDBOX_DIR, output_filename)

        self.assertTrue(os.path.exists(output_path))

        with open(output_path, encoding='utf-8') as output:
            lines = output.read().splitlines()
            self.assertEqual(len(lines), 19)

            # Output may be sorted in various ways, so just check that all
            # files are in the right block and are all accounted for.
            fail_group = ['file4.csv', 'file9.csv']
            first_fix_of_pair = ['file2.csv', 'file6.csv']
            second_fix_of_pair = ['file3.csv', 'file8.csv']

            self.assertEqual(lines[0], 'MSC (placeholder@mail.com)')
            self.assertEqual(lines[1], 'Total files received: 11')
            self.assertEqual(lines[2], 'Number of passed files: 5')
            self.assertEqual(lines[3], 'Number of manually repaired files: 4')
            self.assertEqual(lines[4], 'Number of failed files: 2')

            self.assertEqual(lines[5], 'Summary of Failures:')
            self.assertNotIn('.csv', lines[6])
            self.assertIn(lines[7], fail_group)
            self.assertNotIn('.csv', lines[8])
            self.assertIn(lines[9], fail_group)

            self.assertEqual(lines[10], 'Summary of Fixes:')
            self.assertNotIn('.csv', lines[11])
            self.assertNotIn('.csv', lines[12])
            self.assertIn(lines[13], first_fix_of_pair)
            self.assertIn(lines[14], second_fix_of_pair)
            self.assertNotIn('.csv', lines[15])
            self.assertNotIn('.csv', lines[16])
            self.assertIn(lines[17], first_fix_of_pair)
            self.assertIn(lines[18], second_fix_of_pair)

    def test_email_summary_multiple_causes(self):
        """
        Test email report generation when files fail or are fixed due to
        multiple different issues.
        """

        input_root = resolve_test_data_path(
            'data/reports/multiple_causes_two_runs')
        email_report = report.EmailSummary(input_root, SANDBOX_DIR)

        emails = {'MSC': 'placeholder@mail.com'}
        email_report.write(emails)

        today = datetime.now().strftime('%Y-%m-%d')
        output_filename = f'failed-files-{today}'
        output_path = os.path.join(SANDBOX_DIR, output_filename)

        self.assertTrue(os.path.exists(output_path))

        with open(output_path, encoding='utf-8') as output:
            lines = output.read().splitlines()
            self.assertEqual(len(lines), 17)

            self.assertEqual(lines[0], 'MSC (placeholder@mail.com)')
            self.assertEqual(lines[1], 'Total files received: 5')
            self.assertEqual(lines[2], 'Number of passed files: 0')
            self.assertEqual(lines[3], 'Number of manually repaired files: 2')
            self.assertEqual(lines[4], 'Number of failed files: 3')

            fix_group = ['file1.csv', 'file3.csv']
            fail_group = ['file2.csv', 'file4.csv', 'file5.csv']

            self.assertEqual(lines[5], 'Summary of Failures:')
            self.assertNotIn('.csv', lines[6])
            self.assertIn(lines[7], fail_group)
            self.assertNotIn('.csv', lines[8])
            self.assertIn(lines[9], fail_group)
            self.assertNotIn('.csv', lines[10])
            self.assertIn(lines[11], fail_group)
            self.assertEqual(lines[12], 'Summary of Fixes:')
            self.assertNotIn('.csv', lines[13])
            self.assertIn(lines[14], fix_group)
            self.assertNotIn('.csv', lines[15])
            self.assertIn(lines[16], fix_group)

            # Check that all error causes (messages) are distinct.
            self.assertEqual(len(set([lines[6], lines[8], lines[10]])), 3)
            self.assertEqual(len(set([lines[13], lines[15]])), 2)


if __name__ == '__main__':
    unittest.main()
