
import csv
import pathlib
import os
import unittest

from datetime import datetime

from woudc_data_registry import models, parser, report, util


SANDBOX_DIR = '/tmp/woudc-data-registry'


def dummy_extCSV(source):
    """
    Returns a parser.ExtendedCSV instace built from the filepath <source>
    with dummy output settings (no logs or reports).
    """

    with report.OperatorReport() as error_bank:
        return parser.ExtendedCSV(source, error_bank)


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

    def setUpClass():
        os.mkdir(SANDBOX_DIR)

    def tearDownClass():
        os.rmdir(SANDBOX_DIR)

    def tearDown(self):
        clear_sandbox()


class OperatorReportTest(SandboxTestSuite):
    """Test suite for OperatorReport, error severity, and file format"""

    def test_operator_report_output_location(self):
        """Test that operator reports write a file in the working directory"""

        with report.OperatorReport(SANDBOX_DIR) as op_report:
            operator_path = pathlib.Path(op_report.filepath())
            self.assertEquals(str(operator_path.parent), SANDBOX_DIR)

    def test_uses_error_definition(self):
        """Test that error/warning feedback responds to input files"""

        # The two error files below have different error types for error 1.
        all_warnings = resolve_test_data_path('data/reports/all_warnings.csv')
        all_errors = resolve_test_data_path('data/reports/all_errors.csv')

        with report.OperatorReport(SANDBOX_DIR) as op_report:
            op_report.read_error_definitions(all_warnings)

            self.assertIn(1, op_report._error_definitions)
            _, success = op_report.add_message(1)
            self.assertFalse(success)

            op_report.read_error_definitions(all_errors)

            self.assertIn(1, op_report._error_definitions)
            _, success = op_report.add_message(1)
            self.assertTrue(success)

    def test_passing_operator_report(self):
        """Test that a passing file is written in the operator report"""

        filename = '20080101.Kipp_Zonen.UV-S-E-T.000560.PMOD-WRC.csv'
        infile = resolve_test_data_path('data/general/{}'.format(filename))
        contents = util.read_file(infile)

        with report.OperatorReport(SANDBOX_DIR) as op_report:
            ecsv = parser.ExtendedCSV(contents, op_report)

            ecsv.validate_metadata_tables()
            ecsv.validate_dataset_tables()
            data_record = models.DataRecord(ecsv)
            data_record.filename = filename

            agency = ecsv.extcsv['DATA_GENERATION']['Agency']

            today = datetime.now().strftime('%Y-%m-%d')
            output_path = os.path.join(SANDBOX_DIR,
                                       'operator-report-{}.csv'.format(today))

            op_report.add_message(200)  # File passes validation
            op_report.write_passing_file(infile, ecsv, data_record)

        self.assertTrue(os.path.exists(output_path))
        with open(output_path) as output:
            reader = csv.reader(output)
            next(reader)

            report_line = next(reader)
            self.assertEquals(report_line[0], 'P')
            self.assertEquals(report_line[2], '200')
            self.assertIn(agency, report_line)
            self.assertIn(os.path.basename(infile), report_line)

            with self.assertRaises(StopIteration):
                next(reader)

    def test_warning_operator_report(self):
        """Test that file warnings are written in the operator report"""

        filename = 'ecsv-trailing-commas.csv'
        infile = resolve_test_data_path('data/general/{}'.format(filename))
        contents = util.read_file(infile)

        with report.OperatorReport(SANDBOX_DIR) as op_report:
            ecsv = parser.ExtendedCSV(contents, op_report)

            # Some warnings are encountered during parsing.
            ecsv.validate_metadata_tables()
            ecsv.validate_dataset_tables()
            data_record = models.DataRecord(ecsv)
            data_record.filename = filename

            agency = ecsv.extcsv['DATA_GENERATION']['Agency']

            today = datetime.now().strftime('%Y-%m-%d')
            output_path = os.path.join(SANDBOX_DIR,
                                       'operator-report-{}.csv'.format(today))

            op_report.add_message(200)  # File passes validation
            op_report.write_passing_file(infile, ecsv, data_record)

        self.assertTrue(os.path.exists(output_path))
        with open(output_path) as output:
            reader = csv.reader(output)
            next(reader)

            expected_warnings = len(ecsv.warnings)
            for _ in range(expected_warnings):
                report_line = next(reader)

                self.assertEquals(report_line[0], 'P')
                self.assertEquals(report_line[1], 'Warning')
                self.assertIn(agency, report_line)
                self.assertIn(os.path.basename(infile), report_line)

            report_line = next(reader)
            self.assertEquals(report_line[0], 'P')
            self.assertEquals(report_line[1], 'Warning')
            self.assertEquals(report_line[2], '200')
            self.assertIn(agency, report_line)
            self.assertIn(os.path.basename(infile), report_line)

            with self.assertRaises(StopIteration):
                next(reader)

    def test_failing_operator_report(self):
        """Test that a failing file is written in the operator report"""

        filename = 'ecsv-missing-instrument-name.csv'
        infile = resolve_test_data_path('data/general/{}'.format(filename))
        contents = util.read_file(infile)

        ecsv = None
        agency = 'UNKNOWN'

        with report.OperatorReport(SANDBOX_DIR) as op_report:
            try:
                ecsv = parser.ExtendedCSV(contents, op_report)
                ecsv.validate_metadata_tables()
                agency = ecsv.extcsv['DATA_GENERATION']['Agency']

                ecsv.validate_dataset_tables()
                raise AssertionError('Parsing of {} did not fail'
                                     .format(infile))
            except (parser.MetadataValidationError,
                    parser.NonStandardDataError):
                output_path = os.path.join(SANDBOX_DIR, 'run1')

                op_report.add_message(209)
                op_report.write_failing_file(infile, agency, ecsv)

        today = datetime.now().strftime('%Y-%m-%d')
        output_path = os.path.join(SANDBOX_DIR,
                                   'operator-report-{}.csv'.format(today))

        self.assertTrue(os.path.exists(output_path))
        with open(output_path) as output:
            reader = csv.reader(output)
            next(reader)

            warnings = 0
            errors = 0

            expected_warnings = len(ecsv.warnings)
            expected_errors = len(ecsv.errors)
            for _ in range(expected_warnings + expected_errors):
                report_line = next(reader)
                self.assertEquals(report_line[0], 'F')

                if report_line[1] == 'Warning':
                    warnings += 1
                elif report_line[1] == 'Error':
                    errors += 1

            self.assertEquals(warnings, expected_warnings)
            self.assertEquals(errors, expected_errors)

            report_line = next(reader)
            self.assertEquals(report_line[0], 'F')
            self.assertEquals(report_line[1], 'Error')
            self.assertEquals(report_line[2], '209')
            self.assertIn(agency, report_line)
            self.assertIn(os.path.basename(infile), report_line)

            with self.assertRaises(StopIteration):
                next(reader)

    def test_mixed_operator_report(self):
        """
        Test that passing and failing files are written to the operator report
        when a mixture of the two is processed
        """

        infile_root = resolve_test_data_path('data/reports/pass_and_fail')

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
                    ecsv = parser.ExtendedCSV(contents, op_report)
                except (parser.MetadataValidationError,
                        parser.NonStandardDataError) as err:
                    expected_errors[fullpath] = len(err.errors)

                    op_report.add_message(209)
                    op_report.write_failing_file(fullpath, agency)
                    continue

                try:
                    ecsv.validate_metadata_tables()
                    agency = ecsv.extcsv['DATA_GENERATION']['Agency']

                    ecsv.validate_dataset_tables()
                    data_record = models.DataRecord(ecsv)
                    data_record.filename = infile

                    expected_warnings[fullpath] = len(ecsv.warnings)
                    expected_errors[fullpath] = 0
                    op_report.write_passing_file(fullpath, ecsv, data_record)
                except (parser.MetadataValidationError,
                        parser.NonStandardDataError):
                    expected_warnings[fullpath] = len(ecsv.warnings)
                    expected_errors[fullpath] = len(ecsv.errors)

                    op_report.add_message(209)
                    op_report.write_failing_file(fullpath, agency, ecsv)

        today = datetime.now().strftime('%Y-%m-%d')
        output_path = os.path.join(SANDBOX_DIR,
                                   'operator-report-{}.csv'.format(today))

        self.assertTrue(os.path.exists(output_path))
        with open(output_path) as output:
            reader = csv.reader(output)
            next(reader)

            for line in reader:
                if expected_errors[line[12]] == 0:
                    self.assertEquals(line[0], 'P')
                    self.assertEquals(line[1], 'Warning')
                else:
                    self.assertEquals(line[0], 'F')

                if line[2] == '200':
                    self.assertEquals(expected_errors[line[12]], 0)
                elif line[2] == '209':
                    self.assertGreater(expected_errors[line[12]], 0)
                elif line[1] == 'Warning':
                    warnings[line[12]] += 1
                elif line[1] == 'Error':
                    errors[line[12]] += 1

        self.assertEquals(warnings, expected_warnings)
        self.assertEquals(errors, expected_errors)


class RunReportTest(SandboxTestSuite):
    """Test suite for RunReport, file writing and file format"""

    def test_run_report_output_location(self):
        """Test that run reports write a file in the working directory"""

        run_report = report.RunReport(SANDBOX_DIR)

        run_report_path = pathlib.Path(run_report.filepath())
        self.assertEquals(str(run_report_path.parent), SANDBOX_DIR)

    def test_passing_run_report(self):
        """Test that a passing file is written to the run report"""

        filename = '20080101.Kipp_Zonen.UV-S-E-T.000560.PMOD-WRC.csv'
        infile = resolve_test_data_path('data/general/{}'.format(filename))
        contents = util.read_file(infile)

        run_report = report.RunReport(SANDBOX_DIR)
        with report.OperatorReport() as error_bank:
            ecsv = parser.ExtendedCSV(contents, error_bank)

            ecsv.validate_metadata_tables()
            ecsv.validate_dataset_tables()
            data_record = models.DataRecord(ecsv)
            data_record.filename = filename

            agency = ecsv.extcsv['DATA_GENERATION']['Agency']
            output_path = os.path.join(SANDBOX_DIR, 'run_report')

            run_report.write_passing_file(infile, agency)

        self.assertTrue(os.path.exists(output_path))
        with open(output_path) as output:
            lines = output.read().splitlines()
            self.assertEquals(len(lines), 2)

            self.assertEquals(lines[0], agency)
            self.assertEquals(lines[1], 'Pass: {}'.format(infile))

    def test_failing_run_report(self):
        """Test that a failing file is written to the run report"""

        filename = 'ecsv-missing-instrument-name.csv'
        infile = resolve_test_data_path('data/general/{}'.format(filename))
        contents = util.read_file(infile)

        ecsv = None
        # Agency typically filled in with FTP username for failing files.
        agency = 'rmda'

        with report.OperatorReport() as error_bank:
            run_report = report.RunReport(SANDBOX_DIR)

            try:
                ecsv = parser.ExtendedCSV(contents, error_bank)
                ecsv.validate_metadata_tables()
                agency = ecsv.extcsv['DATA_GENERATION']['Agency']

                ecsv.validate_dataset_tables()
                raise AssertionError('Parsing of {} did not fail'
                                     .format(infile))
            except (parser.MetadataValidationError,
                    parser.NonStandardDataError):
                output_path = os.path.join(SANDBOX_DIR, 'run_report')

                run_report.write_failing_file(infile, agency)

        self.assertTrue(os.path.exists(output_path))
        with open(output_path) as output:
            lines = output.read().splitlines()
            self.assertEquals(len(lines), 2)

            self.assertEquals(lines[0], agency)
            self.assertEquals(lines[1], 'Fail: {}'.format(infile))

    def test_non_extcsv_run_report(self):
        """Test that an unparseable file is written to the run report"""

        filename = 'not-an-ecsv.dat'
        infile = resolve_test_data_path('data/general/{}'.format(filename))
        contents = util.read_file(infile)

        agency = 'UNKNOWN'

        with report.OperatorReport() as error_bank:
            run_report = report.RunReport(SANDBOX_DIR)

            try:
                _ = parser.ExtendedCSV(contents, error_bank)
                raise AssertionError('Parsing of {} did not fail'
                                     .format(infile))
            except (parser.MetadataValidationError,
                    parser.NonStandardDataError):
                output_path = os.path.join(SANDBOX_DIR, 'run_report')

                run_report.write_failing_file(infile, agency)

                self.assertTrue(os.path.exists(output_path))
                with open(output_path) as output:
                    lines = output.read().splitlines()
                    self.assertEquals(len(lines), 2)

                    self.assertEquals(lines[0], agency)
                    self.assertEquals(lines[1], 'Fail: {}'.format(infile))

    def test_mixed_run_report(self):
        """
        Test that passing and failing files are written to the run report
        when a mixture of the two is processed
        """

        infile_root = resolve_test_data_path('data/reports/pass_and_fail')

        agency = 'MSC'

        expected_passes = set()
        expected_fails = set()

        with report.OperatorReport() as error_bank:
            run_report = report.RunReport(SANDBOX_DIR)

            for infile in os.listdir(infile_root):
                fullpath = os.path.join(infile_root, infile)

                try:
                    contents = util.read_file(fullpath)
                    ecsv = parser.ExtendedCSV(contents, error_bank)
                except (parser.MetadataValidationError,
                        parser.NonStandardDataError):
                    expected_fails.add(fullpath)
                    run_report.write_failing_file(fullpath, agency)
                    continue

                try:
                    ecsv.validate_metadata_tables()
                    ecsv.validate_dataset_tables()
                    data_record = models.DataRecord(ecsv)
                    data_record.filename = infile

                    expected_passes.add(fullpath)
                    run_report.write_passing_file(fullpath, agency)
                except (parser.MetadataValidationError,
                        parser.NonStandardDataError):
                    expected_fails.add(fullpath)
                    run_report.write_failing_file(fullpath, agency)

        self.assertEquals(len(expected_passes), 6)
        self.assertEquals(len(expected_fails), 4)

        output_path = os.path.join(SANDBOX_DIR, 'run_report')
        self.assertTrue(os.path.exists(output_path))

        with open(output_path) as output:
            lines = output.read().splitlines()
            self.assertEquals(lines[0], agency)
            self.assertEquals(len(lines),
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

        infile_root = resolve_test_data_path('data/reports/agencies')

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

            for dirpath, dirnames, filenames in os.walk(infile_root):
                for infile in filenames:
                    fullpath = os.path.join(dirpath, infile)
                    # Agency inferred from directory name.
                    agency = dirpath.split('/')[-1]

                    try:
                        contents = util.read_file(fullpath)
                        ecsv = parser.ExtendedCSV(contents, error_bank)
                    except (parser.MetadataValidationError,
                            parser.NonStandardDataError):
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
                        data_record.filename = infile

                        expected_passes[agency].add(fullpath)
                        run_report.write_passing_file(fullpath, agency)
                    except (parser.MetadataValidationError,
                            parser.NonStandardDataError):
                        agency = agency_aliases[agency]
                        if agency not in expected_passes:
                            expected_passes[agency] = set()
                        if agency not in expected_fails:
                            expected_fails[agency] = set()

                        expected_fails[agency].add(fullpath)
                        run_report.write_failing_file(fullpath, agency)

        self.assertEquals(len(expected_passes['CAS-IAP']), 1)
        self.assertEquals(len(expected_passes['DWD-MOHp']), 2)
        self.assertEquals(len(expected_passes['MLCD-LU']), 3)
        self.assertEquals(len(expected_passes['MSC']), 4)

        self.assertEquals(len(expected_fails['CAS-IAP']), 0)
        self.assertEquals(len(expected_fails['DWD-MOHp']), 1)
        self.assertEquals(len(expected_fails['MLCD-LU']), 0)
        self.assertEquals(len(expected_fails['MSC']), 1)

        output_path = os.path.join(SANDBOX_DIR, 'run_report')
        self.assertTrue(os.path.exists(output_path))

        with open(output_path) as output:
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
        self.assertEquals(str(email_report_path.parent), SANDBOX_DIR)


if __name__ == '__main__':
    unittest.main()
