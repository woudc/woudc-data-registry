
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
        all_warnings = resolve_test_data_path('config/all_warnings.csv')
        all_errors = resolve_test_data_path('config/all_errors.csv')

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

        infile_root = resolve_test_data_path('data/general/pass_and_fail')

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

        infile_root = resolve_test_data_path('data/general/pass_and_fail')

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

        infile_root = resolve_test_data_path('data/general/agencies')

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

    def test_find_operator_report_empty(self):
        """Test that no operator reports are found when none exist"""

        project_root = resolve_test_data_path('data/reports')
        email_report = report.EmailSummary(project_root)

        operator_reports = email_report.find_operator_reports()

        self.assertEquals([], operator_reports)

    def test_find_operator_report_one_run(self):
        """Test that operator reports are found when one exists"""

        project_root = resolve_test_data_path('data/reports/one_pass')
        email_report = report.EmailSummary(project_root)

        operator_reports = email_report.find_operator_reports()
        expected_parent = resolve_test_data_path('data/reports/one_pass/run1')

        self.assertEquals(1, len(operator_reports))
        self.assertIn(expected_parent, operator_reports[0])

    def test_find_operator_report_many_runs(self):
        """
        Test that all operator reports are found when they are spread
        across multiple run directories
        """

        project_root = resolve_test_data_path('data/reports/six_reports')
        email_report = report.EmailSummary(project_root)

        operator_reports = email_report.find_operator_reports()
        expected_path_pattern = \
            'data/reports/six_reports/run{}/operator-report-9999-12-31.csv'

        self.assertEquals(6, len(operator_reports))

        for run_number in range(1, 6 + 1):
            expected_path = resolve_test_data_path(
                expected_path_pattern.format(run_number))
            self.assertIn(expected_path, set(operator_reports))

    def test_email_summary_single_pass(self):

        """Test email report generation for a single passing file"""

        input_root = resolve_test_data_path('data/reports/one_pass')
        email_report = report.EmailSummary(input_root, SANDBOX_DIR)

        emails = {'MSC': 'placeholder@site.com'}
        email_report.write(emails)

        today = datetime.now().strftime('%Y-%m-%d')
        output_filename = 'failed-files-{}'.format(today)
        output_path = os.path.join(SANDBOX_DIR, output_filename)

        self.assertTrue(os.path.exists(output_path))

        with open(output_path) as output:
            lines = output.read().splitlines()
            self.assertEquals(len(lines), 5)

            self.assertEquals(lines[0], 'MSC (placeholder@site.com)')
            self.assertEquals(lines[1], 'Total files received: 1')
            self.assertEquals(lines[2], 'Number of passed files: 1')
            self.assertEquals(lines[3], 'Number of manually repaired files: 0')
            self.assertEquals(lines[4], 'Number of failed files: 0')

    def test_email_summary_single_fail(self):
        """Test email report generation for a single failing file"""

        input_root = resolve_test_data_path('data/reports/one_fail')
        email_report = report.EmailSummary(input_root, SANDBOX_DIR)

        emails = {'MSC': 'placeholder@site.com'}
        email_report.write(emails)

        today = datetime.now().strftime('%Y-%m-%d')
        output_filename = 'failed-files-{}'.format(today)
        output_path = os.path.join(SANDBOX_DIR, output_filename)

        self.assertTrue(os.path.exists(output_path))

        with open(output_path) as output:
            lines = output.read().splitlines()
            self.assertEquals(len(lines), 8)

            self.assertEquals(lines[0], 'MSC (placeholder@site.com)')
            self.assertEquals(lines[1], 'Total files received: 1')
            self.assertEquals(lines[2], 'Number of passed files: 0')
            self.assertEquals(lines[3], 'Number of manually repaired files: 0')
            self.assertEquals(lines[4], 'Number of failed files: 1')

            self.assertEquals(lines[5], 'Summary of Failures:')
            self.assertNotIn('.csv', lines[6])
            self.assertEquals(lines[7], 'file1.csv')

    def test_email_report_one_run_mixed(self):
        """
        Test email report generation with passing and failing files
        all in one operator report
        """

        input_root = resolve_test_data_path('data/reports//pass_and_fail')
        email_report = report.EmailSummary(input_root, SANDBOX_DIR)

        emails = {'MSC': 'placeholder@site.com'}
        email_report.write(emails)

        today = datetime.now().strftime('%Y-%m-%d')
        output_filename = 'failed-files-{}'.format(today)
        output_path = os.path.join(SANDBOX_DIR, output_filename)

        self.assertTrue(os.path.exists(output_path))

        with open(output_path) as output:
            lines = output.read().splitlines()
            self.assertEquals(len(lines), 10)

            self.assertEquals(lines[0], 'MSC (placeholder@site.com)')
            self.assertEquals(lines[1], 'Total files received: 5')
            self.assertEquals(lines[2], 'Number of passed files: 2')
            self.assertEquals(lines[3], 'Number of manually repaired files: 0')
            self.assertEquals(lines[4], 'Number of failed files: 3')

            self.assertEquals(lines[5], 'Summary of Failures:')
            self.assertNotIn('.csv', lines[6])
            # Alphabetical order of files: the first one has capital F.
            self.assertEquals(lines[7], 'File5.csv')
            self.assertEquals(lines[8], 'file2.csv')
            self.assertEquals(lines[9], 'file3.csv')

    def test_email_report_multiple_causes_one_group(self):
        """
        Test email report generation where a single group of files
        experiences multiple error types.
        """

        input_root = resolve_test_data_path('data/reports/multiple_causes')
        email_report = report.EmailSummary(input_root, SANDBOX_DIR)

        emails = {'MSC': 'placeholder@site.com'}
        email_report.write(emails)

        today = datetime.now().strftime('%Y-%m-%d')
        output_filename = 'failed-files-{}'.format(today)
        output_path = os.path.join(SANDBOX_DIR, output_filename)

        self.assertTrue(os.path.exists(output_path))

        with open(output_path) as output:
            lines = output.read().splitlines()
            self.assertEquals(len(lines), 12)

            self.assertEquals(lines[0], 'MSC (placeholder@site.com)')
            self.assertEquals(lines[1], 'Total files received: 5')
            self.assertEquals(lines[2], 'Number of passed files: 2')
            self.assertEquals(lines[3], 'Number of manually repaired files: 0')
            self.assertEquals(lines[4], 'Number of failed files: 3')

            self.assertEquals(lines[5], 'Summary of Failures:')
            # Three error descriptions shared by all the files below.
            self.assertNotIn('.csv', lines[6])
            self.assertNotIn('.csv', lines[7])
            self.assertNotIn('.csv', lines[8])
            # Alphabetical order of files: the first one has capital F.
            self.assertEquals(lines[9], 'File5.csv')
            self.assertEquals(lines[10], 'file2.csv')
            self.assertEquals(lines[11], 'file3.csv')

    def test_email_report_multiple_agencies(self):
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
        output_filename = 'failed-files-{}'.format(today)
        output_path = os.path.join(SANDBOX_DIR, output_filename)

        self.assertTrue(os.path.exists(output_path))

        with open(output_path) as output:
            lines = output.read().splitlines()
            self.assertEquals(len(lines), 29)

            self.assertEquals(lines[0], 'CAS-IAP (casiap@mail.com)')
            self.assertEquals(lines[1], 'Total files received: 1')
            self.assertEquals(lines[2], 'Number of passed files: 1')
            self.assertEquals(lines[3], 'Number of manually repaired files: 0')
            self.assertEquals(lines[4], 'Number of failed files: 0')

            self.assertEquals(lines[6], 'DWD-MOHp (dwd@mail.com)')
            self.assertEquals(lines[7], 'Total files received: 3')
            self.assertEquals(lines[8], 'Number of passed files: 2')
            self.assertEquals(lines[9], 'Number of manually repaired files: 0')
            self.assertEquals(lines[10], 'Number of failed files: 1')

            self.assertEquals(lines[11], 'Summary of Failures:')
            self.assertNotIn('.csv', lines[12])
            self.assertEquals(lines[13], 'file2.csv')

            self.assertEquals(lines[15], 'MLCD-LU (mlcd@mail.com)')
            self.assertEquals(lines[16], 'Total files received: 3')
            self.assertEquals(lines[17], 'Number of passed files: 3')
            self.assertEquals(lines[18],
                              'Number of manually repaired files: 0')
            self.assertEquals(lines[19], 'Number of failed files: 0')

            self.assertEquals(lines[21], 'MSC (msc@mail.com)')
            self.assertEquals(lines[22], 'Total files received: 5')
            self.assertEquals(lines[23], 'Number of passed files: 4')
            self.assertEquals(lines[24],
                              'Number of manually repaired files: 0')
            self.assertEquals(lines[25], 'Number of failed files: 1')

            self.assertEquals(lines[26], 'Summary of Failures:')
            self.assertNotIn('.csv', lines[27])
            self.assertEquals(lines[28], 'file4.csv')


if __name__ == '__main__':
    unittest.main()
