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

import os
import csv
import logging

from datetime import date
from collections import OrderedDict

from woudc_data_registry import config


LOGGER = logging.getLogger(__name__)


class Report:
    """
    Superclass for WOUDC Data Registry reports that are generated during
    a processing run, one file at a time.

    The expected behaviour for these types of reports is to accept a
    passing or failing file once it is done processing, add it onto the
    existing report file (if one exists) and write it all out to disk
    immediately, so the user can look at the reports before the processing
    command is complete.

    This is meant to be an abstract superclass and should not be instantiated
    on its own.
    """

    def __init__(self, root):
        """
        Initialize a new Report that writes to the directory <root>.

        :param root: Path to the processing run's working directory.
        """

        self._working_directory = root

    def _load_processing_results_pass(self, filepath, *args):
        """
        Picks out important metadata about the file given under <filepath>
        after it passed processing, using the additional arguments list
        to help construct messages for the report.

        Subclasses must override this method to define what arguments they
        need and what information to store for report messages.

        :param filepath: Path to an incoming data file.
        :param args: Additional information from or about the file.
        :returns: void
        """

        raise NotImplementedError()

    def _load_processing_results_fail(self, filepath, *args):
        """
        Picks out important metadata about the file given under <filepath>
        after it failed processing, using the additional arguments list
        to help construct messages and error logs for the report.

        Subclasses must override this method to define what arguments they
        need and what information to store for report messages.

        :param filepath: Path to an incoming data file.
        :param args: Additional information from or about the file.
        :returns: void
        """

        raise NotImplementedError()

    def filepath(self):
        """
        Returns a full path to the report that will be placed in this
        report object's working directory.

        :returns: Full path to this report file.
        """

        raise NotImplementedError()

    def write_passing_file(self, filepath, *args):
        """
        Record in this processing report that the file given under <filepath>
        has been processed successfully.

        Additional information about the file, passed through by additional
        args, will be used to fill out the report entry. See subclass'
        load_processing_results method for more information.

        :param filepath: Path to an incoming data file.
        :param args: Additional information from or about the file.
        :returns: void
        """

        if self._working_directory is not None:
            self._load_processing_results_pass(filepath, *args)
            self.write()

    def write_failing_file(self, filepath, *args):
        """
        Record in this processing report that the file given under <filepath>
        failed to process, at least for the current processing command.

        Additional information about the file, passed through by additional
        args, will be used to fill out the report entry. See subclass'
        load_processing_results method for more information.

        :param filepath: Path to an incoming data file.
        :param args: Additional information from or about the file.
        :returns: void
        """

        if self._working_directory is not None:
            self._load_processing_results_fail(filepath, *args)
            self.write()

    def write(self):
        """
        Write a new report into the working directory (or update an existing
        report if one exists) with internally stored file metadata.

        Users should generally not call these methods, and instead use the
        write_passing_file and write_failing_file methods to load file
        metadata and write it in one call. However, subclasses must define
        this write method to specify their file formats.

        :returns: void
        """

        raise NotImplementedError()


class OperatorReport(Report):
    """
    A WOUDC Data Registry operator report is like a simplified log used
    during a processing run to identify errors in files. It describes
    all files and all the warnings and errors that are encountered
    while handling them, but is not a true log file and so stays concise.

    An operator report is a CSV file with one row of headers one line
    for each warning or error in a file, in order of appearance and in
    the order the files were processed.

    The `OperatorReport` class is responsible for writing operator reports.
    """

    def __init__(self, root=None):
        """
        Initialize a new OperatorReport that writes to the directory <root>.

        For use in dummy or verification-only runs, passing <root> as None
        causes no output files to be produced.

        :param root: Path to the processing run's working directory.
        """

        super(OperatorReport, self).__init__(root)

        self._report_batch = OrderedDict([
            ('Processing Status', None),
            ('Error Type', []),
            ('Error Code', []),
            ('Line Number', []),
            ('Message', []),
            ('Dataset', None),
            ('Data Level', None),
            ('Data Form', None),
            ('Agency', None),
            ('Station Type', None),
            ('Station ID', None),
            ('Filename', None),
            ('Incoming Path', None),
            ('Outgoing Path', None),
            ('URN', None)
        ])

        self.operator_report = None

        self._error_definitions = {}
        self.read_error_definitions(config.WDR_ERROR_CONFIG)

    def __enter__(self):
        """
        Open and set up the file where this operator report will be written.
        """

        if self._working_directory is not None:
            filepath = self.filepath()
            self.operator_report = open(filepath, 'w')

            header = ','.join(self._report_batch.keys())
            self.operator_report.write(header + '\n')

        return self

    def __exit__(self, *args):
        """
        Close the file where the operator report has been written.
        """

        if self.operator_report is not None:
            self.operator_report.close()

    def _load_processing_results_common(self, filepath, contributor, extcsv):
        """
        Helper used to extract values values about the file located at
        <filepath>, mainly from its Extended CSV object (if it exists).

        Extended CSV fields that are present will be copied into their places
        in operator report lines. Those that are not present will be replaced
        with empty strings in the report. Some filepath information is taken
        from <filepath> itself.

        :param filepath: Path to an incoming data file.
        :param contributor: Acronym of the contributor that submitted the file.
        :param extcsv: Extended CSV object from parsing the file, if available.
        :returns: void
        """

        extcsv_to_batch_fields = [
            ('Station Type', 'PLATFORM', 'Type'),
            ('Station ID', 'PLATFORM', 'ID'),
            ('Dataset', 'CONTENT', 'Category'),
            ('Data Level', 'CONTENT', 'Level'),
            ('Data Form', 'CONTENT', 'Form'),
            ('Agency', 'DATA_GENERATION', 'Agency')
        ]

        # Tranfer core file metadata from the Extended CSV to the report batch.
        for batch_field, table_name, extcsv_field in extcsv_to_batch_fields:
            try:
                self._report_batch[batch_field] = \
                    str(extcsv.extcsv[table_name][extcsv_field])
            except (TypeError, KeyError, AttributeError):
                # Some parsing or processing error occurred and the
                # ExtCSV value is unavailable.
                self._report_batch[batch_field] = None

        self._report_batch['Agency'] = contributor
        self._report_batch['Incoming Path'] = filepath
        self._report_batch['Filename'] = os.path.basename(filepath)

    def _load_processing_results_pass(self, filepath, extcsv, data_record):
        """
        Picks out and stores features of the file under <filepath> from
        the Extended CSV and data records it generated after being processed.

        :param filepath: Path to an incoming data file.
        :param extcsv: Extended CSV object generated from parsing the file.
        :param data_record: DataRecord object generated by processing the file.
        :returns: void
        """

        self._report_batch['Processing Status'] = 'P'

        contributor = extcsv.extcsv['DATA_GENERATION']['Agency']
        self._load_processing_results_common(filepath, contributor, extcsv)

        self._report_batch['Outgoing Path'] = \
            data_record.get_waf_path(config.WDR_WAF_BASEDIR)
        self._report_batch['URN'] = data_record.data_record_id

    def _load_processing_results_fail(self, filepath, contributor,
                                      extcsv=None):
        """
        Salvages any information about the file under <filepath> after it
        failed processing, and record it in preparation for error reporting.

        If the file was able to through parse successfully then <extcsv> should
        be the Extended CSV object generated by parsing. Passing the value
        None for that parameter means the file failed during parsing.

        Since the file has failed processing, its metadata values cannot be
        trusted and the <contributor> parameter is used as a best guess of
        who submitted the file, perhaps based on its FTP acount name.

        :param filepath: Path to an incoming data file.
        :param contributor: Acronym of the contributor that submitted the file.
        :param extcsv: Extended CSV object from parsing the file, if available.
        :returns: void
        """

        self._report_batch['Processing Status'] = 'F'

        self._load_processing_results_common(filepath, contributor, extcsv)

        self._report_batch['Outgoing Path'] = None
        self._report_batch['URN'] = None

    def filepath(self):
        """
        Returns a full path to the operator report that will be placed
        in this report's working directory.

        :returns: Full path to this operator report file.
        """

        if self._working_directory is None:
            return None
        else:
            today = date.today().strftime('%Y-%m-%d')
            filename = 'operator-report-{}.csv'.format(today)

            return os.path.join(self._working_directory, filename)

    def read_error_definitions(self, filepath):
        """
        Loads the error definitions found in <filepath> to apply those
        rules to future error/warning determination.

        :param filepath: Path to an error definition file.
        :returns: void
        """

        with open(filepath) as error_definitions:
            reader = csv.reader(error_definitions, escapechar='\\')
            next(reader)  # Skip header line.

            for row in reader:
                error_code = int(row[0])
                self._error_definitions[error_code] = row[1:3]

    def add_message(self, error_code, line=None, **kwargs):
        """
        Logs that an error of type <error_code> was found at line <line>
        in an input file.

        Returns two elements: the first is the warning/error message string,
        and the second is False iff <error_code> matches an error severe
        enough to cause a processing run to abort.

        :param error_code: Numeric code of an error, as defined in the
                           error definition file.
        :param line: Line number where the error occurred, or None
                     if not applicable.
        :param kwargs: Keyword parameters to insert into the error message.
        :returns: Error message, and False iff a severe error has occurred.
        """

        try:
            error_class, message_template = self._error_definitions[error_code]
            message = message_template.format(**kwargs)
        except KeyError:
            msg = 'Unrecognized error code {}'.format(error_code)
            LOGGER.error(msg)
            raise ValueError(msg)

        self._report_batch['Line Number'].append(line)
        self._report_batch['Error Code'].append(error_code)
        self._report_batch['Error Type'].append(error_class)
        self._report_batch['Message'].append(message)

        severe = error_class != 'Warning'
        return message, severe

    def write(self):
        """
        Write a new operator report into the working directory (or update an
        existing one), which contains a summary of errors and warnings
        encountered in all files from the current processing attempt.

        See processing workflow for more information.

        :returns: void
        """

        if self._working_directory is None:
            # Ensure no files are written if working directory is null.
            return

        column_names = ['Line Number', 'Error Type', 'Error Code', 'Message']
        rows = zip(*[self._report_batch[name] for name in column_names])

        for line, err_type, err_code, message in rows:
            tokens = [
                self._report_batch['Processing Status'],
                err_type,
                err_code,
                line,
                message.replace(',', '\\,'),
                self._report_batch['Dataset'],
                self._report_batch['Data Level'],
                self._report_batch['Data Form'],
                self._report_batch['Agency'],
                self._report_batch['Station Type'],
                self._report_batch['Station ID'],
                self._report_batch['Filename'],
                self._report_batch['Incoming Path'],
                self._report_batch['Outgoing Path'],
                self._report_batch['URN']
            ]

            row = ','.join([
                '' if token is None else str(token) for token in tokens
            ])
            self.operator_report.write(row + '\n')

        # Reset file metadata in preparation for the next file to report.
        for field, column in self._report_batch.items():
            if isinstance(column, list):
                self._report_batch[field].clear()
            else:
                self._report_batch[field] = None


class RunReport(Report):
    """
    A WOUDC Data Registry run report is the simplest kind of report file
    generated by the Data Registry during a processing command call.
    It simply reports which files pass and which ones fail in that call.

    Inside a run report, files are split up into blocks based on what
    contributor submitted them. Each block starts with a line containing
    the contributor's acronym, followed by a list of lines of the form
    <status>: <filepath>, where <status> stands for either Pass or Fail.

    The `RunReport` class is responsible for writing run reports.
    """

    def __init__(self, root=None):
        """
        Initialize a new RunReport that will write to the directory <root>.

        For use in dummy or verification-only runs, passing <root> as None
        causes no output files to be produced.

        :param root: Path to the processing run's working directory.
        """

        super(RunReport, self).__init__(root)

        self._contributor_status = {}
        self._contributors = {
            'unknown': 'UNKNOWN'
        }

    def _load_processing_results_pass(self, filepath, contributor):
        """
        Associates the file given under <filepath> with the contributor
        acronym <contributor>, and records that the file was processed
        successfully.

        :param filepath: Path to an incoming data file.
        :param contributor: Acronym of the contributor that submitted the file.
        :returns: void
        """

        contributor_raw = contributor.replace('-', '').lower()
        self._contributors[contributor_raw] = contributor

        if contributor not in self._contributor_status:
            self._contributor_status[contributor] = []
        self._contributor_status[contributor].append(('Pass', filepath))

    def _load_processing_results_fail(self, filepath, contributor):
        """
        Associates the file given under <filepath> with the contributor
        acronym <contributor>, and records that the file failed to process.

        :param filepath: Path to an incoming data file.
        :param contributor: Acronym of the contributor that submitted the file.
        :returns: void
        """

        contributor_raw = contributor.replace('-', '').lower()
        if contributor_raw not in self._contributors:
            self._contributors[contributor_raw] = contributor

        if contributor not in self._contributor_status:
            self._contributor_status[contributor] = []
        self._contributor_status[contributor].append(('Fail', filepath))

    def filepath(self):
        """
        Returns a full path to the run report that will be written
         to this report's working directory.

        :returns: Full path to this run report file.
        """

        if self._working_directory is None:
            return None
        else:
            filename = 'run_report'
            return os.path.join(self._working_directory, filename)

    def write(self):
        """
        Write a new run report into the working directory (or update an
        existing one), which summarizes which files in the last
        processing attempt passed or failed.

        Files in a run report are grouped by contributing agency. Each
        contributor is represented by a block starting with its acronym,
        which is followed by a list of lines of the form <status>: <filepath>,
        where <status> stands for either Pass or Fail.

        :returns: void
        """

        for contributor in list(self._contributor_status.keys()):
            contributor_raw = contributor.replace('-', '').lower()

            if contributor_raw in self._contributors:
                contributor_official = self._contributors[contributor_raw]

                if contributor != contributor_official:
                    self._contributor_status[contributor_official].extend(
                        self._contributor_status.pop(contributor))
            else:
                if 'UNKNOWN' not in self._contributor_status:
                    self._contributor_status['UNKNOWN'] = []
                self._contributor_status['UNKNOWN'].extend(
                    self._contributor_status.pop(contributor_raw))

        contributor_list = sorted(list(self._contributor_status.keys()))
        if 'UNKNOWN' in contributor_list:
            # Move UNKNOWN to the end of the list, ignoring alphabetical order.
            contributor_list.remove('UNKNOWN')
            contributor_list.append('UNKNOWN')

        blocks = []
        for contributor in contributor_list:
            # List all files processed for each agency along with their status.
            package = contributor + '\n'
            process_results = self._contributor_status[contributor]

            for status, filepath in process_results:
                package += '{}: {}\n'.format(status, filepath)

            blocks.append(package)

        output_path = self.filepath()
        with open(output_path, 'w') as run_report:
            contents = '\n'.join(blocks)
            run_report.write(contents)


class EmailSummary:
    """
    A WOUDC Data Registry email summary is a file that briefly describes
    the overall performance of each contributor's submitted files over
    an entire processing run. It is used to give feedback to the contributors
    as well as for internal accounting.

    The email summary consists of a series of blocks which start with a
    contributor's acronym and mailing list. Below that, four lines describe
    how many of the contributor's files were processed, passed, fixed, or
    failed. Specific filenames and the failures/fixes they encountered
    are described at the bottom of the block.

    The `EmailSummary` class is responsible for writing email summary files.
    """

    def __init__(self, root):
        """
        Initialize a new EmailSummary that will write to the directory <root>.

        The path <root> is important for two reasons. First, as the processing
        run's working directory it contains all the logs and reports from
        the run, which are used to detect file status. Second, it is the
        output directory where the email summary file itself is written

        :param root: Path to the processing run's working directory.
        """

        self._working_directory = root

    def filepath(self):
        """
        Returns a full path to the email report for the processing run
        in this report's working directory.

        :returns: Full path to the processing run's email report.
        """

        today = date.today().strftime('%Y-%m-%d')
        filename = 'failed-files-{}'.format(today)

        return os.path.join(self._working_directory, filename)

    def write(self, addresses):
        """
        Write an email feedback summary to the working directory.
        The file describes, per agency, how many files in the whole
        processing run failed, were recovered, or passed the first time.

        The email summary is generated from the operator reports in the
        working directory. That is, the operator reports determine
        what counts as an error or a pass or a fail.

        See processing workflow for more information.

        :param addresses: Map of contributor acronym to email address.
        :returns: void
        """

        pass
