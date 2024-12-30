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

import click
import logging
import os

from pathlib import Path
from woudc_extcsv import (ExtendedCSV, NonStandardDataError,
                          MetadataValidationError)

from woudc_data_registry import config
from woudc_data_registry.util import (is_text_file, read_file,
                                      send_email)


from woudc_data_registry.processing import Process

from woudc_data_registry.generate_metadata import update_extents
from woudc_data_registry.models import Contributor
from woudc_data_registry.registry import Registry
from woudc_data_registry.report import OperatorReport, RunReport, EmailSummary
from woudc_data_registry.search import SearchIndex


LOGGER = logging.getLogger(__name__)


def orchestrate(source, working_dir, metadata_only=False,
                verify_only=False, bypass=False):
    """
    Core orchestation workflow

    :param source: Path to input file or directory tree containing them.
    :param working_dir: Output directory for log and report files.
    :param metadata_only: `bool` of whether to verify only the
                          common metadata tables.
    :param verify_only: `bool` of whether to verify the file for correctness
                        without processing.
    :param bypass: `bool` of whether to skip permission prompts for adding
                   new records.
    :returns: void
    """

    files_to_process = []

    if os.path.isfile(source):
        fullpath = Path(source).parent.resolve()
        parent_dir = os.path.basename(str(fullpath))

        # Use parent dir to guess the contributor acronym during processing
        # runs, where the parent path is the contributor's FTP name.
        files_to_process = [(source, parent_dir)]
    elif os.path.isdir(source):
        for root, dirs, files in os.walk(source):
            parent_dir = os.path.basename(root)

            for f in files:
                fullpath = os.path.join(root, f)
                files_to_process.append((fullpath, parent_dir))

    files_to_process.sort()

    passed = []
    failed = []

    registry = Registry()
    search_engine = SearchIndex()

    with OperatorReport(working_dir) as op_report, \
         click.progressbar(files_to_process, label='Processing files') as run_:  # noqa

        run_report = RunReport(working_dir)

        for file_to_process, contributor in run_:
            click.echo(f'Processing filename: {file_to_process}')

            LOGGER.info('Detecting file')
            if not is_text_file(file_to_process):
                _, is_error = op_report.add_message(101)
                if is_error:
                    op_report.write_failing_file(file_to_process, contributor)
                    run_report.write_failing_file(file_to_process, contributor)

                    failed.append(file_to_process)
                    continue

            try:
                contents = read_file(file_to_process)

                LOGGER.info('Parsing data record')
                extcsv = ExtendedCSV(contents, op_report)

                LOGGER.info('Validating Extended CSV')
                extcsv.validate_metadata_tables()
                contributor = extcsv.extcsv['DATA_GENERATION']['Agency']

                if not metadata_only:
                    extcsv.validate_dataset_tables()
                LOGGER.info('Valid Extended CSV')

                p = Process(registry, search_engine, op_report)
                data_record = p.validate(extcsv, bypass=bypass,
                                         metadata_only=metadata_only)
                if data_record is None:
                    click.echo('Not ingesting')
                    failed.append(file_to_process)

                    op_report.write_failing_file(file_to_process,
                                                 contributor, extcsv)
                    run_report.write_failing_file(file_to_process, contributor)
                else:
                    data_record.ingest_filepath = file_to_process
                    data_record.filename = os.path.basename(file_to_process)
                    data_record.url = \
                        data_record.get_waf_path(config.WDR_WAF_BASEURL)
                    data_record.output_filepath = \
                        data_record.get_waf_path(config.WDR_WAF_BASEDIR)
                    if verify_only:
                        click.echo('Verified but not ingested')
                    else:
                        p.persist()
                        click.echo('Ingested successfully')

                    op_report.write_passing_file(file_to_process, extcsv,
                                                 data_record)
                    run_report.write_passing_file(file_to_process, contributor)

                    passed.append(file_to_process)

            except UnicodeDecodeError as err:
                LOGGER.error(f'Unknown file format: {err}')

                click.echo('Not ingested')
                failed.append(file_to_process)

                op_report.write_failing_file(file_to_process, contributor)
                run_report.write_failing_file(file_to_process, contributor)
            except NonStandardDataError as err:
                LOGGER.error(f'Invalid Extended CSV: {err.errors}')

                click.echo('Not ingested')
                failed.append(file_to_process)

                op_report.write_failing_file(file_to_process, contributor)
                run_report.write_failing_file(file_to_process, contributor)
            except MetadataValidationError as err:
                LOGGER.error(f'Invalid Extended CSV: {err.errors}')

                click.echo('Not ingested')
                failed.append(file_to_process)

                op_report.write_failing_file(file_to_process, contributor)
                run_report.write_failing_file(file_to_process, contributor)
            except Exception as err:
                click.echo(f'Processing failed: {err}')
                failed.append(file_to_process)

                op_report.write_failing_file(file_to_process, contributor)
                run_report.write_failing_file(file_to_process, contributor)

    registry.close_session()

    for name in files_to_process:
        if name in passed:
            click.echo(f'Pass: {name}')
        elif name in failed:
            click.echo(f'Fail: {name}')

    click.echo(f'({len(passed)}/{len(files_to_process)} files passed)')


@click.group()
def data():
    """Data processing"""
    pass


@click.command()
@click.pass_context
@click.argument('source', type=click.Path(exists=True, resolve_path=True,
                                          dir_okay=True, file_okay=True))
@click.option('--report', '-r', 'reports_dir', default=None,
              help='Path to main output directory for logs and reports')
@click.option('--lax', '-l', 'lax', is_flag=True,
              help='Only validate core metadata tables')
@click.option('--yes', '-y', 'bypass', is_flag=True, default=False,
              help='Bypass permission prompts while ingesting')
def ingest(ctx, source, reports_dir, lax, bypass):
    """ingest a single data submission or directory of files"""

    orchestrate(source, reports_dir, metadata_only=lax, bypass=bypass)
    update_extents()


@click.command()
@click.pass_context
@click.argument('source', type=click.Path(exists=True, resolve_path=True,
                                          dir_okay=True, file_okay=True))
@click.option('--lax', '-l', 'lax', is_flag=True,
              help='Only validate core metadata tables')
@click.option('--yes', '-y', 'bypass', is_flag=True, default=False,
              help='Bypass permission prompts while ingesting')
def verify(ctx, source, lax, bypass):
    """verify a single data submission or directory of files"""

    orchestrate(source, None, metadata_only=lax,
                verify_only=True, bypass=bypass)


@click.command()
@click.pass_context
@click.option('--working-dir', '-w', 'working_dir', required=True,
              type=click.Path(exists=True, resolve_path=True, file_okay=False))
def generate_emails(ctx, working_dir):
    """Write an email report based on the processing run in <working_dir>"""

    registry = Registry()
    email_summary = EmailSummary(working_dir)

    contributors = registry.query_full_index(Contributor)
    ctx.addresses = {model.acronym: model.email for model in contributors}

    email_summary.write(ctx.addresses)


@click.command()
@click.pass_context
@click.option('--test', is_flag=True, help="Enable the test flag.")
@click.option('--ops', is_flag=True, help="Enable the ops flag.")
@click.argument('failed_files', type=click.File('r'))
def send_feedback(ctx, failed_files, test, ops):
    """Send operating reports to contributors. """

    LOGGER.debug("test: {} ops: {}".format(test, ops))
    with open(config.WDR_TEMPLATE_PATH, 'r') as file:
        message = file.read()

    templates = failed_files.read().split('\n\n')
    template_collection = [template.split('\n') for template in templates]

    subject = 'WOUDC data processing report (contributor_acronym)'
    host = config.WDR_EMAIL_HOST
    port = config.WDR_EMAIL_PORT
    from_email_address = config.WDR_EMAIL_FROM_USERNAME
    cc_addresses = [config.WDR_EMAIL_CC]
    bcc_addresses = [config.WDR_EMAIL_BCC]

    LOGGER.info('Configs all set to send feedback to contributors')

    for contributor in template_collection:
        acronym = contributor[0].split(' ')[0].lower()
        specific_message = message.replace(
            "$EMAIL_SUMMARY", "\n".join(contributor[1:]))
        specific_subject = subject.replace('contributor_acronym', acronym)

        if test:
            to_email_addresses = config.WDR_EMAIL_TO.split(",")
            subject = (
                'TEST: WOUDC data processing report ({})'.format(acronym))
            LOGGER.info(
                'Sending Test data report to agency: %s with emails to: %s',
                acronym, to_email_addresses
            )
            send_email(
                specific_message, subject, from_email_address,
                to_email_addresses, host, port, cc_addresses,
                bcc_addresses
            )
        elif ops:
            to_email_addresses = [
                email.strip() for email in contributor[0].split(' ')[1]
                .translate(str.maketrans("", "", "()")).split(";")]
            LOGGER.info(
                'Sending data report to agency: %s with emails to: %s',
                acronym, to_email_addresses
            )
            send_email(
                specific_message, specific_subject, from_email_address,
                to_email_addresses, host, port, cc_addresses,
                bcc_addresses
            )
        LOGGER.debug(
            'Sent email to %s with emails to %s',
            acronym, to_email_addresses
        )
    LOGGER.info('Processing Reports have been sent')


data.add_command(ingest)
data.add_command(verify)
data.add_command(generate_emails, name='generate-emails')
data.add_command(send_feedback, name='send-feedback')
