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

import click

from woudc_data_registry.processing import Process

from woudc_data_registry.registry import Registry
from woudc_data_registry.search import SearchIndex


def orchestrate(file_, directory, metadata_only=False,
                verify_only=False, bypass=False):
    """
    Core orchestation workflow

    :param file_: File to process.
    :param directory: Directory to process (recursive).
    :param metadata_only: `bool` of whether to verify only the
                          common metadata tables.
    :param verify_only: `bool` of whether to verify the file for correctness
                        without processing.
    :param bypass: `bool` of whether to skip permission prompts for adding
                   new records.
    :returns: void
    """

    files_to_process = []

    if file_ is not None:
        files_to_process = [file_]
    elif directory is not None:
        for root, dirs, files in os.walk(directory):
            for f in files:
                files_to_process.append(os.path.join(root, f))

    files_to_process.sort()

    passed = []
    failed = []

    registry = Registry()
    search_engine = SearchIndex()

    with click.progressbar(files_to_process, label='Processing files') as run_:
        for file_to_process in run_:
            click.echo('Processing filename: {}'.format(file_to_process))
            p = Process(registry, search_engine)
            try:
                if p.validate(file_to_process, metadata_only=metadata_only,
                              verify_only=verify_only, bypass=bypass):

                    if verify_only:
                        click.echo('Verified but not ingested')
                    else:
                        p.persist()
                        click.echo('Ingested successfully')
                    passed.append(file_to_process)
                else:
                    click.echo('Not ingested')
                    failed.append(file_to_process)
            except Exception as err:
                click.echo('Processing failed: {}'.format(err))
                failed.append(file_to_process)

    registry.close_session()

    for name in files_to_process:
        if name in passed:
            click.echo('Pass: {}'.format(name))
        elif name in failed:
            click.echo('Fail: {}'.format(name))

    click.echo('({}/{} files passed)'
               .format(len(passed), len(files_to_process)))


@click.group()
def data():
    """Data processing"""
    pass


@click.command()
@click.pass_context
@click.option('--file', '-f', 'file_',
              type=click.Path(exists=True, resolve_path=True),
              help='Path to data record')
@click.option('--directory', '-d', 'directory',
              type=click.Path(exists=True, resolve_path=True,
                              dir_okay=True, file_okay=False),
              help='Path to directory of data records')
@click.option('--lax', '-l', 'lax', is_flag=True,
              help='Only validate core metadata tables')
@click.option('--yes', '-y', 'bypass', is_flag=True, default=False,
              help='Bypass permission prompts while ingesting')
def ingest(ctx, file_, directory, lax, bypass):
    """ingest a single data submission or directory of files"""

    if file_ is not None and directory is not None:
        msg = '--file and --directory are mutually exclusive'
        raise click.ClickException(msg)

    if file_ is None and directory is None:
        msg = 'One of --file or --directory is required'
        raise click.ClickException(msg)

    orchestrate(file_, directory, metadata_only=lax, bypass=bypass)


@click.command()
@click.pass_context
@click.option('--file', '-f', 'file_',
              type=click.Path(exists=True, resolve_path=True),
              help='Path to data record')
@click.option('--directory', '-d', 'directory',
              type=click.Path(exists=True, resolve_path=True,
                              dir_okay=True, file_okay=False),
              help='Path to directory of data records')
@click.option('--lax', '-l', 'lax', is_flag=True,
              help='Only validate core metadata tables')
@click.option('--yes', '-y', 'bypass', is_flag=True, default=False,
              help='Bypass permission prompts while ingesting')
def verify(ctx, file_, directory, lax, bypass):
    """verify a single data submission or directory of files"""

    if file_ is not None and directory is not None:
        msg = '--file and --directory are mutually exclusive'
        raise click.ClickException(msg)

    if file_ is None and directory is None:
        msg = 'One of --file or --directory is required'
        raise click.ClickException(msg)

    orchestrate(file_, directory, metadata_only=lax, verify_only=True,
                bypass=bypass)


data.add_command(ingest)
data.add_command(verify)
