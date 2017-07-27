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
# Copyright (c) 2017 Government of Canada
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

import logging
import os

import click

from woudc_data_registry import config
from woudc_data_registry.parser import ExtendedCSV, MetadataValidationError
from woudc_data_registry.util import read_file

LOGGER = logging.getLogger(__name__)


def process_data_record(data_record):
    """process incoming data record"""

    LOGGER.info('Parsing data record')
    dr = ExtendedCSV(data_record)

    LOGGER.info('Validating Extended CSV')
    # validate Extended CSV
    try:
        dr.validate_metadata()
    except MetadataValidationError as mve:
        LOGGER.exception('Invalid Extended CSV')
        LOGGER.exception(mve.errors)

    # verify:
    # - Extended CSV core fields against registry
    # - taxonomy/URI check
    # - duplicate data submitted
    # - new version of file

    # register Extended CSV to registry


@click.command()
@click.pass_context
@click.option('--file', '-f', 'file_',
              type=click.Path(exists=True, resolve_path=True),
              help='Path to data record')
@click.option('--directory', '-d', 'directory',
              type=click.Path(exists=True, resolve_path=True,
                              dir_okay=True, file_okay=False),
              help='Path to directory of data records')
def process(ctx, file_, directory):
    """process a single data submission or directory of files"""

    if file_ is not None and directory is not None:
        msg = '--file and --directory are mutually exclusive'
        raise click.ClickException(msg)

    if file_ is not None:
        process_data_record(read_file(file_))

    elif directory is not None:
        for root, dirs, files in os.walk(directory):
            for f in files:
                file_contents = read_file(os.path.join(root, f))
                process_data_record(file_contents)


#    from sqlalchemy.exc import DataError
#    from sqlalchemy.orm import sessionmaker
#
#    from woudc_data_registry import config, parser
#
#    engine = create_engine(config.DATABASE_URL, echo=config.DEBUG)
#    Session = sessionmaker(bind=engine)
#    session = Session()
#
#    extcsv_ = parser.ExtendedCSV('/users/ec/dmsec/kralidist/woudc-data-registry/woudc-data-registry/20040709.ECC.2Z.2ZL1.NOAA-CMDL.csv')  # noqa
#    d1 = DataRecord(extcsv_)
#    d1.url = 'http://woudc.org/'
#
#    try:
#        session.add(d1)
#        session.commit()
#        session.close()
#    except DataError as err:
#        session.rollback()
#        click.echo('ERROR: {}'.format(err))
