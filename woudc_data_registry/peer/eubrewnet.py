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
# Copyright (c) 2021 Government of Canada
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

import csv
import logging

import click

from woudc_data_registry.models import PeerDataRecord
from woudc_data_registry.registry import Registry

from woudc_data_registry.peer.file_indices_extractor import (
    config_lookup,
    get_metadata
)

LOGGER = logging.getLogger(__name__)


def parse_index(csv_dict_reader):
    """
    iterates through CSV DictReader object

    :param dict_reader: `csv.DictReader` object

    :returns: generator of parsed rows
    """

    LOGGER.info('Start the execution of file index metadata extraction.')

    overwrite_flag = True
    lookup_lists = config_lookup(overwrite_flag)

    for row in csv_dict_reader:
        # Resolve station metadata lookup
        station_metadata = get_metadata(
            row['Station_name'], row['Agency'], lookup_lists)

        if station_metadata[1] is not None:
            properties = dict(
                source='eubrewnet',
                measurement=row['Measurement'],
                agency=row['Agency'],
                station_id=row['WOUDC_ID'],
                station_name=row['Station_name'],
                gaw_id=row['GAW_ID'],
                station_type=row['Station_type'],
                level=row['Product_level'],
                instrument_type=row['Instrument_type'],
                country_id=station_metadata[1],
                pi_name=row['PI_name'],
                pi_email=row['PI_email'],
                url=row['Link'],
                y=row['Lat'],
                x=row['Lon_E'],
                z=row['Height'],
                start_datetime=row['Start_time'],
                end_datetime=row['End_time']
            )
            yield properties
        else:
            LOGGER.debug('No station metadata found.')
            msg = 'Failed to persist PeerDataRecord({})'.format(row['Link'])
            LOGGER.error(msg)
            yield {}


@click.group()
def eubrewnet():
    """Eubrewnet data centre management"""
    pass


@click.command()
@click.pass_context
@click.option('-fi', '--file-index', type=click.Path(exists=True,
              resolve_path=True), help='Path to file index')
def index(ctx, file_index):
    """index EUBREWNET file index"""

    if file_index is None:
        raise click.ClickException('missing -fi/--file-index parameter')

    registry_ = Registry()

    click.echo('Clearing existing EUBREWNET records')
    registry_.session.query(PeerDataRecord).filter(
        PeerDataRecord.source == 'eubrewnet').delete()
    registry_.session.commit()

    click.echo('Indexing EUBREWNET records from {}'.format(file_index))
    with open(file_index, encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for dict_row in parse_index(reader):
            peer_data_record = PeerDataRecord(dict_row)
            registry_.save(peer_data_record)


eubrewnet.add_command(index)
