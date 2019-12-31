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

from datetime import datetime
import json
import logging

import click

from woudc_data_registry.epicentre.metadata import (
    add_metadata, get_metadata, update_metadata, delete_metadata)
from woudc_data_registry.models import Station, StationName
from woudc_data_registry.util import json_serial

from woudc_data_registry import config

LOGGER = logging.getLogger(__name__)

save_to_registry = config.get_config_extra('CLI', 'registry_enabled')
save_to_index = config.get_config_extra('CLI', 'search_index_enabled')


def build_station_name(ecsv):
    """
    Creates and returns a StationName instance from the contents of <ecsv>
    """

    station_id = str(ecsv.extcsv['PLATFORM']['ID'])
    station_name = ecsv.extcsv['PLATFORM']['Name']
    name_id = '{}:{}'.format(station_id, station_name)

    observation_time = ecsv.extcsv['TIMESTAMP']['Date']
    model = {
        'identifier': name_id,
        'station_id': station_id,
        'name': station_name,
        'first_seen': observation_time,
        'last_seen': observation_time
    }

    return StationName(model)


@click.group()
def station():
    """Station management"""
    pass


@click.command('list')
@click.pass_context
def list_(ctx):
    """List all stations"""

    for c in get_metadata(Station):
        click.echo('{} {}'.format(c.station_id.ljust(3), c.station_name.name))


@click.command('show')
@click.pass_context
@click.argument('identifier', required=True)
def show(ctx, identifier):
    """Show station details"""

    r = get_metadata(Station, identifier)

    if len(r) == 0:
        click.echo('Station not found')
        return

    click.echo(json.dumps(r[0].__geo_interface__, indent=4,
                          default=json_serial))


@click.command('add')
@click.option('-id', '--identifier', 'identifier', required=True,
              help='acronym')
@click.option('-t', '--type', 'type_', required=False, default='STN',
              help='station type')
@click.option('-n', '--name', 'name', required=True, help='name')
@click.option('-gi', '--gaw-id', 'gaw_id', required=True, help='GAW ID')
@click.option('-c', '--country', 'country', required=True, help='country')
@click.option('-w', '--wmo-region', 'wmo_region', required=True,
              help='WMO region')
@click.option('-sd', '--start-date', 'start_date', required=True,
              help='start date')
@click.option('-ed', '--end-date', 'end_date', required=False,
              help='end date')
@click.option('-g', '--geometry', 'geometry', required=True,
              help='latitude,longitude,elevation')
@click.pass_context
def add(ctx, identifier, name, type_, gaw_id, country,
        wmo_region, start_date, end_date, geometry):
    """Add a station"""

    geom_tokens = geometry.split(',')

    station_ = {
        'station_id': identifier,
        'station_name': name,
        'station_type': type_,
        'gaw_id': gaw_id,
        'country_id': country,
        'wmo_region_id': wmo_region,
        'start_date': start_date,
        'end_date': end_date,
        'x': geom_tokens[1],
        'y': geom_tokens[0],
        'z': geom_tokens[2]
    }

    add_metadata(Station, station_, save_to_registry, save_to_index)
    click.echo('Station {} added'.format(identifier))


@click.command('update')
@click.option('-id', '--identifier', 'identifier', required=True,
              help='acronym')
@click.option('-t', '--type', 'type_', help='station type')
@click.option('-n', '--name', 'name', help='name')
@click.option('-gi', '--gaw-id', 'gaw_id', help='GAW ID')
@click.option('-c', '--country', 'country', help='country')
@click.option('-w', '--wmo-region', 'wmo_region', help='WMO region')
@click.option('-sd', '--start-date', 'start_date', help='start date')
@click.option('-ed', '--end-date', 'end_date', help='end date')
@click.option('-g', '--geometry', 'geometry',
              help='latitude,longitude,elevation')
@click.pass_context
def update(ctx, identifier, name, type_, gaw_id, country,
           wmo_region, start_date, end_date, geometry):
    """Update station information"""

    station_ = {
        'station_id': identifier,
        'last_validated_datetime': datetime.utcnow()
    }

    if name:
        station_['station_name'] = name
    if type_:
        station_['station_type'] = type_
    if gaw_id:
        station_['gaw_id'] = gaw_id
    if country:
        station_['country_id'] = country
    if wmo_region:
        station_['wmo_region_id'] = wmo_region
    if start_date:
        station_['start_date'] = start_date
    if end_date:
        station_['end_date'] = end_date

    if geometry:
        geom_tokens = geometry.split(',')
        station_['x'] = geom_tokens[1]
        station_['y'] = geom_tokens[0]
        station_['z'] = geom_tokens[2]

    if len(station_.keys()) == 1:
        click.echo('No updates specified')
        return

    update_metadata(Station, identifier, station_,
                    save_to_registry, save_to_index)
    click.echo('Station {} updated'.format(identifier))


@click.command('delete')
@click.argument('identifier', required=True)
@click.pass_context
def delete(ctx, identifier):
    """Delete a station"""

    if len(get_metadata(Station, identifier)) == 0:
        click.echo('Station not found')
        return

    q = 'Are you sure you want to delete station {}?'.format(identifier)

    if click.confirm(q):  # noqa
        delete_metadata(Station, identifier, save_to_registry, save_to_index)

    click.echo('Station {} deleted'.format(identifier))


station.add_command(list_)
station.add_command(show)
station.add_command(add)
station.add_command(update)
station.add_command(delete)
