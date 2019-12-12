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

import click
import json

from woudc_data_registry.epicentre.metadata import (
    add_metadata, get_metadata, update_metadata, delete_metadata)
from woudc_data_registry.models import Instrument
from woudc_data_registry.util import json_serial

from woudc_data_registry import config

save_to_registry = config.EXTRAS['cli']['registry_enabled']
save_to_index = config.EXTRAS['cli']['search_index_enabled']


def build_instrument(ecsv):
    """
    Creates and returns an Instrument instance from the contents of <ecsv>
    """

    name = ecsv.extcsv['INSTRUMENT']['Name']
    model = str(ecsv.extcsv['INSTRUMENT']['Model'])
    serial = str(ecsv.extcsv['INSTRUMENT']['Number'])
    station = str(ecsv.extcsv['PLATFORM']['ID'])
    dataset = ecsv.extcsv['CONTENT']['Category']
    location = [ecsv.extcsv['LOCATION'].get(f, None)
                for f in ['Longitude', 'Latitude', 'Height']]
    timestamp_date = ecsv.extcsv['TIMESTAMP']['Date']

    instrument_id = ':'.join([name, model, serial, station, dataset])
    model = {
        'identifier': instrument_id,
        'name': name,
        'model': model,
        'serial': serial,
        'station_id': station,
        'dataset_id': dataset,
        'start_date': timestamp_date,
        'end_date': timestamp_date,
        'x': location[0],
        'y': location[1],
        'z': location[2]
    }

    return Instrument(model)


@click.group()
def instrument():
    """Instrument management"""
    pass


@click.command('list')
@click.pass_context
def list_(ctx):
    """List all instruments"""

    for c in get_metadata(Instrument):
        descriptor = ' '.join([c.name, c.model, c.serial])
        station = '{}{}'.format(c.station.station_type, c.station_id)
        click.echo('{} - {}, {}'.format(descriptor.ljust(30), station,
                                        c.dataset_id))


@click.command('show')
@click.pass_context
@click.argument('identifier', required=True)
def show(ctx, identifier):
    """Show instrument details"""

    r = get_metadata(Instrument, identifier)

    if len(r) == 0:
        click.echo('Instrument not found')
        return

    click.echo(json.dumps(r[0].__geo_interface__, indent=4,
                          default=json_serial))


@click.command('add')
@click.option('-st', '--station', 'station', required=True,
              help='station ID')
@click.option('-d', '--dataset', 'dataset', required=True, help='dataset')
@click.option('-n', '--name', 'name', required=True, help='instrument name')
@click.option('-m', '--model', 'model', required=True,
              help='instrument model')
@click.option('-s', '--serial', 'serial', required=True,
              help='instrument serial number')
@click.option('-g', '--geometry', 'geometry', required=True,
              help='latitude,longitude[,height]')
@click.pass_context
def add(ctx, station, dataset, name, model, serial, geometry):
    """Add an instrument"""

    geom_tokens = geometry.split(',')
    if len(geom_tokens) == 2:
        geom_tokens.append(None)

    instrument_ = {
        'station_id': station,
        'dataset_id': dataset,
        'name': name,
        'model': model,
        'serial': serial,
        'x': geom_tokens[1],
        'y': geom_tokens[0],
        'z': geom_tokens[2]
    }

    result = add_metadata(Instrument, instrument_,
                          save_to_registry, save_to_index)
    click.echo('Instrument {} added'.format(result.instrument_id))


@click.command('update')
@click.option('-id', '--identifier', 'identifier', required=True,
              help='identifier')
@click.option('-st', '--station', 'station', help='station ID')
@click.option('-d', '--dataset', 'dataset', help='dataset')
@click.option('-n', '--name', 'name', help='instrument name')
@click.option('-m', '--model', 'model', help='instrument model')
@click.option('-s', '--serial', 'serial', help='instrument serial number')
@click.option('-g', '--geometry', 'geometry',
              help='latitude,longitude[,height]')
@click.pass_context
def update(ctx, identifier, station, dataset, name, model, serial, geometry):
    """Update instrument information"""

    instrument_ = {}

    if station:
        instrument_['station_id'] = station
    if dataset:
        instrument_['dataset_id'] = dataset
    if name:
        instrument_['name'] = name
    if model:
        instrument_['model'] = model
    if serial:
        instrument_['serial'] = serial

    if geometry:
        geom_tokens = geometry.split(',')
        if len(geom_tokens) == 2:
            geom_tokens.append(None)

        instrument_['x'] = geom_tokens[1]
        instrument_['y'] = geom_tokens[0]
        instrument_['z'] = geom_tokens[2]

    if len(instrument_.keys()) == 0:
        click.echo('No updates specified')
        return

    update_metadata(Instrument, identifier, instrument_,
                    save_to_registry, save_to_index)
    click.echo('Instrument {} updated'.format(identifier))


@click.command('delete')
@click.argument('identifier', required=True)
@click.pass_context
def delete(ctx, identifier):
    """Delete an instrument"""

    if len(get_metadata(Instrument, identifier)) == 0:
        click.echo('Instrument not found')
        return

    q = 'Are you sure you want to delete instrument {}?'.format(identifier)

    if click.confirm(q):  # noqa
        delete_metadata(Instrument, identifier,
                        save_to_registry, save_to_index)

    click.echo('Instrument {} deleted'.format(identifier))


instrument.add_command(list_)
instrument.add_command(show)
instrument.add_command(add)
instrument.add_command(update)
instrument.add_command(delete)
