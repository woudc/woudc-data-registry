
import click
import json

from woudc_data_registry.epicentre.metadata import (
    add_metadata, get_metadata, update_metadata, delete_metadata)
from woudc_data_registry.models import Instrument
from woudc_data_registry.util import json_serial


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

    instrument_id = ':'.join([name, model, serial, station, dataset])
    model = {
        'identifier': instrument_id,
        'name': name,
        'model': model,
        'serial': serial,
        'station_id': station,
        'dataset_id': dataset,
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
@click.option('-id', '--identifier', 'identifier', required=True,
              help='identifier')
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
def add(ctx, identifier, station, dataset, name, model, serial, geometry):
    """Add an instrument"""

    geom_tokens = geometry.split(',')
    if len(geom_tokens) == 2:
        geom_tokens.append(None)

    instrument_ = {
        'identifier': identifier,
        'station_id': station,
        'dataset_id': dataset,
        'name': name,
        'model': model,
        'serial': serial,
        'x': geom_tokens[0],
        'y': geom_tokens[1],
        'z': geom_tokens[2]
    }

    add_metadata(Instrument, instrument_)
    click.echo('Instrument {} added'.format(identifier))


@click.command('update')
@click.option('-id', '--identifier', 'identifier', required=True,
              help='identifier')
@click.option('-g', '--geometry', 'geometry',
              help='latitude,longitude[,height]')
@click.pass_context
def update(ctx, identifier, geometry):
    """Update instrument information"""

    instrument_ = {}

    if geometry:
        geom_tokens = geometry.split(',')
        if len(geom_tokens) == 2:
            geom_tokens.append(None)

        instrument_['x'] = geom_tokens[0]
        instrument_['y'] = geom_tokens[1]
        instrument_['z'] = geom_tokens[2]

    if len(instrument_.keys()) == 0:
        click.echo('No updates specified')
        return

    update_metadata(Instrument, identifier, instrument_)
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
        delete_metadata(Instrument, identifier)

    click.echo('Instrument {} deleted'.format(identifier))


instrument.add_command(list_)
instrument.add_command(show)
instrument.add_command(add)
instrument.add_command(update)
instrument.add_command(delete)
