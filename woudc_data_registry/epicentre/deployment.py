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
from datetime import date

import json

from woudc_data_registry.epicentre.metadata import (
    add_metadata, get_metadata, update_metadata, delete_metadata)
from woudc_data_registry.models import Deployment
from woudc_data_registry.util import json_serial

from woudc_data_registry import config

save_to_registry = config.EXTRAS['cli']['registry_enabled']
save_to_index = config.EXTRAS['cli']['search_index_enabled']


def build_deployment(ecsv):
    """Creates and returns a Deployment instance from the contents of <ecsv>"""

    station = str(ecsv.extcsv['PLATFORM']['ID'])
    agency = ecsv.extcsv['DATA_GENERATION']['Agency']
    project = ecsv.extcsv['CONTENT']['Class']
    timestamp_date = ecsv.extcsv['TIMESTAMP']['Date']

    contributor_id = ':'.join([agency, project])
    deployment_id = ':'.join([station, agency, project])
    deployment_model = {
        'identifier': deployment_id,
        'station_id': station,
        'contributor_id': contributor_id,
        'start_date': timestamp_date,
        'end_date': timestamp_date
    }

    return Deployment(deployment_model)


@click.group()
def deployment():
    """Deployment management"""
    pass


@click.command('list')
@click.pass_context
def list_(ctx):
    """List all deployments"""

    for c in get_metadata(Deployment):
        click.echo('{} @ {}'.format(c.contributor_id.ljust(20), c.station_id))


@click.command('show')
@click.pass_context
@click.argument('identifier', required=True)
def show(ctx, identifier):
    """Show deployment details"""

    r = get_metadata(Deployment, identifier)

    if len(r) == 0:
        click.echo('Deployment not found')
        return

    click.echo(json.dumps(r[0].__geo_interface__, indent=4,
                          default=json_serial))


@click.command('add')
@click.option('-s', '--station', 'station', required=True, help='station')
@click.option('-c', '--contributor', 'contributor', required=True,
              help='contributor')
@click.option('-sd', '--start', 'start_date', required=False,
              default=date.today().strftime('%Y-%m-%d'),
              type=click.DateTime(['%Y-%m-%d']), help='deployment start date')
@click.option('-ed', '--end', 'end_date', required=False, default=None,
              type=click.DateTime(['%Y-%m-%d']), help='deployment end date')
@click.pass_context
def add(ctx, station, contributor, start_date, end_date):
    """Add a deployment"""

    start_date = start_date.date()
    if end_date is not None:
        end_date = end_date.date()

    deployment_ = {
        'station_id': station,
        'contributor_id': contributor,
        'start_date': start_date,
        'end_date': end_date
    }

    result = add_metadata(Deployment, deployment_,
                          save_to_registry, save_to_index)
    click.echo('Deployment {} added'.format(result.deployment_id))


@click.command('update')
@click.option('-id', '--identifier', 'identifier', required=True,
              help='identifier')
@click.option('-s', '--station', 'station', help='station')
@click.option('-c', '--contributor', 'contributor', help='contributor')
@click.option('-sd', '--start', 'start_date',
              type=click.DateTime(['%Y-%m-%d']), help='deployment start date')
@click.option('-ed', '--end', 'end_date', type=click.DateTime(['%Y-%m-%d']),
              help='deployment end date')
@click.pass_context
def update(ctx, identifier, station, contributor, start_date, end_date):
    """Update deployment information"""

    deployment_ = {}

    if station:
        deployment_['station_id'] = station
    if contributor:
        deployment_['contributor_id'] = contributor
    if start_date:
        deployment_['start_date'] = start_date.date()
    if end_date:
        deployment_['end_date'] = end_date.date()

    if len(deployment_.keys()) == 0:
        click.echo('No updates specified')
        return

    update_metadata(Deployment, identifier, deployment_,
                    save_to_registry, save_to_index)
    click.echo('Deployment {} updated'.format(identifier))


@click.command('delete')
@click.argument('identifier', required=True)
@click.pass_context
def delete(ctx, identifier):
    """Delete a deployment"""

    if len(get_metadata(Deployment, identifier)) == 0:
        click.echo('Contributor not found')
        return

    q = 'Are you sure you want to delete deployment {}?'.format(identifier)

    if click.confirm(q):  # noqa
        delete_metadata(Deployment, identifier,
                        save_to_registry, save_to_index)

    click.echo('Deployment {} deleted'.format(identifier))


deployment.add_command(list_)
deployment.add_command(show)
deployment.add_command(add)
deployment.add_command(update)
deployment.add_command(delete)
