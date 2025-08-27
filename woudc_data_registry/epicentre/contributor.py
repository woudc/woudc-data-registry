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

from datetime import datetime
import json
import logging

import click

from woudc_data_registry import cli_options
from woudc_data_registry.epicentre.metadata import (
    add_metadata, get_metadata, update_metadata, delete_metadata)
from woudc_data_registry.models import Contributor
from woudc_data_registry.util import json_serial

from woudc_data_registry import config

LOGGER = logging.getLogger(__name__)

save_to_registry = config.EXTRAS['cli']['registry_enabled']
save_to_index = config.EXTRAS['cli']['search_index_enabled']


@click.group()
def contributor():
    """Contributor management"""
    pass


@click.command('list')
@click.pass_context
@cli_options.OPTION_VERBOSITY
def list_(ctx, verbosity):
    """List all contributors"""

    for c in get_metadata(Contributor):
        click.echo(f'{c.contributor_id.ljust(24)} {c.name}')


@click.command()
@click.pass_context
@cli_options.OPTION_VERBOSITY
@click.argument('identifier', required=True)
def show(ctx, identifier, verbosity):
    """Show contributor details"""

    r = get_metadata(Contributor, identifier)

    if len(r) == 0:
        click.echo('Contributor not found')
        return

    click.echo(json.dumps(r[0].__geo_interface__, indent=4,
                          default=json_serial))


@click.command()
@click.pass_context
@cli_options.OPTION_VERBOSITY
@click.option('-n', '--name', 'name', required=True, help='name')
@click.option('-a', '--acronym', 'acronym', required=True, help='acronym')
@click.option('-c', '--country', 'country', required=True, help='country')
@click.option('-p', '--project', 'project', required=True, help='project')
@click.option('-w', '--wmo-region', 'wmo_region', required=True,
              help='WMO region')
@click.option('-u', '--url', 'url', required=True, help='URL')
@click.option('-e', '--email', 'email', required=True, help='email')
@click.option('-f', '--ftp-username', 'ftp_username', required=True,
              help='FTP username')
@click.option('-g', '--geometry', 'geometry', required=True,
              help='latitude,longitude')
def add(ctx, name, acronym, country, project, wmo_region,
        url, email, ftp_username, geometry, verbosity):
    """Add a contributor"""

    geom_tokens = geometry.split(',')

    contributor_ = {
        'name': name,
        'acronym': acronym,
        'country_id': country,
        'project_id': project,
        'wmo_region_id': wmo_region,
        'url': url,
        'email': email,
        'ftp_username': ftp_username,
        'x': geom_tokens[1],
        'y': geom_tokens[0]
    }

    result = add_metadata(Contributor, contributor_,
                          save_to_registry, save_to_index)
    click.echo(f'Contributor {result.contributor_id} added')


@click.command()
@click.pass_context
@cli_options.OPTION_VERBOSITY
@click.option('-id', '--identifier', 'identifier', required=True,
              help='acronym')
@click.option('-n', '--name', 'name', help='name')
@click.option('-a', '--acronym', 'acronym', help='acronym')
@click.option('-c', '--country', 'country', help='country')
@click.option('-p', '--project', 'project', help='project')
@click.option('-w', '--wmo-region', 'wmo_region', help='WMO region')
@click.option('-u', '--url', 'url', help='URL')
@click.option('-e', '--email', 'email', help='email')
@click.option('-f', '--ftp-username', 'ftp_username', help='FTP username')
@click.option('-g', '--geometry', 'geometry', help='latitude,longitude')
def update(ctx, identifier, name, acronym, country, project,
           wmo_region, url, email, ftp_username, geometry, verbosity):
    """Update contributor information"""

    contributor_ = {
        'last_validated_datetime': datetime.utcnow()
    }

    if name:
        contributor_['name'] = name
    if acronym:
        contributor_['acronym'] = acronym
    if country:
        contributor_['country_id'] = country
    if project:
        contributor_['project_id'] = project
    if wmo_region:
        contributor_['wmo_region_id'] = wmo_region
    if url:
        contributor_['url'] = url
    if email:
        contributor_['email'] = email
    if ftp_username:
        contributor_['ftp_username'] = ftp_username
    if geometry:
        geom_tokens = geometry.split(',')
        contributor_['x'] = geom_tokens[1]
        contributor_['y'] = geom_tokens[0]

    if len(contributor_.keys()) == 1:
        click.echo('No updates specified')
        return

    update_metadata(Contributor, identifier, contributor_,
                    save_to_registry, save_to_index)
    click.echo(f'Contributor {identifier} updated')


@click.command()
@click.pass_context
@cli_options.OPTION_VERBOSITY
@click.argument('identifier', required=True)
def delete(ctx, identifier, verbosity):
    """Delete a contributor"""

    if len(get_metadata(Contributor, identifier)) == 0:
        click.echo('Contributor not found')
        return

    q = f'Are you sure you want to delete contributor {identifier}?'

    if click.confirm(q):  # noqa
        delete_metadata(Contributor, identifier,
                        save_to_registry, save_to_index)

    click.echo(f'Contributor {identifier} deleted')


contributor.add_command(list_)
contributor.add_command(show)
contributor.add_command(add)
contributor.add_command(update)
contributor.add_command(delete)
