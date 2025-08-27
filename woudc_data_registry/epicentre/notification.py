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
import yaml

from woudc_data_registry.epicentre.metadata import (
    add_metadata, get_metadata, update_metadata, delete_metadata)
from woudc_data_registry.models import Notification
from woudc_data_registry.util import json_serial

from woudc_data_registry import cli_options, config

LOGGER = logging.getLogger(__name__)

save_to_registry = config.EXTRAS['cli']['registry_enabled']
save_to_index = config.EXTRAS['cli']['search_index_enabled']


@click.group()
def notification():
    """News notification management"""
    pass


@click.command('list')
@click.pass_context
@cli_options.OPTION_VERBOSITY
def list_(ctx, verbosity):
    """List all news notifications"""

    for c in get_metadata(Notification):
        click.echo(f'{c.published_date} {c.title_en}')


@click.command()
@click.pass_context
@cli_options.OPTION_VERBOSITY
@click.argument('identifier', required=True)
def show(ctx, identifier, verbosity):
    """Show news notification details"""

    r = get_metadata(Notification, identifier)

    if len(r) == 0:
        click.echo('Notification not found')
        return

    click.echo(json.dumps(r[0].__geo_interface__, indent=4,
                          default=json_serial))


@click.command()
@click.pass_context
@cli_options.OPTION_VERBOSITY
@click.option('-id', '--identifier', 'identifier', required=False,
              help='Forced news item identifier')
@click.option('-p', '--path', required=True,
              help='Path to YML file with news notification definition')
def add(ctx, identifier, path, verbosity):
    """Add a news notification"""

    LOGGER.info('Loading notification file from: %s', path)
    with open(path) as news_file:
        notification = yaml.safe_load(news_file)
        if 'published' not in notification:
            notification['published'] = datetime.now().strftime('%Y-%m-%d')

        if identifier:
            notification['identifier'] = identifier

        LOGGER.debug('Parsed notification:\n%s',
                     yaml.dump(notification, sort_keys=False,
                               allow_unicode=True))

        added = add_metadata(Notification, notification,
                             save_to_registry, save_to_index)
        click.echo(f'Notification {added.notification_id} added')


@click.command()
@click.pass_context
@cli_options.OPTION_VERBOSITY
@click.option('-id', '--identifier', 'identifier', required=False,
              help='Forced news item identifier')
@click.option('-p', '--path', required=True,
              help='Path to YML file with news notification definition')
def update(ctx, identifier, path, verbosity):
    """Update news notification"""

    LOGGER.info('Loading notification file from: %s', path)
    with open(path) as news_file:
        notification = yaml.safe_load(news_file)

        if len(notification.keys()) == 1:
            click.echo('No updates specified')
            return

        LOGGER.debug('Parsed notification:\n%s',
                     yaml.dump(notification, sort_keys=False,
                               allow_unicode=True))

        update_metadata(Notification, identifier, notification,
                        save_to_registry, save_to_index)
        click.echo(f'Notification {identifier} updated')


@click.command()
@click.pass_context
@cli_options.OPTION_VERBOSITY
@click.argument('identifier', required=True)
def delete(ctx, identifier, verbosity):
    """Delete a notification"""

    if len(get_metadata(Notification, identifier)) == 0:
        click.echo('Station not found')
        return

    q = f'Are you sure you want to delete news notification {identifier}?'

    if click.confirm(q):  # noqa
        delete_metadata(Notification, identifier,
                        save_to_registry, save_to_index)

    click.echo(f'News notification {identifier} deleted')


notification.add_command(list_)
notification.add_command(show)
notification.add_command(add)
notification.add_command(update)
notification.add_command(delete)
