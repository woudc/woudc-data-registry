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

import json
import logging

import click
import requests

from woudc_data_registry import config
from woudc_data_registry.util import json_serial

LOGGER = logging.getLogger(__name__)


class SearchIndex(object):
    """Search index"""

    def __init__(self):
        """constructor"""

        self.type = config.SEARCH_TYPE

        self.url = '{}/woudc-data-registry'.format(
            config.SEARCH_URL.rstrip('/'))

        self.headers = {'Content-Type': 'application/json'}

    def create_index(self):
        result = requests.put(self.url)
        if not result.ok:
            raise SearchIndexError(
                result.json()['error']['reason'])
        return True

    def delete_index(self):
        result = requests.delete(self.url)
        if not result.ok:
            raise SearchIndexError(
                result.json()['error']['reason'])
        return True

    def index_data_record(self, data):
        """index or update a document"""

        url = '{}/data_record/_search'.format(self.url)
        identifier = data['properties']['identifier']

        query = {
            'query': {
                'match': {
                    'properties.urn': data['properties']['urn']
                }
            },
            '_source': {
                'excludes': ['properties.raw']
            }
        }

        result = requests.post(url, data=json.dumps(query)).json()

        if result['hits']['total'] > 0:  # exists, update
            print('UPDATE')
            LOGGER.debug('existing record, updating')
            url = '{}/data_record/{}/_update'.format(self.url, identifier)

            print(url)
            data_ = json.dumps({'doc': data}, default=json_serial)

            result = requests.post(url, data=data_)
        else:  # index new
            print('INSERT')
            LOGGER.debug('new record, indexing')
            data_ = json.dumps(data, default=json_serial)
            url = '{}/data_record/{}'.format(self.url, identifier)
            result = requests.put(url, headers=self.headers, data=data_)

        print(result)
        if not result.ok:
            raise SearchIndexError(result.json()['error']['reason'])

        return True

    def unindex_data_record(self, data):
        """delete document from index"""

        identifier = data['properties']['identifier']

        url = '{}/data_record/{}'.format(self.url, identifier)

        result = requests.delete(url)
        print(result.text)

        if result.status_code == 404:
            raise SearchIndexError('Data record {} does not exist'.format(
                identifier))


class SearchIndexError(Exception):
    """custom exception handler"""
    pass


@click.group()
def search():
    pass


@click.command()
@click.pass_context
def create_index(ctx):
    """create search index"""

    click.echo('Creating index')
    es = SearchIndex()
    es.create_index()
    click.echo('Done')


@click.command()
@click.pass_context
def delete_index(ctx):
    """delete search index"""

    click.echo('Deleting index')
    es = SearchIndex()
    es.delete_index()
    click.echo('Done')


search.add_command(create_index)
search.add_command(delete_index)
