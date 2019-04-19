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

import json
import logging

import click
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import (ConnectionError, NotFoundError,
                                      RequestError)
from woudc_data_registry import config
from woudc_data_registry.util import json_serial

LOGGER = logging.getLogger(__name__)

INDEXES = {
    'contributor': 'woudc-data-registry.contributor',
    'station': 'woudc-data-registry.station',
    'instrument': 'woudc-data-registry.instrument',
    'data_record': 'woudc-data-registry.data_record'
}


class SearchIndex(object):
    """Search index"""

    def __init__(self):
        """constructor"""

        self.type = config.WDR_SEARCH_TYPE
        self.url = config.WDR_SEARCH_URL

        LOGGER.debug('Connecting to Elasticsearch')
        self.connection = Elasticsearch([self.url])

        self.headers = {'Content-Type': 'application/json'}

    def create(self):
        """create search indexes"""
        settings = {
            'mappings': {
                'FeatureCollection': {
                    'properties': {
                        'geometry': {
                            'type': 'geo_shape'
                        }
                    }
                }
            },
            'settings': {
                'index': {
                    'number_of_shards': 1,
                    'number_of_replicas': 0
                }
            }
        }

        for key, value in INDEXES.items():
            try:
                self.connection.indices.create(index=value, body=settings)
            except (ConnectionError, RequestError) as err:
                LOGGER.error(err)
                raise SearchIndexError(err)

    def delete(self):
        """delete search indexes"""
        for key, value in INDEXES.items():
            try:
                self.connection.indices.delete(value)
            except NotFoundError as err:
                LOGGER.error(err)
                raise SearchIndexError(err)

    def index_data_record(self, data):
        """
        index or update a document

        :param data: `dict` of GeoJSON representation

        :returns: `bool` status of indexing result
        """

        identifier = data['id']

        try:
            result = self.connection.get(index=INDEXES['data_record'],
                                         doc_type='FeatureCollection',
                                         id=identifier)
            LOGGER.debug('existing record, updating')
            data_ = json.dumps({'doc': data}, default=json_serial)
            result = self.connection.update(index=INDEXES['data_record'],
                                            doc_type='FeatureCollection',
                                            id=identifier, body=data_)
            LOGGER.debug('Result: {}'.format(result))
        except NotFoundError as err:  # index new
            LOGGER.debug(err)
            LOGGER.info('new record, indexing')
            data_ = json.dumps(data, default=json_serial)
            LOGGER.debug('indexing {}'.format(identifier))
            try:
                result = self.connection.index(index=INDEXES['data_record'],
                                               doc_type='FeatureCollection',
                                               id=identifier, body=data_)
                LOGGER.debug('Result: {}'.format(result))
            except RequestError as err:
                LOGGER.error(err)
                raise SearchIndexError(err)

        return True

    def unindex_data_record(self, identifier):
        """
        delete document from index

        :param identifier: identifier of data record

        :returns: `bool` status of un-indexing result
        """

        result = self.connection.delete(index=INDEXES['data_record'],
                                        doc_type='FeatureCollection',
                                        id=identifier)

        if result.status_code == 404:
            msg = 'Data record {} does not exist'.format(identifier)
            LOGGER.error(msg)
            raise SearchIndexError(msg)

        return True


class SearchIndexError(Exception):
    """custom exception handler"""
    pass


@click.group()
def search():
    """Search"""
    pass


@click.command('create-index')
@click.pass_context
def create_index(ctx):
    """create search index"""

    click.echo('Creating index')
    es = SearchIndex()
    es.create()
    click.echo('Done')


@click.command('delete-index')
@click.pass_context
def delete_index(ctx):
    """delete search index"""

    click.echo('Deleting index')
    es = SearchIndex()
    es.delete()
    click.echo('Done')


search.add_command(create_index)
search.add_command(delete_index)
