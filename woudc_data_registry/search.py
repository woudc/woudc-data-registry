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

import logging
from urllib.parse import urlparse

import click
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import (ConnectionError, NotFoundError,
                                      RequestError)
from woudc_data_registry import config

LOGGER = logging.getLogger(__name__)


typedefs = {
    'keyword': {
        'type': 'keyword',
        'ignore_above': 256
    }
}

DATE_FORMAT = 'date_time_no_millis'

MAPPINGS = {
    'projects': {
        'index': 'project',
        'properties': {
            'identifier': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            }
        }
    },
    'datasets': {
        'index': 'dataset',
        'properties': {
            'identifier': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'data_class': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            }
        }
    },
    'countries': {
        'index': 'country',
        'properties': {
            'identifier': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'country_name_en': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'country_name_fr': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'wmo_region_id': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'wmo_membership': {
                'type': 'date',
                'format': DATE_FORMAT
            },
            'regional_involvement': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'link': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            }
        }
    },
    'contributors': {
        'index': 'contributor',
        'properties': {
            'identifier': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'acronym': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'name': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'project': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'country_name_en': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'country_name_fr': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'wmo_region_id': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'url': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'active': {
                'type': 'boolean'
            },
            'start_date': {
                'type': 'date',
                'format': DATE_FORMAT
            },
            'end_date': {
                'type': 'date',
                'format': DATE_FORMAT
            },
            'last_validated_datetime': {
                'type': 'date',
                'format': DATE_FORMAT
            }
        }
    },
    'stations': {
        'index': 'station',
        'properties': {
            'woudc_id': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'gaw_id': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'name': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'type': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'country_name_en': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'country_name_fr': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'wmo_region_id': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'active': {
                'type': 'boolean'
            },
            'start_date': {
                'type': 'date',
                'format': DATE_FORMAT
            },
            'end_date': {
                'type': 'date',
                'format': DATE_FORMAT
            },
            'last_validated_datetime': {
                'type': 'date',
                'format': DATE_FORMAT
            },
            'gaw_url': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            }
        }
    },
    'instruments': {
        'index': 'instrument',
        'properties': {
            'identifier': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'station_id': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'station_name': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'data_class': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'dataset': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'name': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'contributor_name': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'model': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'serial': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'start_date': {
                'type': 'date',
                'format': DATE_FORMAT
            },
            'end_date': {
                'type': 'date',
                'format': DATE_FORMAT
            },
            'waf_url': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            }
        }
    },
    'deployments': {
        'index': 'deployment',
        'properties': {
            'identifier': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'station_id': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'station_type': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'station_name': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'station_country_en': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'station_country_fr': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'contributor': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'contributor_name': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'contributor_project': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'contributor_url': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'start_date': {
                'type': 'date',
                'format': DATE_FORMAT
            },
            'end_date': {
                'type': 'date',
                'format': DATE_FORMAT
            }
        }
    },
    'data_records': {
        'index': 'data_record',
        'properties': {
            'identifier': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'content_class': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'content_category': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'content_form': {
                'type': 'byte'
            },
            'content_level': {
                'type': 'float'
            },
            'data_generation_agency': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'data_generation_date': {
                'type': 'date',
                'format': DATE_FORMAT
            },
            'data_generation_version': {
                'type': 'float'
            },
            'data_generation_scientific_authority': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'platform_id': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'platform_type': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'platform_name': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'platform_country': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'platform_gaw_id': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'instrument_name': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'instrument_model': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'instrument_number': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'timestamp_utcoffset': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'timestamp_date': {
                'type': 'date',
                'format': DATE_FORMAT
            },
            'timestamp_time': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'published': {
                'type': 'boolean'
            },
            'received_datetime': {
                'type': 'date',
                'format': DATE_FORMAT
            },
            'inserted_datetime': {
                'type': 'date',
                'format': DATE_FORMAT
            },
            'processed_datetime': {
                'type': 'date',
                'format': DATE_FORMAT
            },
            'published_datetime': {
                'type': 'date',
                'format': DATE_FORMAT
            },
            'number_of_observations': {
                'type': 'integer'
            },
            'url': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            }
        }
    }
}


class SearchIndex(object):
    """Search index"""

    def __init__(self):
        """constructor"""

        self.type = config.WDR_SEARCH_TYPE
        self.url = config.WDR_SEARCH_URL
        self.index_basename = config.WDR_SEARCH_INDEX_BASENAME

        LOGGER.debug('Connecting to Elasticsearch')
        url_parsed = urlparse(self.url)
        url_settings = {
            'host': url_parsed.hostname
        }

        if url_parsed.port is None:  # proxy to default HTTP(S) port
            if url_parsed.scheme == 'https':
                url_settings['port'] = 443
                url_settings['scheme'] = url_parsed.scheme
            else:
                url_settings['port'] = 80
        else:  # was set explictly
            url_settings['port'] = url_parsed.port

        if url_parsed.path is not None:
            url_settings['url_prefix'] = url_parsed.path

        LOGGER.debug('URL settings: {}'.format(url_settings))

        AUTH = (config.WDR_SEARCH_USERNAME, config.WDR_SEARCH_PASSWORD)
        if None in AUTH:
            self.connection = Elasticsearch([url_settings])
        else:
            LOGGER.debug('Connecting using username {}'.format(AUTH[0]))
            self.connection = Elasticsearch([url_settings], http_auth=AUTH,
                                            verify_certs=False)

        self.headers = {'Content-Type': 'application/json'}

    def generate_index_name(self, index_name):
        """
        Generates index name with prefix if specified in config/environment

        :param index_name: ES index name
        :returns: fully qualified index name
        """

        if self.index_basename is not None:
            return '{}.{}'.format(self.index_basename, index_name)

        return index_name

    def create(self):
        """create search indexes"""

        search_index_config = config.EXTRAS.get('search_index', {})

        for key, definition in MAPPINGS.items():
            # Skip indexes that have been manually disabled.
            enabled_flag = '{}_enabled'.format(key)
            if not search_index_config.get(enabled_flag, True):
                continue

            index_name = self.generate_index_name(definition['index'])

            settings = {
                'mappings': {
                    'properties': {
                        'geometry': {
                            'type': 'geo_shape'
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

            if 'properties' in definition:
                settings['mappings']['properties']['properties'] = {
                    'properties': definition['properties']
                }

            try:
                self.connection.indices.create(index=index_name, body=settings)
            except (ConnectionError, RequestError) as err:
                LOGGER.error(err)
                raise SearchIndexError(err)

    def delete(self):
        """delete search indexes"""

        search_index_config = config.EXTRAS.get('search_index', {})

        for key, definition in MAPPINGS.items():
            # Skip indexes that have been manually disabled.
            enabled_flag = '{}_enabled'.format(key)
            if not search_index_config.get(enabled_flag, True):
                continue

            index_name = self.generate_index_name(definition['index'])

            try:
                self.connection.indices.delete(index_name)
            except NotFoundError as err:
                LOGGER.error(err)
                raise SearchIndexError(err)

    def get_record_version(self, identifier):
        """
        get version of data record

        :param identifier: identifier of data record
        :returns: `float` version of data record
        """

        try:
            index_name = self.generate_index_name(
                MAPPINGS['data_records']['index'])

            result = self.connection.get(index=index_name,
                                         id=identifier)
            return result['_source']['properties']['data_generation_version']
        except NotFoundError:
            return None

    def index(self, domain, target):
        """
        Index (or update if already present) one or more documents in
        <target> that belong to the index associated with <domain>.

        :param domain: A model class that all entries in <target> belong to.
        :param target: GeoJSON dictionary of model data or a list of them.
        :returns: `bool` of whether the operation was successful.
        """

        search_index_config = config.EXTRAS.get('search_index', {})
        enabled_flag = '{}_enabled'.format(domain.__tablename__)

        if not search_index_config.get(enabled_flag, True):
            msg = '{} index is currently frozen'.format(domain.__tablename__)
            LOGGER.warning(msg)
            return False

        index_name = self.generate_index_name(
            MAPPINGS[domain.__tablename__]['index'])

        if isinstance(target, dict):
            # Index/update single document the normal way.
            wrapper = {
                'doc': target,
                'doc_as_upsert': True
            }

            LOGGER.debug('Indexing 1 document into {}'.format(index_name))
            self.connection.update(index=index_name, id=target['id'],
                                   body=wrapper)
        else:
            # Index/update multiple documents using bulk API.
            wrapper = [{
                '_op_type': 'update',
                '_index': index_name,
                '_type': '_doc',
                '_id': document['id'],
                'doc': document,
                'doc_as_upsert': True
            } for document in target]

            LOGGER.debug('Indexing {} documents into {}'
                         .format(len(target), index_name))
            helpers.bulk(self.connection, wrapper)

        return True

    def unindex(self, domain, target):
        """
        Delete one or more documents, referred to by <target>,
        that belong to the index associated with <domain>.

        :param domain: A model class that all entries in <target> belong to.
        :param target: GeoJSON dictionary of model data or a list of them.
        :returns: `bool` of whether the operation was successful.
        """

        search_index_config = config.EXTRAS.get('search_index', {})
        enabled_flag = '{}_enabled'.format(domain.__tablename__)

        if not search_index_config.get(enabled_flag, True):
            msg = '{} index is currently frozen'.format(domain.__tablename__)
            LOGGER.warning(msg)
            return False

        index_name = self.generate_index_name(
            MAPPINGS[domain.__tablename__]['index'])

        if isinstance(target, str):
            # <target> is a document ID, delete normally.
            result = self.connection.delete(index=index_name, id=target)

            if result['result'] != 'deleted':
                msg = 'Data record {} does not exist'.format(target)
                LOGGER.error(msg)
                raise SearchIndexError(msg)
        elif isinstance(target, dict):
            # <target> is the single GeoJSON object to delete.
            result = self.connection.delete(index=index_name, id=target['id'])

            if result['result'] != 'deleted':
                msg = 'Data record {} does not exist'.format(target['id'])
                LOGGER.error(msg)
                raise SearchIndexError(msg)
        else:
            # Delete multiple documents using bulk API.
            wrapper = [{
                '_op_type': 'delete',
                '_index': index_name,
                '_type': '_doc',
                '_id': document['id']
            } for document in target]

            helpers.bulk(self.connection, wrapper)

        return True

    def unindex_except(self, domain, targets):
        """
        Deletes all documents from the index associated with <domain>
        that have no matching identifier in <targets>

        :param domain: A model class that all entries in <target> belong to.
        :param target: List of GeoJSON model data.
        :returns: `bool` of whether the operation was successful.
        """

        search_index_config = config.EXTRAS.get('search_index', {})
        enabled_flag = '{}_enabled'.format(domain.__tablename__)

        if not search_index_config.get(enabled_flag, True):
            msg = '{} index is currently frozen'.format(domain.__tablename__)
            LOGGER.warning(msg)
            return False

        index_name = self.generate_index_name(
            MAPPINGS[domain.__tablename__]['index'])

        ids = [document['id'] for document in targets]

        query = {
            'query': {
                'bool': {
                    'mustNot': {
                        'ids': {
                            'values': ids
                        }
                    }
                }
            }
        }

        self.connection.delete_by_query(index_name, query)
        return True


class SearchIndexError(Exception):
    """custom exception handler"""
    pass


@click.group()
def search():
    """Search"""
    pass


@click.command('setup')
@click.pass_context
def create_indexes(ctx):
    """create search indexes"""

    click.echo('Creating indexes')
    es = SearchIndex()
    es.create()
    click.echo('Done')


@click.command('teardown')
@click.pass_context
def delete_indexes(ctx):
    """delete search indexes"""

    click.echo('Deleting indexes')
    es = SearchIndex()
    es.delete()
    click.echo('Done')


search.add_command(create_indexes)
search.add_command(delete_indexes)
