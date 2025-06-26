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

import logging

import click
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import (ConnectionError, NotFoundError,
                                      RequestError)
from elastic_transport import TlsError
from woudc_data_registry import config

LOGGER = logging.getLogger(__name__)


typedefs = {
    'keyword': {
        'type': 'keyword',
        'ignore_above': 256
    }
}

dataset_links = {
    'type': 'nested',
    'properties': {
        'label_en': {
            'type': 'text',
            'fields': {'raw': typedefs['keyword']}
        },
        'label_fr': {
            'type': 'text',
            'fields': {'raw': typedefs['keyword']}
        },
        'description_en': {
            'type': 'text',
            'fields': {'raw': typedefs['keyword']}
        },
        'description_fr': {
            'type': 'text',
            'fields': {'raw': typedefs['keyword']}
        },
        'function': {
            'type': 'text',
            'fields': {'raw': typedefs['keyword']}
        },
        'linktype': {
            'type': 'text',
            'fields': {'raw': typedefs['keyword']}
        },
        'url': {
            'type': 'text',
            'fields': {'raw': typedefs['keyword']}
        }
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
            },
            'dataset_name': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'dataset_level': {
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
    'discovery_metadata': {
        'index': 'discovery_metadata',
        'properties': {
            'identifier': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'abstract_en': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'abstract_fr': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'dataset_snapshots': dataset_links,
            'doi': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'keywords_en': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'keywords_fr': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'language': {
                'type': 'object',
                'properties': {
                    'code': {'type': 'keyword'}
                }
            },
            'woudc:content_category': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'levels': {
                'type': 'nested',
                'properties': {
                    'label_en': {
                        'type': 'text',
                        'fields': {'raw': typedefs['keyword']}
                    },
                    'networks': {
                        'type': 'nested',
                        'properties': {
                            'instruments': {
                                'type': 'text',
                                'fields': {'raw': typedefs['keyword']}
                            },
                            'label_en': {
                                'type': 'text',
                                'fields': {'raw': typedefs['keyword']}
                            }
                        }
                    }
                }
            },
            'search': dataset_links,
            'spatial_extent': {
                'type': 'long',
                'fields': {'raw': typedefs['keyword']}
            },
            'temporal_begin': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'temporal_end': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'title_en': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'title_fr': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'topic_category': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'uri': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'dataset_snapshots': dataset_links,
            'waf': dataset_links,
            'wfs': dataset_links,
            'wms': dataset_links
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
            'dataset_id': {
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
            'country_name_en': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'country_name_fr': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'contributor_acronym': {
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
                'type': 'keyword',
                'fields': {'raw': typedefs['keyword']}
            },
            'content_form': {
                'type': 'byte'
            },
            'content_level': {
                'type': 'float'
            },
            'dataset_id': {
                'type': 'keyword',
                'fields': {'raw': typedefs['keyword']}
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
                'format': DATE_FORMAT,
                'fields': {'raw': typedefs['keyword']}
            },
            'timestamp_time': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'timestamp_utc': {
                'type': 'date',
                'format': DATE_FORMAT,
                'fields': {'raw': typedefs['keyword']}
            },
            'published': {
                'type': 'boolean'
            },
            'received_datetime': {
                'type': 'date',
                'format': DATE_FORMAT,
                'fields': {'raw': typedefs['keyword']}
            },
            'inserted_datetime': {
                'type': 'date',
                'format': DATE_FORMAT,
                'fields': {'raw': typedefs['keyword']}
            },
            'processed_datetime': {
                'type': 'date',
                'format': DATE_FORMAT,
                'fields': {'raw': typedefs['keyword']}
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
    },
    'contributions': {
        'index': 'contribution',
        'properties': {
            'identifier': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'project_id': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'dataset_id': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'station_id': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'name': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'country_id': {
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
            'instrument_name': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'contributor_name': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'start_date': {
                'type': 'date'
            },
            'end_date': {
                'type': 'date'
            }
        }
    },
    'notifications': {
        'index': 'notification',
        'properties': {
            'title_en': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'title_fr': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'description_en': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'description_fr': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'keywords_en': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'keywords_fr': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'published_date': {
                'type': 'date',
                'format': DATE_FORMAT
            },
            'banner': {
                'type': 'boolean'
            },
            'visible': {
                'type': 'boolean'
            }
        }
    },
    'peer_data_records': {
        'index': 'peer_data_record',
        'properties': {
            'identifier': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'source': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'measurement': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'station_id': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'country_id': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'contributor_id': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'contributor_url': {
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
            'station_type': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'instrument_type': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'url': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'level': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'start_datetime': {
                'type': 'date',
                'format': DATE_FORMAT,
                'fields': {'raw': typedefs['keyword']}
            },
            'end_datetime': {
                'type': 'date',
                'format': DATE_FORMAT,
                'fields': {'raw': typedefs['keyword']}
            },
            'last_validated_datetime': {
                'type': 'date',
                'format': DATE_FORMAT,
                'fields': {'raw': typedefs['keyword']}
            },
        }
    },
    'uv_index_hourly': {
        'index': 'uv_index_hourly',
        'properties': {
            'identifier': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'file_path': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'dataset_id': {
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
            'station_gaw_id': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'station_gaw_url': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'contributor_name': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'contributor_id': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'contributor_url': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'country_id': {
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
            'gaw_id': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'solar_zenith_angle': {
                'type': 'float'
            },
            'observation_utcoffset': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'observation_date': {
                'type': 'date',
                'format': DATE_FORMAT,
                'fields': {'raw': typedefs['keyword']}
            },
            'observation_time': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'timestamp_utc': {
                'type': 'date',
                'format': DATE_FORMAT,
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
            'instrument_serial': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'uv_index': {
                'type': 'float',
            },
            'uv_daily_max': {
                'type': 'float',
            },
            'uv_index_qa': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'url': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
        }
    },
    'totalozone': {
        'index': 'totalozone',
        'properties': {
            'identifier': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'file_path': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'dataset_id': {
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
            'station_gaw_id': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'station_gaw_url': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'contributor_name': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'contributor_id': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'contributor_url': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'country_id': {
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
            'gaw_id': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'observation_date': {
                'type': 'date',
                'format': DATE_FORMAT,
                'fields': {'raw': typedefs['keyword']}
            },
            'daily_date': {
                'type': 'date',
                'format': DATE_FORMAT,
                'fields': {'raw': typedefs['keyword']}
            },
            'daily_wlcode': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'daily_obscode': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'daily_columno3': {
                'type': 'float',
            },
            'daily_stdevo3': {
                'type': 'float',
            },
            'daily_utc_begin': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'daily_utc_end': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'daily_utc_mean': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'daily_nobs': {
                'type': 'float',
            },
            'daily_mmu': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'daily_columnso2': {
                'type': 'float',
            },
            'monthly_date': {
                'type': 'date',
                'format': DATE_FORMAT,
                'fields': {'raw': typedefs['keyword']}
            },
            'monthly_columno3': {
                'type': 'float',
            },
            'monthly_stdevo3': {
                'type': 'float',
            },
            'monthly_npts': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'url': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
        }
    },
    'ozonesonde': {
        'index': 'ozonesonde',
        'properties': {
            'identifier': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'file_path': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'dataset_id': {
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
            'station_gaw_id': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'station_gaw_url': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'contributor_name': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'contributor_id': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'contributor_url': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'country_id': {
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
            'pressure': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'o3partialpressure': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'temperature': {
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
            'instrument_serial': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'timestamp_date': {
                'type': 'date',
                'format': DATE_FORMAT,
                'fields': {'raw': typedefs['keyword']}
            },
            'url': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            }
        }
    },
    'station_dobson_corrections': {
        'index': 'station_dobson_corrections',
        'properties': {
            'identifier': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'station_id': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'AD_corrected': {
                'type': 'boolean'
            },
            'CD_correction': {
                'type': 'boolean'
            },
            'AD_correcting_source': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'CD_correcting_source': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'CD_correcting_factor': {
                'type': 'text',
                'fields': {'raw': typedefs['keyword']}
            },
            'correction_comments': {
                'type': 'text'
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
        self.verify_certs = config.WDR_SEARCH_CERT_VERIFY

        LOGGER.debug(
            f"Connecting to Elasticsearch (verify_certs=${self.verify_certs})"
        )

        try:
            self.connection = Elasticsearch(
                self.url,
                verify_certs=self.verify_certs
            )
        except TlsError as err:
            if self.verify_certs:
                msg = (
                    f"SSL certificate verification failed: {err}.\n"
                    "Check your SSL certificates or set "
                    "WDR_SEARCH_CERT_VERIFY=False if "
                    "connecting to an internal dev server."
                )
            else:
                msg = f"Unexpected TLS error: {err}"

            LOGGER.error(msg)
            raise SearchIndexError(msg)

        self.headers = {'Content-Type': 'application/json'}

    def generate_index_name(self, index_name):
        """
        Generates index name with prefix if specified in config/environment

        :param index_name: ES index name
        :returns: fully qualified index name
        """

        return f'{self.index_basename}.{index_name}'

    def create(self):
        """create search indexes"""

        search_index_config = config.EXTRAS.get('search_index', {})

        for key, definition in MAPPINGS.items():
            # Skip indexes that have been manually disabled.
            enabled_flag = f'{key}_enabled'
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
            except TlsError as err:
                LOGGER.error(
                    "TLS error occurred while creating "
                    f"index '{index_name}': {err}"
                )
                raise SearchIndexError(
                    "TLS error while creating index "
                    f"'{index_name}': {err}.\n"
                    "Check your SSL certificates or set "
                    "WDR_SEARCH_CERT_VERIFY=False if "
                    "connecting to an internal dev server."
                )
            except (ConnectionError, RequestError) as err:
                LOGGER.error(
                    "Error occurred while creating "
                    f"index '{index_name}': {err}"
                )
                raise SearchIndexError(
                    "Failed to create index "
                    f"'{index_name}': {err}.\n"
                    "Check your Elasticsearch connection, ensure the index "
                    "settings are correct, and verify if the index "
                    "already exists."
                )
            except Exception as err:
                LOGGER.error(
                    "Unexpected error occurred while creating "
                    f"index '{index_name}': {err}"
                )
                raise SearchIndexError(
                    "Unexpected error while creating index "
                    f"'{index_name}': {err}.\n"
                    "Please check the logs for more details."
                )

    def delete(self):
        """delete search indexes"""

        search_index_config = config.EXTRAS.get('search_index', {})

        for key, definition in MAPPINGS.items():
            # Skip indexes that have been manually disabled.
            enabled_flag = f'{key}_enabled'
            if not search_index_config.get(enabled_flag, True):
                continue

            index_name = self.generate_index_name(definition['index'])

            try:
                self.connection.indices.delete(index=index_name)
            except NotFoundError as err:
                LOGGER.error(
                    f"Index '{index_name}' not found. Skipping deletion.")
                raise SearchIndexError(
                    f"Index '{index_name}' not found: {err}")
            except TlsError as err:
                LOGGER.error(
                    "TLS error occurred while trying to delete "
                    f"index '{index_name}': {err}"
                )
                raise SearchIndexError(
                    "TLS error while deleting "
                    f"index '{index_name}': {err}.\n"
                    "Check your SSL certificates or set "
                    "WDR_SEARCH_CERT_VERIFY=False if "
                    "connecting to an internal dev server."
                )
            except Exception as err:
                LOGGER.error(
                    "Unexpected error occurred while deleting "
                    f"index '{index_name}': {err}"
                )
                raise SearchIndexError(
                    "Unexpected error while deleting "
                    f"index '{index_name}': {err}"
                )

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
        enabled_flag = f'{domain.__tablename__}_enabled'

        if not search_index_config.get(enabled_flag, True):
            msg = f'{domain.__tablename__} index is currently frozen'
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

            LOGGER.debug(f'Indexing 1 document into {index_name}')
            self.connection.update(
                index=index_name,
                id=target['id'],
                body=wrapper
            )
        else:
            # Index/update multiple documents using bulk API.
            wrapper = ({
                '_op_type': 'update',
                '_index': index_name,
                '_id': document['id'],
                'doc': document,
                'doc_as_upsert': True
            } for document in target)

            LOGGER.debug(f'Indexing documents into {index_name}')
            try:
                # Perform the bulk operation
                success_count, failed = helpers.bulk(
                    self.connection,
                    wrapper,
                    raise_on_error=False,
                    raise_on_exception=False
                )

                if failed:
                    LOGGER.error(
                        f"Some documents failed to index in '{index_name}'.")
                    for fail in failed:
                        LOGGER.error(f"Failure: {fail}")
                    click.echo(
                        f"[WARNING] {len(failed)} documents failed to index "
                        f"in '{index_name}'. "
                        "Check error logs for details."
                    )
                if success_count > 0:
                    LOGGER.info(f"Successfully indexed {success_count} "
                                f" documents into '{index_name}'.")
                    click.echo(f"[SUCCESS] {success_count} documents indexed "
                               f"into '{index_name}'.")

            except TlsError as err:
                msg = (
                    "TLS error occurred while bulk indexing "
                    f"index '{index_name}': {err}.\n"
                    "Check your SSL certificates or set "
                    "WDR_SEARCH_CERT_VERIFY=False if "
                    "connecting to an internal dev server."
                )
                LOGGER.error(msg)
                raise SearchIndexError(msg)

            except (ConnectionError, RequestError) as err:
                msg = (
                    "Elasticsearch connection error occurred while bulk "
                    f"indexing index '{index_name}': {err}.\n"
                    "Ensure that your Elasticsearch cluster is up and "
                    "reachable."
                )
                LOGGER.error(msg)
                raise SearchIndexError(msg)

            except Exception as err:
                msg = (
                    "Unexpected error while bulk indexing "
                    f"index '{index_name}': {err}"
                )
                LOGGER.exception(msg)
                raise SearchIndexError(msg)

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
        enabled_flag = f'{domain.__tablename__}_enabled'

        if not search_index_config.get(enabled_flag, True):
            msg = f'{domain.__tablename__} index is currently frozen'
            LOGGER.warning(msg)
            return False

        index_name = self.generate_index_name(
            MAPPINGS[domain.__tablename__]['index'])

        if isinstance(target, str):
            # <target> is a document ID, delete normally.
            result = self.connection.delete(index=index_name, id=target)

            if result['result'] != 'deleted':
                msg = f'Data record {target} does not exist'
                LOGGER.error(msg)
                raise SearchIndexError(msg)
        elif isinstance(target, dict):
            # <target> is the single GeoJSON object to delete.
            result = self.connection.delete(index=index_name, id=target['id'])

            if result['result'] != 'deleted':
                msg = f"Data record {target['id']} does not exist"
                LOGGER.error(msg)
                raise SearchIndexError(msg)
        else:
            # Delete multiple documents using bulk API.
            wrapper = ({
                '_op_type': 'delete',
                '_index': index_name,
                '_id': document['id']
            } for document in target)

            helpers.bulk(self.connection, wrapper,
                         raise_on_error=False, raise_on_exception=False)

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
        enabled_flag = f'{domain.__tablename__}_enabled'

        if not search_index_config.get(enabled_flag, True):
            msg = f'{domain.__tablename__} index is currently frozen'
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

        self.connection.delete_by_query(index=index_name, body=query)
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
