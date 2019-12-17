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
import os

from woudc_data_registry.util import str2bool

LOGGER = logging.getLogger(__name__)

WDR_LOGGING_LOGLEVEL = os.getenv('WDR_LOGGING_LOGLEVEL', 'ERROR')
WDR_LOGGING_LOGFILE = os.getenv('WDR_LOGGING_LOGFILE', None)

WDR_DB_DEBUG = str2bool(os.getenv('WDR_DB_DEBUG', False))
WDR_DB_TYPE = os.getenv('WDR_DB_TYPE', None)
WDR_DB_HOST = os.getenv('WDR_DB_HOST', None)
WDR_DB_PORT = int(os.getenv('WDR_DB_PORT', 5432))
WDR_DB_USERNAME = os.getenv('WDR_DB_USERNAME', None)
WDR_DB_PASSWORD = os.getenv('WDR_DB_PASSWORD', None)
WDR_DB_NAME = os.getenv('WDR_DB_NAME', None)
WDR_SEARCH_TYPE = os.getenv('WDR_SEARCH_TYPE', 'elasticsearch')
WDR_SEARCH_INDEX_BASENAME = os.getenv('WDR_SEARCH_INDEX_BASENAME',
                                      'woudc-data-registry')
WDR_SEARCH_URL = os.getenv('WDR_SEARCH_URL', None)
WDR_WAF_BASEDIR = os.getenv('WDR_WAF_BASEDIR', None)
WDR_WAF_BASEURL = os.getenv('WDR_WAF_BASEURL', 'https://woudc.org/archive')
WDR_TABLE_SCHEMA = os.getenv('WDR_TABLE_SCHEMA', None)
WDR_TABLE_CONFIG = os.getenv('WDR_TABLE_CONFIG', None)
WDR_ERROR_CONFIG = os.getenv('WDR_ERROR_CONFIG', None)
WDR_ALIAS_CONFIG = os.getenv('WDR_ALIAS_CONFIG', None)

if WDR_DB_TYPE is None:
    msg = 'WDR_DB_TYPE is not set!'
    LOGGER.error(msg)
    raise EnvironmentError(msg)

if WDR_DB_TYPE == 'sqlite':
    if WDR_DB_NAME is None:
        msg = 'WDR_DB_NAME e is not set!'
        LOGGER.error(msg)
        raise EnvironmentError(msg)
    WDR_DATABASE_URL = '{}:///{}'.format(WDR_DB_TYPE, WDR_DB_NAME)
else:
    if None in [WDR_DB_USERNAME, WDR_DB_PASSWORD, WDR_SEARCH_TYPE,
                WDR_SEARCH_URL, WDR_WAF_BASEDIR, WDR_WAF_BASEURL]:
        msg = 'System environment variables are not set!'
        LOGGER.error(msg)
        raise EnvironmentError(msg)

    WDR_DATABASE_URL = '{}://{}:{}@{}:{}/{}'.format(WDR_DB_TYPE,
                                                    WDR_DB_USERNAME,
                                                    WDR_DB_PASSWORD,
                                                    WDR_DB_HOST,
                                                    WDR_DB_PORT,
                                                    WDR_DB_NAME)

if None in [WDR_TABLE_SCHEMA, WDR_TABLE_CONFIG, WDR_ERROR_CONFIG]:
    msg = 'Central configuration environment variables are not set!'
    LOGGER.error(msg)
    raise EnvironmentError(msg)
