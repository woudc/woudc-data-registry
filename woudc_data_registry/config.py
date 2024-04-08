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
import os

import yaml

from woudc_data_registry.util import str2bool

LOGGER = logging.getLogger(__name__)

WDR_LOGGING_LOGLEVEL = os.getenv('WDR_LOGGING_LOGLEVEL', 'ERROR')
WDR_LOGGING_LOGFILE = os.getenv('WDR_LOGGING_LOGFILE')

WDR_DB_DEBUG = str2bool(os.getenv('WDR_DB_DEBUG', False))
WDR_DB_TYPE = os.getenv('WDR_DB_TYPE')
WDR_DB_HOST = os.getenv('WDR_DB_HOST')
WDR_DB_PORT = int(os.getenv('WDR_DB_PORT', 5432))
WDR_DB_USERNAME = os.getenv('WDR_DB_USERNAME')
WDR_DB_PASSWORD = os.getenv('WDR_DB_PASSWORD')
WDR_DB_NAME = os.getenv('WDR_DB_NAME')
WDR_SEARCH_TYPE = os.getenv('WDR_SEARCH_TYPE', 'elasticsearch')
WDR_SEARCH_URL = os.getenv('WDR_SEARCH_URL')
WDR_WAF_BASEDIR = os.getenv('WDR_WAF_BASEDIR')
WDR_WAF_BASEURL = os.getenv('WDR_WAF_BASEURL', 'https://woudc.org/archive')
WDR_ERROR_CONFIG = os.getenv('WDR_ERROR_CONFIG')
WDR_ALIAS_CONFIG = os.getenv('WDR_ALIAS_CONFIG')
WDR_EXTRA_CONFIG = os.getenv('WDR_EXTRA_CONFIG')
WDR_UV_INDEX_FORMULA_LOOKUP = os.getenv('WDR_UV_INDEX_FORMULA_LOOKUP')

if WDR_SEARCH_URL is not None:
    WDR_SEARCH_URL = WDR_SEARCH_URL.rstrip('/')

if WDR_WAF_BASEURL is not None:
    WDR_WAF_BASEURL = WDR_WAF_BASEURL.rstrip('/')

if WDR_DB_TYPE is None:
    msg = 'WDR_DB_TYPE is not set!'
    LOGGER.error(msg)
    raise EnvironmentError(msg)

if WDR_DB_TYPE == 'sqlite':
    if WDR_DB_NAME is None:
        msg = 'WDR_DB_NAME e is not set!'
        LOGGER.error(msg)
        raise EnvironmentError(msg)
    WDR_DATABASE_URL = f'{WDR_DB_TYPE}:///{WDR_DB_NAME}'
else:
    if None in [WDR_DB_USERNAME, WDR_DB_PASSWORD, WDR_SEARCH_TYPE,
                WDR_SEARCH_URL, WDR_WAF_BASEDIR, WDR_WAF_BASEURL]:
        msg = 'System environment variables are not set!'
        LOGGER.error(msg)
        raise EnvironmentError(msg)

    auth = f'{WDR_DB_USERNAME}:{WDR_DB_PASSWORD}'
    host_port_name = f'{WDR_DB_HOST}:{WDR_DB_PORT}/{WDR_DB_NAME}'

    WDR_DATABASE_URL = f'{WDR_DB_TYPE}://{auth}@{host_port_name}'

if None in [WDR_ERROR_CONFIG, WDR_EXTRA_CONFIG]:
    msg = 'Central configuration environment variables are not set!'
    LOGGER.error(msg)
    raise EnvironmentError(msg)


try:
    with open(WDR_EXTRA_CONFIG) as extra_config_file:
        EXTRAS = yaml.safe_load(extra_config_file)
except Exception as err:
    msg = f'Failed to read extra configurations file: {err}'
    LOGGER.error(msg)
    raise EnvironmentError(msg)
