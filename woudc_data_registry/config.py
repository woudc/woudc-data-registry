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
# Copyright (c) 2025 Government of Canada
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

import yaml

LOGGER = logging.getLogger(__name__)

WDR_LOGGING_LOGLEVEL = os.getenv('WDR_LOGGING_LOGLEVEL', 'ERROR')
WDR_LOGGING_LOGFILE = os.getenv('WDR_LOGGING_LOGFILE')
WDR_DB_DEBUG = str2bool(os.getenv('WDR_DB_DEBUG', 'False'))
WDR_DB_TYPE = os.getenv('WDR_DB_TYPE')
WDR_DB_HOST = os.getenv('WDR_DB_HOST')
WDR_DB_PORT = int(os.getenv('WDR_DB_PORT', 5432))
WDR_DB_USERNAME = os.getenv('WDR_DB_USERNAME')
WDR_DB_PASSWORD = os.getenv('WDR_DB_PASSWORD')
WDR_DB_NAME = os.getenv('WDR_DB_NAME')
WDR_SEARCH_TYPE = os.getenv('WDR_SEARCH_TYPE', 'elasticsearch')
WDR_SEARCH_URL = os.getenv('WDR_SEARCH_URL')
WDR_SEARCH_INDEX_BASENAME = os.getenv('WDR_SEARCH_INDEX_BASENAME')
WDR_SEARCH_CERT_VERIFY = str2bool(os.getenv('WDR_SEARCH_CERT_VERIFY', 'True'))
WDR_WAF_BASEDIR = os.getenv('WDR_WAF_BASEDIR')
WDR_WAF_BASEURL = os.getenv('WDR_WAF_BASEURL', 'https://woudc.org/archive')
WDR_ERROR_CONFIG = os.getenv('WDR_ERROR_CONFIG')
WDR_ALIAS_CONFIG = os.getenv('WDR_ALIAS_CONFIG')
WDR_EXTRA_CONFIG = os.getenv('WDR_EXTRA_CONFIG')
WDR_UV_INDEX_FORMULA_LOOKUP = os.getenv('WDR_UV_INDEX_FORMULA_LOOKUP')
WDR_EMAIL_HOST = os.getenv('WDR_EMAIL_HOST')
WDR_EMAIL_PORT = os.getenv('WDR_EMAIL_PORT')
WDR_EMAIL_SECURE = os.getenv('WDR_EMAIL_SECURE')
WDR_EMAIL_TEST = str2bool(os.getenv('WDR_EMAIL_TEST', 'True'))
WDR_EMAIL_FROM_USERNAME = os.getenv('WDR_EMAIL_FROM_USERNAME')
WDR_EMAIL_FROM_PASSWORD = os.getenv('WDR_EMAIL_FROM_PASSWORD')
WDR_EMAIL_TO = os.getenv('WDR_EMAIL_TO')
WDR_EMAIL_CC = os.getenv('WDR_EMAIL_CC')
WDR_EMAIL_BCC = os.getenv('WDR_EMAIL_BCC')
WDR_FEEDBACK_TEMPLATE_PATH = os.getenv('WDR_FEEDBACK_TEMPLATE_PATH')
WDR_FILE_TRASH = os.getenv('WDR_FILE_TRASH')
WDR_FTP_HOST = os.getenv('WDR_FTP_HOST')
WDR_FTP_USER = os.getenv('WDR_FTP_USER')
WDR_FTP_PASS = os.getenv('WDR_FTP_PASS')
WDR_FTP_BASEDIR_INCOMING = os.getenv('WDR_FTP_BASEDIR_INCOMING')
WDR_ACKNOWLEDGE_SUBMISSION_HOURS = os.getenv(
    'WDR_ACKNOWLEDGE_SUBMISSION_HOURS')
WDR_ACKNOWLEDGE_TEMPLATE_PATH = os.getenv('WDR_ACKNOWLEDGE_TEMPLATE_PATH')
WDR_FTP_SKIP_DIRS_INCOMING = os.getenv('WDR_FTP_SKIP_DIRS_INCOMING')
WDR_FTP_KEEP_FILES = str2bool(os.getenv('WDR_FTP_KEEP_FILES', 'True'))
WDR_NOTIFICATION_MESSAGE = os.getenv('WDR_NOTIFICATION_MESSAGE')
WDR_MQTT_BROKER_HOST = os.getenv('WDR_MQTT_BROKER_HOST')
WDR_MQTT_BROKER_PORT = os.getenv('WDR_MQTT_BROKER_PORT')
WDR_MQTT_BROKER_USERNAME = os.getenv('WDR_MQTT_BROKER_USERNAME')
WDR_MQTT_BROKER_PASSWORD = os.getenv('WDR_MQTT_BROKER_PASSWORD')
WDR_MQTT_CLIENT_ID = os.getenv('WDR_MQTT_CLIENT_ID')
WDR_WOUDC_STATION_TEMIS_COMBINED = os.getenv(
    'WDR_WOUDC_STATION_TEMIS_COMBINED')
WDR_DOBSON_CORRECTION_OUTPUT = os.getenv(
    'WDR_DOBSON_CORRECTION_OUTPUT')

if not WDR_SEARCH_INDEX_BASENAME:
    msg = 'WDR_SEARCH_INDEX_BASENAME was not set. \
        Defaulting to: woudc_data_registry'
    LOGGER.warning(msg)
    WDR_SEARCH_INDEX_BASENAME = 'woudc_data_registry'

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
