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

import os

from woudc_data_registry.util import str2bool


WDR_DEBUG = str2bool(os.getenv('WDR_DEBUG', False))

WDR_DB_TYPE = os.getenv('WDR_DB_TYPE', 'postgresql')
WDR_DB_HOST = os.getenv('WDR_DB_HOST', 'localhost')
WDR_DB_PORT = int(os.getenv('WDR_DB_PORT', 5432))
WDR_DB_USERNAME = os.getenv('WDR_DB_USERNAME', None)
WDR_DB_PASSWORD = os.getenv('WDR_DB_PASSWORD', None)
WDR_DB_NAME = os.getenv('WDR_DB_NAME', 'woudc-data-registry')
WDR_SEARCH_TYPE = os.getenv('WDR_SEARCH_TYPE', 'elasticsearch')
WDR_SEARCH_URL = os.getenv('WDR_SEARCH_URL', 'elasticsearch')
WDR_WAF_BASEURL = os.getenv('WDR_WAF_BASEURL', 'https://woudc.org/archive')
WDR_WAF_BASEDIR = os.getenv('WDR_WAF_BASEDIR', None)

if None in [WDR_DB_USERNAME, WDR_DB_PASSWORD, WDR_SEARCH_TYPE,
            WDR_SEARCH_URL, WDR_WAF_BASEDIR]:
    raise EnvironmentError('System environment variables are not set!')

WDR_DATABASE_URL = '{}://{}:{}@{}:{}/{}'.format(WDR_DB_TYPE, WDR_DB_USERNAME,
                                                WDR_DB_PASSWORD, WDR_DB_HOST,
                                                WDR_DB_PORT, WDR_DB_NAME)
