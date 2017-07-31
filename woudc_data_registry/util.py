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

import logging
import io

LOGGER = logging.getLogger(__name__)


def point2ewkt(x, y, z=None, srid=4326):
    """helper function to generate EWKT of point"""

    if z is None:
        point = 'SRID={};POINT({} {})'.format(srid, x, y)
    else:
        point = 'SRID={};POINTZ({} {} {})'.format(srid, x, y, z)

    return point


def read_file(filename, encoding='utf-8'):
    """read file contents"""

    LOGGER.debug('Reading file %s (encoding %s)', filename, encoding)

    try:
        with io.open(filename, encoding=encoding) as fh:
            return fh.read().strip()
    except UnicodeDecodeError as err:
        LOGGER.warning('utf-8 decoding failed.  Trying latin-1')
        with io.open(filename, encoding='latin-1') as fh:
            return fh.read().strip()


def str2bool(value):
    """
    helper function to return Python boolean
    type (source: https://stackoverflow.com/a/715468)
    """

    value2 = False

    if isinstance(value, bool):
        value2 = value
    else:
        value2 = value.lower() in ('yes', 'true', 't', '1')

    return value2


def is_text_file(file_):
    """detect if file is of type text"""

    with open(file_, 'rb') as ff:
        data = ff.read(1024)

    return not is_binary_string(data)


def is_binary_string(string_):
    """
    detect if string is binary (https://stackoverflow.com/a/7392391)
    """

    if isinstance(string_, str):
        string_ = bytes(string_, 'utf-8')

    textchars = (bytearray({7, 8, 9, 10, 12, 13, 27} |
                 set(range(0x20, 0x100)) - {0x7f}))
    return bool(string_.translate(None, textchars))
