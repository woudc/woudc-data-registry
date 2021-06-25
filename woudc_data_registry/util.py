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

from datetime import date, datetime, time
import logging
import io

LOGGER = logging.getLogger(__name__)

RFC3339_DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%SZ'


def point2geojsongeometry(x, y, z=None):
    """
    helper function to generate GeoJSON geometry of point

    :param x: x coordinate
    :param y: y coordinate
    :param z: y coordinate (default=None)
    :returns: `dict` of GeoJSON geometry
    """

    if z is None or int(z) == 0:
        LOGGER.debug('Point has no z property')
        coordinates = [x, y]
    else:
        LOGGER.debug('Point has z property')
        coordinates = [x, y, z]

    if None in coordinates:
        return None

    geometry = {
        'type': 'Point',
        'coordinates': coordinates
    }

    return geometry


def read_file(filename, encoding='utf-8'):
    """
    read file contents

    :param filename: filename
    :param encoding: encoding (default=utf-8)
    :returns: buffer of file contents
    """

    LOGGER.debug('Reading file {} (encoding {})'.format(filename, encoding))

    try:
        with io.open(filename, encoding=encoding) as fh:
            return fh.read().strip()
    except UnicodeDecodeError as err:
        LOGGER.warning('utf-8 decoding failed: {}'.format(err))
        LOGGER.info('Trying latin-1')
        with io.open(filename, encoding='latin-1') as fh:
            return fh.read().strip()


def str2bool(value):
    """
    helper function to return Python boolean
    type (source: https://stackoverflow.com/a/715468)

    :param value: value to be evaluated
    :returns: `bool` of whether the value is boolean-ish
    """

    value2 = False

    if isinstance(value, bool):
        value2 = value
    else:
        value2 = value.lower() in ('yes', 'true', 't', '1', 'on')

    return value2


def parse_integer_range(bounds_string):
    """
    Returns an integer lower bound and upper bound of the range defined within
    <bounds_string>. Formats accepted include 'n', 'n+', and 'm-n'.

    :param bounds_string: String representing a range of integers
    :return: Pair of integer lower bound and upper bound on the range
    """

    if bounds_string.endswith('+'):
        lower_bound = int(bounds_string[:-1])
        upper_bound = float('inf')
    elif bounds_string.count('-') == 1:
        lower_bound, upper_bound = map(int, bounds_string.split('-'))
    else:
        lower_bound = int(bounds_string)
        upper_bound = lower_bound

    return (lower_bound, upper_bound)


def strftime_rfc3339(datetimeobj=None):
    """
    Returns a version of <datetimeobj> as an RFC3339-formatted string
    (YYYY-MM-DD'T'HH:MM:SS'Z').

    :param datetimeobj: A datetime.datetime or datetime.date object.
    :returns: A string (or None) version of <datetimeobj> in RFC3339 format.
    """

    if datetimeobj is None:
        return None
    else:
        return datetimeobj.strftime(RFC3339_DATETIME_FORMAT)


def is_text_file(file_):
    """
    detect if file is of type text

    :param file_: file to be tested
    :returns: `bool` of whether the file is text
    """

    with open(file_, 'rb') as ff:
        data = ff.read(1024)

    return not is_binary_string(data)


def is_binary_string(string_):
    """
    detect if string is binary (https://stackoverflow.com/a/7392391)

    :param string_: `str` to be evaluated
    :returns: `bool` of whether the string is binary
    """

    if isinstance(string_, str):
        string_ = bytes(string_, 'utf-8')

    textchars = (bytearray({7, 8, 9, 10, 12, 13, 27} |
                 set(range(0x20, 0x100)) - {0x7f}))
    return bool(string_.translate(None, textchars))


def json_serial(obj):
    """
    helper function to convert to JSON non-default
    types (source: https://stackoverflow.com/a/22238613)

    :param obj: `object` to be evaluate
    :returns: JSON non-default type to `str`
    """

    if isinstance(obj, (datetime, date, time)):
        serial = obj.isoformat()
        return serial

    msg = '{} type {} not serializable'.format(obj, type(obj))
    LOGGER.error(msg)
    raise TypeError(msg)


def is_plural(value):
    """
    helps function to determine whether a value is plural or singular

    :param value: value to be evaluated
    :returns: `bool` of whether the value is plural
    """

    if int(value) == 1:
        return False
    else:
        return True


def get_date(date_, force_date=False):
    """
    helper function to evaluate/transform date objects or strings
    into datetime objects

    :param date_: date value (`str` or `datetime.date`)

    :returns: `datetime.date` representation of value
    """

    if isinstance(date_, date) or date_ is None:
        return date_
    else:
        if not force_date:
            return datetime.strptime(date_, RFC3339_DATETIME_FORMAT)
        else:
            return datetime.strptime(date_, '%Y-%m-%d').date()
