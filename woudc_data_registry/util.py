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

from datetime import date, datetime, time
import logging
import io
import os
import shutil
import smtplib

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from woudc_data_registry.registry import Registry
import woudc_data_registry.config as config

LOGGER = logging.getLogger(__name__)

RFC3339_DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%SZ'


def send_email(message, subject, from_email_address, to_email_addresses,
               host, port, cc_addresses=None, bcc_addresses=None, secure=False,
               from_email_password=None):
    """
    Send email

    :param message: body of the email
    :param subject: subject of the email
    :param from_email_address: email of the sender
    :param to_email_addresses: list of emails of the receipients
    :param host: host of SMTP server
    :param port: port on SMTP server
    :param cc_addresses: list of cc email addresses
    :param bcc_addresses: list of bcc email addresses
    :param secure: Turn on/off TLS
    :param from_email_password: password of sender, if TLS is turned on
    :returns: list of emailing statuses
    """
    try:
        server = smtplib.SMTP(host, port)
    except Exception as err:
        msg = 'Unable to establish connection to {}:{}'.format(host, port)
        LOGGER.critical(msg)
        raise err

    if all([secure, from_email_password is not None]):
        try:
            server.starttls()
        except Exception as err:
            msg = 'Unable to start TLS: {}'.format(err)
        try:
            server.login(from_email_address, from_email_password)
        except Exception as err:
            msg = 'Unable to login using username {}: {}'.format(
                from_email_address, err)

    send_statuses = []
    cc = False
    LOGGER.debug('cc: {}' .format(cc_addresses))
    # cc
    if all([
            cc_addresses is not None,
            cc_addresses != ['']
            ]):
        cc = True
    LOGGER.debug('bcc: {}' .format(bcc_addresses))
    bcc = False
    if all([
            bcc_addresses is not None,
            bcc_addresses != ['']
            ]):
        bcc = True

    LOGGER.debug('to_email: {}' .format(to_email_addresses))
    if isinstance(to_email_addresses, str):
        # checking whether to split by ';' or ','
        if ';' in to_email_addresses:
            to_email_addresses = to_email_addresses.split(';')
        else:
            to_email_addresses = to_email_addresses.split(',')

    # set up the message
    msg = MIMEMultipart()
    msg['From'] = from_email_address
    msg['To'] = ', '.join(to_email_addresses)
    if cc:
        msg['Cc'] = ', '.join(cc_addresses)  # Add CC addresses if they exist
    msg['Subject'] = subject
    msg.attach(MIMEText(message, 'plain'))

    # Convert the message to a string
    text = msg.as_string()
    LOGGER.debug('Message: {}' .format(text))

    # send message
    try:
        LOGGER.debug(
            'Sending report to {}'.format(to_email_addresses)
            )
        LOGGER.debug('cc_addresses: {}'.format(cc_addresses))
        recipients = to_email_addresses + cc_addresses
        if (bcc):  # Add BCC addresses if they exist
            recipients += bcc_addresses
            LOGGER.debug('bcc_addresses: {}'.format(bcc_addresses))

        send_status = server.sendmail(
            msg['From'], recipients, text)
        send_statuses.append(send_status)
    except Exception as err:
        error_msg = (
            'Unable to send mail from: {} to {}: {}'.format(
                msg['From'], msg['To'], err
            )
        )

        LOGGER.error(error_msg)
        raise err

    server.quit()


def delete_file_from_record(file_path, table):
    registry = Registry()

    filename = file_path.split('/')[-1]
    condition = {'filename': filename, 'output_filepath': file_path}

    result = registry.query_multiple_fields(table, condition)
    if not result:
        LOGGER.error(f'File {filename} or out_filepath {file_path} not \
                    found in {table} table')
        return

    try:
        # Remove the file from data_records
        registry.delete_by_multiple_fields(table, condition)

        LOGGER.info(f"Deleted file from {table} table")

        # Remove the file from WAF
        shutil.move(file_path, config.WDR_FILE_TRASH)

        # Check if the file now exists in the trash directory
        if filename in os.listdir(config.WDR_FILE_TRASH):
            LOGGER.info(f"File {filename} successfully moved to trash.")
        else:
            LOGGER.error(f"Failed to move {filename} to trash. \
                The file is not in {config.WDR_FILE_TRASH}.")
            raise Exception

        registry.session.commit()
    except Exception as err:
        LOGGER.error('Failed to delete file: {}'.format(err))
        registry.session.rollback()
    finally:
        registry.close_session()


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

    LOGGER.debug(f'Reading file {filename} (encoding {encoding})')

    try:
        with io.open(filename, encoding=encoding) as fh:
            return fh.read().strip()
    except UnicodeDecodeError as err:
        LOGGER.warning(f'utf-8 decoding failed: {err}')
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

    msg = f'{obj} type {type(obj)} not serializable'
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
