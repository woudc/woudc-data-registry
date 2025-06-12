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

import tarfile
import rarfile
import zipfile

import ftputil

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from woudc_data_registry.registry import Registry
import woudc_data_registry.config as config

LOGGER = logging.getLogger(__name__)

RFC3339_DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%SZ'


def gathering(host, username, password, basedir_incoming, skip_dirs,
              px_basedir, keep_files):
    """gather candidate files from incoming FTP"""

    gathered = []
    gathered_files_to_remove = []

    with ftputil.FTPHost(host, username, password) as ftp:
        LOGGER.info('Connected to %s', host)
        LOGGER.info('Skip directories: %s', skip_dirs)
        for root, dirs, files in ftp.walk(basedir_incoming, topdown=True):
            LOGGER.debug('Removing skip directories: %s', skip_dirs)
            dirs[:] = [d for d in dirs if d not in skip_dirs]
            for name in files:
                ipath = '%s/%s' % (root, name)
                mtime = datetime.fromtimestamp(ftp.stat(ipath)[8])
                fpath = '%s%s%s' % (px_basedir, os.sep, ipath)
                ipath_ftp_abs = '%s%s' % (host, ipath)

                LOGGER.info('Adding file %s to processing queue',
                            ipath_ftp_abs)
                gathered.append({
                    'contributor': ipath.lstrip('/').split('/')[0],
                    'ipath': ipath,
                    'ipath_ftp_abs': ipath_ftp_abs,
                    'mtime': mtime,
                    'fpath': os.path.normpath(fpath),
                    'decomp': False
                })

        LOGGER.info('%d files in processing queue', len(gathered))
        for gathered_file in gathered:
            LOGGER.info('Inspecting gathered file %s', gathered_file)
            if all([gathered_file['ipath'] is not None,
                    not gathered_file['decomp']]):
                LOGGER.info('File passed in')
                if ftp.path.isfile(gathered_file['ipath']):
                    try:
                        os.makedirs(os.path.dirname(gathered_file['fpath']))
                    except OSError as err:
                        LOGGER.warning('Local directory %s not created: %s',
                                       gathered_file['fpath'], err)

                    ftp.download(gathered_file['ipath'],
                                 gathered_file['fpath'])

                    LOGGER.info('Downloaded FTP file %s to %s',
                                gathered_file['ipath_ftp_abs'],
                                gathered_file['fpath'])

                    # handle compressed files here
                    if any([tarfile.is_tarfile(gathered_file['fpath']),
                            rarfile.is_rarfile(gathered_file['fpath']),
                            zipfile.is_zipfile(gathered_file['fpath'])]):
                        LOGGER.info('Decompressing file: %s',
                                    gathered_file['fpath'])
                        comm_ipath_ftp_abs = gathered_file['ipath_ftp_abs']
                        comm_fpath = gathered_file['fpath']
                        comm_mtime = gathered_file['mtime']
                        comm_dirname = os.path.dirname(comm_fpath)

                        # decompress
                        try:
                            files = decompress(gathered_file['fpath'])
                        except Exception as err:
                            LOGGER.error('Unable to decompress %s: %s',
                                         gathered_file['fpath'], err)
                            continue
                        LOGGER.info('Decompressed files: %s',
                                    ', '.join(files))

                        for fil in files:
                            fpath = os.path.join(comm_dirname, fil)
                            if os.path.isfile(fpath):
                                gathered.append({
                                    'contributor': gathered_file[
                                        'ipath'].lstrip('/').split('/')[0],
                                    'ipath': fpath,
                                    'ipath_ftp_abs': comm_ipath_ftp_abs,
                                    'mtime': comm_mtime,
                                    'fpath': os.path.normpath(fpath),
                                    'decomp': True
                                })
                        # remove from gathered
                        gathered_files_to_remove.append(gathered_file)

                    if not keep_files:
                        LOGGER.info('Removing file %s',
                                    gathered_file['ipath'])
                        ftp.remove(gathered_file['ipath'])

                else:
                    LOGGER.info('FTP file %s could not be downloaded',
                                gathered_file['ipath_ftp_abs'])

    LOGGER.info('%s archive files gathered to remove',
                len(gathered_files_to_remove))
    for gftr in gathered_files_to_remove:
        gathered.remove(gftr)
        LOGGER.info('Removed %s from gathered list', gftr)
    LOGGER.info('%s files gathered', len(gathered))

    return gathered


def decompress(ipath):
    """decompress compressed files"""

    file_list = []
    success = True

    LOGGER.debug('ipath: %s', ipath)

    if tarfile.is_tarfile(ipath):
        tar = tarfile.open(ipath)
        for item in tar:
            try:
                item.name = os.path.basename(item.name)
                file_list.append(item.name)
                tar.extract(item, os.path.dirname(ipath))
            except Exception as err:
                success = False
                LOGGER.error('Unable to decompress from tar %s: %s',
                             item.name, err)

    elif rarfile.is_rarfile(ipath):
        rar = rarfile.RarFile(ipath)
        for item in rar.infolist():
            try:
                item.filename = os.path.basename(item.filename)
                file_list.append(item.filename)
                rar.extract(item, os.path.dirname(ipath))
            except Exception as err:
                success = False
                LOGGER.error('Unable to decompress from rar %s: %s',
                             item.filename, err)

    elif zipfile.is_zipfile(ipath):
        zipf = zipfile.ZipFile(ipath)
        for item in zipf.infolist():
            if item.filename.endswith('/'):  # filename is dir, skip
                LOGGER.info('item %s is a directory, skipping', item.filename)
                continue
            try:
                item_filename = os.path.basename(item.filename)
                file_list.append(item_filename)
                data = zipf.read(item.filename)
                filename = '%s%s%s' % (os.path.dirname(ipath),
                                       os.sep, item_filename)
                LOGGER.info('Filename: %s', filename)
                with open(filename, 'wb') as ff:
                    ff.write(data)
                # zipf.extract(item.filename, os.path.dirname(ipath))
            except Exception as err:
                success = False
                LOGGER.error('Unable to decompress from zip %s: %s',
                             item.filename, err)
    else:
        msg = ('File %s is not a compressed file and will not be compressed.'
               % ipath)
        LOGGER.warning(msg)

    if success:  # delete the archive file
        LOGGER.info('Deleting archive file: %s', ipath)
        try:
            os.unlink(ipath)
        except Exception as err:
            LOGGER.error(err)
    else:
        LOGGER.info('Decompress failed, not deleting %s', ipath)

    return file_list


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
        to_email_addresses += cc_addresses
        cc = True
    LOGGER.debug('bcc: {}' .format(bcc_addresses))
    # bcc
    if all([
            bcc_addresses is not None,
            bcc_addresses != ['']
            ]):
        to_email_addresses += bcc_addresses

    LOGGER.debug('to_email: {}' .format(to_email_addresses))
    if isinstance(to_email_addresses, str):
        to_email_addresses = to_email_addresses.split(';')

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
        send_status = server.sendmail(
            msg['From'], to_email_addresses + cc_addresses, text)
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
