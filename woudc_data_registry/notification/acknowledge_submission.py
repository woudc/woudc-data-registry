#!/usr/bin/env python
# =================================================================
#
# Terms and Conditions of Use
#
# Unless otherwise noted, computer program source code of this
# distribution is covered under Crown Copyright, Government of
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

import datetime
import logging
import sys
import ftputil
from woudc_data_registry import config
from woudc_data_registry.util import str2bool, send_email
from woudc_data_registry.registry import Registry
from woudc_data_registry.models import Contributor

LOGGER = logging.getLogger(__name__)
FORMATTER = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
SKIP_DIRS = [
    "woudcadmin", "level-0", "org1", "org2", "provisional",
    "calibration", "px-testing", "px-testing2"
]
ACKNOWLEDGE_SUBMISSION_HOURS = config.WDR_ACKNOWLEDGE_SUBMISSION_HOURS
NOW = datetime.datetime.now()
ACKNOWLEDGE_SUBMISSION_ONLY = False
WDR_ACKNOWLEDGE_TEMPLATE_PATH = config.WDR_ACKNOWLEDGE_TEMPLATE_PATH


def days2hours(days):
    """helper function to convert days to hours"""
    LOGGER.debug('Converting %s days into hours', days)
    hours = 24 * int(days)
    LOGGER.debug('Result: %s days = %d hours', days, hours)
    return hours


def dt2iso8601(dto):
    """helper function to convert datetime to ISO8601"""
    return dto.strftime('%Y-%m-%dT%H:%M:%S')


def map_files(acknowledged):
    """helper function to map contributors to files"""

    contributor_dict = {}
    for item in acknowledged:
        if item['contributor'] not in contributor_dict:
            contributor_dict[item['contributor']] = []
        contributor_dict[item['contributor']].append(
            item['ipath'].split('/')[-1]
        )
    return contributor_dict


def send_ack_emails(contributor_dict):
    """helper function to send acknowledgement email to each contributor"""

    test_email = config.WDR_EMAIL_TEST
    msg_template = None

    try:
        agency_emails = _get_agency_emails()
    except Exception as err:
        msg = (
            'Unable to get agency emails from archive due to: %s. '
            'Skipping notifications.' % str(err)
        )
        msg = (
            'Unable to get agency emails from archive due to: %s. '
            'Skipping notifications.' % str(err)
        )
        LOGGER.error(msg)
        raise err

    host = config.WDR_EMAIL_HOST
    port = config.WDR_EMAIL_PORT
    fromaddr = config.WDR_EMAIL_FROM_USERNAME
    password = config.WDR_EMAIL_FROM_PASSWORD

    try:
        ccs = config.WDR_EMAIL_CC.split(',')
    except Exception:
        ccs = []
    try:
        bccs = config.WDR_EMAIL_BCC.split(',')
    except Exception:
        bccs = []

    for contributor in contributor_dict:
        with open(WDR_ACKNOWLEDGE_TEMPLATE_PATH) as fp:
            msg_template = fp.read()
        msg_template = msg_template.replace(
            '$FILES', '\n'.join(contributor_dict[contributor])
        ).replace(
            '$DATE', datetime.datetime.today().strftime('%Y-%m-%d')
        )
        subject = 'WOUDC data submission confirmation (%s)' % (contributor)
        if not str2bool(test_email):
            try:
                toaddrs = agency_emails[contributor]
            except Exception:
                msg = (
                    'Unable to get email for agency: %s. Skipping.'
                    % contributor
                )
                LOGGER.error(msg)
                continue
        else:
            LOGGER.debug(
                'Test mode on. Agency emails from %s would be sent to: %s',
                contributor,
                agency_emails[contributor]
            )
            toaddrs = config.WDR_EMAIL_TO.split(',')
            subject = 'TEST: ' + subject
        try:
            send_email(
                msg_template,
                subject,
                fromaddr,
                toaddrs,
                host,
                port,
                ccs,
                bccs,
                True,
                password
            )
            LOGGER.info(
                'Email sent to: %s using email %s.', contributor, toaddrs
            )
        except Exception as err:
            msg = (
                'Unable to send email to: %s. Due to: %s'
                % (contributor, str(err))
            )
            LOGGER.error(msg)
            continue


def acknowledge_submission(
    host, username, password, basedir_incoming,
    skip_dirs, now, acknowledge_submission_hours,
    send_email=False
):
    """acknowledge submission of candidate files from incoming FTP"""
    acknowledged = []

    with ftputil.FTPHost(host, username, password) as ftp:
        LOGGER.info('Connected to %s', host)
        LOGGER.info('Skip directories: %s', skip_dirs)
        for root, dirs, files in ftp.walk(basedir_incoming, topdown=True):
            LOGGER.debug('Removing skip directories: %s', skip_dirs)
            dirs[:] = [d for d in dirs if d not in skip_dirs]
            for name in files:
                ipath = '%s/%s' % (root, name)
                mtime = datetime.datetime.fromtimestamp(ftp.stat(ipath)[8])
                LOGGER.debug(
                    "Assessing whether to acknowledge submission"
                )
                LOGGER.debug(
                    'Timestamp of file %s: %s',
                    ipath, dt2iso8601(mtime)
                )
                LOGGER.debug(
                    'Timestamp now : %s', dt2iso8601(now)
                )

                delta = now - mtime
                LOGGER.debug('Raw delta: %r', delta)
                delta_hours = days2hours(delta.days)
                LOGGER.debug('Delta in hours: %s', delta_hours)

                if delta_hours < int(acknowledge_submission_hours):
                    LOGGER.info(
                        'File was submitted less than %s hours ago',
                        acknowledge_submission_hours
                    )
                    LOGGER.info(
                        'Adding file %s to processing queue (mtime: %s)',
                        ipath, mtime
                    )
                    LOGGER.info(
                        'File was submitted less than %s hours ago',
                        acknowledge_submission_hours
                    )
                    LOGGER.info(
                        'Adding file %s to processing queue (mtime: %s)',
                        ipath, mtime
                    )
                    acknowledged.append({
                        'contributor': ipath.lstrip('/').split('/')[0],
                        'ipath': ipath,
                        'mtime': mtime,
                    })

    LOGGER.info('%s new files acknowledged', len(acknowledged))

    if send_email:
        contributor_dict = map_files(acknowledged)
        send_ack_emails(contributor_dict)

    return acknowledged


def _get_agency_emails():
    """
    Get agency/ftpdir email from archive + username
    """
    registry = Registry()
    model = Contributor
    try:
        contributor_emails = registry.session.query(
            model.email, model.ftp_username
        )
        emails = {
            ftp_username: email
            for email, ftp_username in contributor_emails
        }

    except Exception as err:
        msg = (
            'Unable to connect to archive database. Due to: %s. Exiting'
            % str(err)
        )
        LOGGER.critical(msg)
        raise err

    registry.close_session()
    return emails
    return emails


if __name__ == "__main__":
    if len(sys.argv) == 2:
        if sys.argv[1] == 'acknowledge_submission':
            ACKNOWLEDGE_SUBMISSION_ONLY = True
            LOGGER.info(
                'Acknowledge submission Gather only mode detected, '
                'no processing or publishing enabled'
            )

    WDR_FTP_HOST = config.WDR_FTP_HOST
    WDR_FTP_USER = config.WDR_FTP_USER
    WDR_FTP_PASS = config.WDR_FTP_PASS
    WDR_FTP_BASEDIR_INCOMING = config.WDR_FTP_BASEDIR_INCOMING

    if ACKNOWLEDGE_SUBMISSION_ONLY:
        LOGGER.info('Acknowledging files')
        FILES_GATHERED = acknowledge_submission(
            WDR_FTP_HOST, WDR_FTP_USER, WDR_FTP_PASS,
            WDR_FTP_BASEDIR_INCOMING, SKIP_DIRS, NOW,
            ACKNOWLEDGE_SUBMISSION_HOURS, True
        )
        sys.exit(0)
