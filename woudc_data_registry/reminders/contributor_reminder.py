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

# Execute notifications

import logging
import argparse
from datetime import date, datetime
from woudc_data_registry.util import send_email, str2bool
from woudc_data_registry import config
from woudc_data_registry.controller import Registry
from woudc_data_registry.models import (
    Contributor,
    ContributorNotification,
    DataRecord
)
from dateutil.relativedelta import relativedelta

WDR_SUBMISSION_REMINDER_TEMPLATE_PATH = (
    config.WDR_SUBMISSION_REMINDER_TEMPLATE_PATH
)

WDR_REMIND_NOT_ACTIVE_FOR = config.WDR_REMIND_NOT_ACTIVE_FOR[1]
WDR_REMIND_NOT_ACTIVE_FOR = config.WDR_REMIND_NOT_ACTIVE_FOR[1]

LOGGER = logging.getLogger(__name__)

PARSER = argparse.ArgumentParser(
        description='Executes notification selection.'
    )

PARSER.add_argument(
    '--mode',
    required=True,
    help='Execution mode.',
    choices=(
        'test',
        'ops'
    )
)
ARGS = PARSER.parse_args()

# connect to db
registry = Registry()


def get_last_activity(acronym):
    """Check if the contributor has been active in the last 6 months."""
    # also check if the contributor has been active for the past 5 years
    # otherwise ignore them
    activities = registry.session.query(DataRecord.received_datetime).filter(
            DataRecord.data_generation_agency == acronym).all()
    if activities != []:
        last_activity = max(activities)
        LOGGER.info(f'Last activity query for {acronym}: {last_activity}')
        if (
            last_activity[0].date() < date.today() - relativedelta(months=int(
                WDR_REMIND_NOT_ACTIVE_FOR))
            and last_activity[0].date() > date.today() - relativedelta(
                years=int(WDR_REMIND_NOT_ACTIVE_FOR))
        ):
            # if the last activity is more than 6 months ago, return False
            LOGGER.info(f'Contributor {acronym} has not been active in the',
                        'last 6 months.')
            return (False, last_activity[0].date())
        return (True, last_activity[0].date())
    LOGGER.info(f'Contributor {acronym} has not submitted any files at all.')
    return (True, datetime.now().date())


# change this
def get_last_reminder(contributor, last_activity):
    """Check which number reminder a contributor has in the last 6 months."""

    # check for the most recent reminder by datetime column,
    # and compare the number
    # if this returns 0 rows then the contributor
    # has not received any reminders
    contributor_id = registry.session.query(Contributor.contributor_id).filter(
        Contributor.acronym == contributor
    )
    most_recent = registry.session.query(
        ContributorNotification.reminder_datetime,
        ContributorNotification.reminder_number
    ).filter(
        ContributorNotification.contributor_id == contributor_id
    )
    if most_recent.count() == 0:
        most_recent_number = 0
    elif last_activity > most_recent.order_by(
            ContributorNotification.reminder_datetime.desc()
            ).first()[0].date():
        # reset the reminder number
        most_recent_number = 0
    else:
        most_recent_number = most_recent.order_by(
            ContributorNotification.reminder_datetime.desc()
        ).first()[1]

    LOGGER.info(f'Contributor {contributor_id} has {most_recent_number}',
                'reminders.')
    return most_recent_number


def get_contributor_emails(registry):
    """Get all contributor emails."""
    model = Contributor
    contributor_emails = registry.session.query(
        model.email, model.acronym)
    emails = {
        acronym: email
        for email, acronym in contributor_emails
        }
    registry.close_session()
    return emails


def send_contributor_reminder(acronym, email):
    """Send contributor reminder emails."""

    # configs
    if ARGS.mode == 'test':
        test_email = True
    else:
        test_email = False
    # test_email = config.WDR_EMAIL_TEST
    msg_template = None
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

    subject = 'WOUDC data submission reminder (%s)' % (acronym)

    with open(WDR_SUBMISSION_REMINDER_TEMPLATE_PATH) as fp:
        msg_template = fp.read()

    if not str2bool(test_email):
        try:
            toaddrs = email.split(';')
        except Exception:
            msg = (
                'Unable to get email for agency: %s. Skipping.'
                % acronym
            )
            LOGGER.error(msg)
    else:
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
        timestamp = datetime.now()
        LOGGER.info(
            'Email sent to: %s using email %s.', acronym, toaddrs
        )
    except Exception as err:
        msg = (
            f"Unable to send email to: {acronym}. Due to: {err}"
        )
        LOGGER.error(msg)

    return timestamp


def insert_reminder(timestamp, acronym, reminder):
    """
    Insert a new reminder for the contributor into the database.
    """
    contributor_id = registry.session.query(Contributor.contributor_id).filter(
        Contributor.acronym == acronym
    ).first()[0]
    # insert a new reminder
    dict_ = {'contributor_id': contributor_id, 'reminder_number': reminder + 1,
             'reminder_datetime': timestamp, 'mode': ARGS.mode}
    LOGGER.info(f"Inserting this reminder: {dict_}")
    new_reminder = ContributorNotification(dict_)
    registry.session.add(new_reminder)
    registry.session.commit()
    LOGGER.info(f'Inserted new reminder for contributor: {acronym}, reminder',
                f'number: {new_reminder.reminder_number}')


if __name__ == '__main__':
    contributors = get_contributor_emails(registry)
    for acronym in contributors:
        active_status, last_activity = get_last_activity(acronym)
        if not active_status:
            reminder_number = get_last_reminder(acronym, last_activity)
            if reminder_number != 4:
                timestamp = send_contributor_reminder(acronym,
                                                      contributors[acronym])
                insert_reminder(timestamp, acronym, reminder_number)
            else:
                LOGGER.info(f'Contributor {acronym} has already received 4'
                            f'reminders. No further reminders will be sent.')

    registry.close_session()
