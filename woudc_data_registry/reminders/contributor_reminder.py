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
from sqlalchemy import func
from woudc_data_registry.util import send_email, str2bool
from woudc_data_registry import config
from woudc_data_registry.registry import Registry
from woudc_data_registry.models import (
    Contributor,
    ContributorNotification,
    DataRecord
)
from dateutil.relativedelta import relativedelta

WDR_REMIND_TEMPLATE_PATH = (
    config.WDR_REMIND_TEMPLATE_PATH
)

WDR_REMIND_NOT_ACTIVE_FOR_VALUE = int(config.WDR_REMIND_NOT_ACTIVE_FOR[1:][:-1]
                                      )
WDR_REMIND_NOT_ACTIVE_FOR_DESIGNATOR = config.WDR_REMIND_NOT_ACTIVE_FOR[-1]
WDR_REMIND_ACTIVE_WITHIN_VALUE = int(config.WDR_REMIND_ACTIVE_WITHIN[1:][:-1])
WDR_REMIND_ACTIVE_WITHIN_DESIGNATOR = config.WDR_REMIND_ACTIVE_WITHIN[-1]
WDR_REMIND_MAX_TIMES = int(config.WDR_REMIND_MAX_TIMES)

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


def iso_to_delta(value, designator):
    """
    take an ISO config value and return its relative delta value

    ex: value = 4, designator = M

    return relative_delta(month=4)
    """
    if designator.lower() == 'm':
        return relativedelta(months=value)
    elif designator.lower() == 'y':
        return relativedelta(years=value)
    elif designator.lower() == 'd':
        return relativedelta(days=value)
    else:
        return None


# Define reminder date thresholds
NOT_ACTIVE_SINCE_DATE = date.today() - iso_to_delta(
    WDR_REMIND_NOT_ACTIVE_FOR_VALUE,
    WDR_REMIND_NOT_ACTIVE_FOR_DESIGNATOR
)
LOGGER.debug(f"NOT_ACTIVE_SINCE_DATE threshold: {NOT_ACTIVE_SINCE_DATE}")
ACTIVE_WITHIN_DATE = date.today() - iso_to_delta(
    WDR_REMIND_ACTIVE_WITHIN_VALUE,
    WDR_REMIND_ACTIVE_WITHIN_DESIGNATOR
)
LOGGER.debug(f"ACTIVE_WITHIN_DATE threshold: {ACTIVE_WITHIN_DATE}")


def get_last_activity(acronym):
    """Check if the contributor has been active in the last 6 months."""

    # Use a single query to get the maximum date, which is more efficient
    last_activity_datetime = (
        registry.session.query(func.max(DataRecord.received_datetime))
        .filter(DataRecord.data_generation_agency == acronym)
        .scalar()
    )

    if not last_activity_datetime:
        # Log a more specific message if no activity is found.
        LOGGER.info(f"Contributor '{acronym}' has no submitted files.")
        # Return a neutral result since no activity means no recent activity.
        # Todo: compare with Contributor.last_validated_datetime for this case
        # to avoid sending reminder emails to newly registered contributors
        return (False, datetime.now().date())

    last_activity_date = last_activity_datetime.date()
    LOGGER.debug(f"Last activity for contributor '{acronym}' was on "
                 f"{last_activity_date}.")
    if (
        last_activity_date < NOT_ACTIVE_SINCE_DATE
        and last_activity_date > ACTIVE_WITHIN_DATE
    ):
        LOGGER.info(f"Contributor '{acronym}' has not been active in the last "
                    "6 months.")
        return (False, last_activity_date)
    else:
        LOGGER.info(f"Contributor '{acronym}' has been active recently.")
        return (True, last_activity_date)


def get_last_reminder(contributor_acronym, last_activity):
    """Check which number reminder a contributor has in the last 6 months."""
    most_recent_notification = (
        registry.session.query(
            ContributorNotification.reminder_datetime,
            ContributorNotification.reminder_number
        )
        .join(Contributor,
              Contributor.contributor_id ==
              ContributorNotification.contributor_id)
        .filter(Contributor.acronym == contributor_acronym)
        .order_by(ContributorNotification.contributor_notification_id.desc())
        .first()
    )
    LOGGER.debug(f"{contributor_acronym}: {most_recent_notification}")

    if not most_recent_notification:
        most_recent_number = 0
    elif last_activity > most_recent_notification[0].date():
        # reset the reminder number because of recent activity
        most_recent_number = 0
    else:
        most_recent_number = most_recent_notification[1]

    LOGGER.info(f"Contributor {contributor_acronym} has {most_recent_number} "
                "reminders.")
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

    with open(WDR_REMIND_TEMPLATE_PATH) as fp:
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
    contributor_id = (
        registry.session.query(Contributor.contributor_id)
        .filter(Contributor.acronym == acronym)
        .first()[0]
    )
    # insert a new reminder
    dict_ = {'contributor_id': contributor_id, 'reminder_number': reminder + 1,
             'reminder_datetime': timestamp, 'mode': ARGS.mode}
    LOGGER.info(f"Inserting this reminder: {dict_}")
    new_reminder = ContributorNotification(dict_)
    registry.session.add(new_reminder)
    registry.session.commit()
    LOGGER.info(f"Inserted new reminder for contributor: {acronym}, reminder "
                f"number: {new_reminder.reminder_number}")


if __name__ == '__main__':
    contributors = get_contributor_emails(registry)
    for acronym in contributors:
        active_status, last_activity = get_last_activity(acronym)
        if not active_status:
            reminder_number = get_last_reminder(acronym, last_activity)
            if reminder_number + 1 <= WDR_REMIND_MAX_TIMES:
                timestamp = send_contributor_reminder(acronym,
                                                      contributors[acronym])
                insert_reminder(timestamp, acronym, reminder_number)
            else:
                LOGGER.info(
                    f"Contributor {acronym} has already received "
                    f"{WDR_REMIND_MAX_TIMES} reminders. No further "
                    "reminders will be sent."
                )

    registry.close_session()
