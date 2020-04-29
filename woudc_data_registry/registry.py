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

import logging
import re

from sqlalchemy import func, create_engine
from sqlalchemy.exc import DataError, SQLAlchemyError
from sqlalchemy.orm import sessionmaker

from woudc_data_registry import config

LOGGER = logging.getLogger(__name__)


class Registry(object):
    """registry"""

    def __init__(self):
        """constructor"""

        LOGGER.debug('Creating SQLAlchemy connection')
        engine = create_engine(config.WDR_DATABASE_URL,
                               echo=config.WDR_DB_DEBUG)
        Session = sessionmaker(bind=engine, expire_on_commit=False)
        self.session = Session()

    def query_full_index(self, domain):
        """
        Queries for the entire contents of the index of model class <domain>.

        :param domain: A model class.
        :returns: List of all objects of that class in the registry.
        """

        LOGGER.debug('Querying all records for {}'.format(domain))
        values = self.session.query(domain).all()

        return values

    def query_distinct(self, domain):
        """
        queries for distinct values

        :param domain: domain to be queried
        :returns: list of distinct values
        """

        LOGGER.debug('Querying distinct values for {}'.format(domain))
        values = [v[0] for v in self.session.query(domain).distinct()]

        return values

    def query_by_field(self, obj, by, value, case_insensitive=False):
        """
        query data by field

        :param obj: Object instance of the table to query in
        :param by: Field name to be queried
        :param value: Value of the field in any query results
        :param case_insensitive: `bool` of whether to query strings
                                 case-insensitively
        :returns: Single element of query results
        """

        field = getattr(obj, by)

        if case_insensitive:
            LOGGER.debug('Querying for LOWER({}) = LOWER({})'
                         .format(field, value))
            condition = func.lower(field) == value.lower()
        else:
            LOGGER.debug('Querying for {} = {}'.format(field, value))
            condition = field == value

        return self.session.query(obj).filter(condition).first()

    def query_by_pattern(self, obj, by, pattern, case_insensitive=False):
        """
        Query data using a single field's value, matching results based on
        a regex pattern.

        :param obj: Class of table to query in.
        :param by: Field name to be queried.
        :param pattern: Wildcard pattern that any result's value must match.
        :param case_insensitive: `bool` of whether to query strings
                                 case-insensitively.
        :returns: One element of query results.
        """

        # Change regular expression notation to SQL notation.
        pattern = pattern.replace('.*', '%')
        pattern = pattern.replace('.+', '_%')
        pattern = re.sub(r'(?<!\\)\.', '_', pattern)
        pattern = pattern.replace(r'\.', '.')

        field = getattr(obj, by)

        if case_insensitive:
            LOGGER.debug('Querying for LOWER({}) LIKE {}'
                         .format(field, pattern.lower()))
            condition = func.lower(field).like(pattern.lower())
        else:
            LOGGER.debug('Querying for {} LIKE {}'.format(field, pattern))
            condition = field.like(pattern)

        return self.session.query(obj).filter(condition).first()

    def query_multiple_fields(self, table, values, fields=None,
                              case_insensitive=()):
        """
        query a table by multiple fields

        :param table: table to be queried
        :param instance: dictionary with query values
        :param fields: fields to be filtered by
        :param case_insensitive: Collection of string fields that should be
                                 queried case-insensitively
        :returns: query results
        """

        conditions = []
        target_fields = fields or values.keys()

        for field in target_fields:
            table_field = getattr(table, field)
            if field in case_insensitive:
                condition = func.lower(table_field) == values[field].lower()
                conditions.append(condition)
            else:
                conditions.append(table_field == values[field])

        results = self.session.query(table).filter(*conditions).first()

        return results

    def save(self, obj=None):
        """
        helper function to save object to registry

        :param obj: object to save (defualt None)
        :returns: void
        """

        registry_config = config.EXTRAS.get('registry', {})

        try:
            if obj is not None:
                flag_name = '_'.join([obj.__tablename__, 'enabled'])
                if registry_config.get(flag_name, True):
                    self.session.add(obj)
                    # self.session.merge(obj)
                else:
                    LOGGER.info('Registry persistence for model {} disabled,'
                                ' skipping'.format(obj.__tablename__))
                    return

            LOGGER.debug('Saving')
            try:
                self.session.commit()
            except SQLAlchemyError as err:
                LOGGER.error('Failed to persist {} due to: {}'
                             .format(obj, err))
                self.session.rollback()
        except DataError as err:
            LOGGER.error('Failed to save to registry: {}'.format(err))
            self.session.rollback()

    def close_session(self):
        """Close the registry's database connection and resources"""

        self.session.close()
