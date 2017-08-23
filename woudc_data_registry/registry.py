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

from sqlalchemy import create_engine
from sqlalchemy.exc import DataError
from sqlalchemy.orm import sessionmaker

from woudc_data_registry import config

LOGGER = logging.getLogger(__name__)


class Registry(object):
    """registry"""

    def __init__(self):
        """constructor"""

        engine = create_engine(config.DATABASE_URL, echo=config.DEBUG)
        Session = sessionmaker(bind=engine, expire_on_commit=False)
        self.session = Session()

    def query_distinct(self, domain):
        """queries for distinct values"""

        values = [v[0] for v in self.session.query(domain).distinct()]

        return values

    def query_by_field(self, obj, obj_instance, by):
        """query data by field"""

        field = getattr(obj, by)
        value = getattr(obj_instance, by)

        results = self.session.query(obj).filter(field == value).all()

        return results

    def save(self, obj):
        """helper function to save object to registry"""

        try:
            self.session.add(obj)
            # self.session.merge(obj)
            self.session.commit()
            self.session.close()
        except DataError as err:
            LOGGER.error('Failed to save to registry: {}'.format(err))
            self.session.rollback()
