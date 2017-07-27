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

from datetime import datetime
import click
import logging

from geoalchemy2 import Geometry
from sqlalchemy import (Column, create_engine, Date, DateTime, Integer, String,
                        Time, UnicodeText)
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy.ext.declarative import declarative_base

from woudc_data_registry import util

LOGGER = logging.getLogger(__name__)
base = declarative_base()


class DataRecord(base):
    """Data Registry Data Record"""

    __tablename__ = 'data_record'

    identifier = Column(Integer, primary_key=True, autoincrement=True)

    # Extended CSV core fields

    content_class = Column(String, nullable=False)
    content_category = Column(String, nullable=False)
    content_level = Column(String, nullable=False)
    content_form = Column(String, nullable=False)
    data_generation_date = Column(Date, nullable=False)
    data_generation_agency = Column(String, nullable=False)
    data_generation_version = Column(String, nullable=False)
    data_generation_scientific_authority = Column(String)
    platform_type = Column(String, default='STN', nullable=False)
    platform_id = Column(String, nullable=False)
    platform_name = Column(String, nullable=False)
    platform_country = Column(String, nullable=False)
    platform_gaw_id = Column(String)
    instrument_name = Column(String, nullable=False)
    instrument_model = Column(String, nullable=False)
    instrument_number = Column(String, nullable=False)
    location = Column(Geometry(management=True, use_typemod=False, srid=4326))
    timestamp_utcoffset = Column(String, nullable=False)
    timestamp_date = Column(Date, nullable=False)
    timestamp_time = Column(Time)

    # data management fields

    insert_datetime = Column(DateTime, nullable=False,
                             default=datetime.utcnow())
    processed_datetime = Column(DateTime, nullable=False,
                                default=datetime.utcnow())
    raw = Column(UnicodeText, nullable=False)
    url = Column(String, nullable=False)

    def __init__(self, ecsv):
        """serializer"""

        LOGGER.debug('Serializing model')
        self.content_class = ecsv.extcsv['CONTENT']['Class']
        self.content_category = ecsv.extcsv['CONTENT']['Category']
        self.content_level = ecsv.extcsv['CONTENT']['Level']
        self.content_form = ecsv.extcsv['CONTENT']['Form']

        self.data_generation_date = ecsv.extcsv['DATA_GENERATION']['Date']
        self.data_generation_agency = ecsv.extcsv['DATA_GENERATION']['Agency']
        self.data_generation_version = \
            ecsv.extcsv['DATA_GENERATION']['Version']
        self.data_generation_scientific_authority = \
            ecsv.extcsv['DATA_GENERATION']['ScientificAuthority']

        self.platform_type = ecsv.extcsv['PLATFORM']['Type']
        self.platform_id = ecsv.extcsv['PLATFORM']['ID']
        self.platform_name = ecsv.extcsv['PLATFORM']['Name']
        self.platform_country = ecsv.extcsv['PLATFORM']['Country']
        self.platform_gaw_id = ecsv.extcsv['PLATFORM']['GAW_ID']

        self.instrument_name = ecsv.extcsv['INSTRUMENT']['Name']
        self.instrument_model = ecsv.extcsv['INSTRUMENT']['Model']
        self.instrument_number = ecsv.extcsv['INSTRUMENT']['Number']

        self.timestamp_utcoffset = ecsv.extcsv['TIMESTAMP']['UTCOffset']
        self.timestamp_date = ecsv.extcsv['TIMESTAMP']['Date']
        self.timestamp_time = ecsv.extcsv['TIMESTAMP']['Time']

        self.location = util.point2wkt(ecsv.extcsv['LOCATION']['Longitude'],
                                       ecsv.extcsv['LOCATION']['Latitude'],
                                       ecsv.extcsv['LOCATION']['Height'])
        self.extcsv = ecsv.extcsv
        self.raw = ecsv._raw

    def __repr__(self):
        return 'DataRecord(%r, %r)' % (self.identifier, self.url)


@click.command()
@click.pass_context
def setup_models(ctx):
    """create models"""

    from woudc_data_registry import config

    engine = create_engine(config.DATABASE_URL, echo=config.DEBUG)

    try:
        click.echo('Generating models')
        base.metadata.create_all(engine, checkfirst=False)
        click.echo('Done')
    except (OperationalError, ProgrammingError) as err:
        click.echo('ERROR: {}'.format(err))


@click.command()
@click.pass_context
def teardown_models(ctx):
    """delete models"""

    from woudc_data_registry import config

    engine = create_engine(config.DATABASE_URL, echo=config.DEBUG)

    try:
        click.echo('Deleting models')
        base.metadata.drop_all(engine, checkfirst=False)
        click.echo('Done')
    except (OperationalError, ProgrammingError) as err:
        click.echo('ERROR: {}'.format(err))
