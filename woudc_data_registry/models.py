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

import geoalchemy2
from sqlalchemy import (Boolean, Column, create_engine, Date, DateTime, Enum,
                        Integer, String, Time, UnicodeText)
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy.ext.declarative import declarative_base

from woudc_data_registry import util

base = declarative_base()


class Geometry(geoalchemy2.types.Geometry):
    """
    multi-geometry class workaround
    TODO: remove when https://github.com/geoalchemy/geoalchemy2/issues/158
          is fixed
    """
    def get_col_spec(self):
        if self.geometry_type == 'GEOMETRY' and self.srid == 0:
            return self.name
        return '%s(%s,%d)' % (self.name, self.geometry_type, self.srid)


class Contributor(base):
    """Data Registry Contributor"""

    __tablename__ = 'contributor'

    wmo_region_enum = Enum('I', 'II', 'III', 'IV', 'V', 'VI',
                           name='wmo_region')

    identifier = Column(Integer, primary_key=True, autoincrement=True)
    acronym = Column(String, nullable=False, unique=True)
    name = Column(String, nullable=False)
    country = Column(String, nullable=False)
    wmo_region = Column(wmo_region_enum, nullable=False)
    url = Column(String, nullable=False)
    email = Column(String, nullable=False)
    active = Column(Boolean, nullable=False, default=True)

    last_validated_datetime = Column(DateTime, nullable=False,
                                     default=datetime.utcnow())

    location = Column(Geometry('POINT', srid=4326), nullable=False)

    def __init__(self, dict_):
        """serializer"""

        self.acronym = dict_['acronym']
        self.name = dict_['name']
        self.country = dict_['country']
        self.wmo_region = dict_['wmo_region']
        self.url = dict_['url']
        self.email = dict_['email']

        self.location = util.point2ewkt(dict_['location']['longitude'],
                                        dict_['location']['latitude'])


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
    data_generation_scientific_authority = Column(String, nullable=True)

    platform_type = Column(String, default='STN', nullable=False)
    platform_id = Column(String, nullable=False)
    platform_name = Column(String, nullable=False)
    platform_country = Column(String, nullable=False)
    platform_gaw_id = Column(String, nullable=True)

    instrument_name = Column(String, nullable=False)
    instrument_model = Column(String, nullable=False)
    instrument_number = Column(String, nullable=False)

    location = Column(Geometry(srid=0), nullable=False)

    timestamp_utcoffset = Column(String, nullable=False)
    timestamp_date = Column(Date, nullable=False)
    timestamp_time = Column(Time, nullable=True)

    # data management fields

    published = Column(Boolean, nullable=False, default=False)

    received_datetime = Column(DateTime, nullable=False,
                               default=datetime.utcnow())

    inserted_datetime = Column(DateTime, nullable=False,
                               default=datetime.utcnow())

    processed_datetime = Column(DateTime, nullable=False,
                                default=datetime.utcnow())

    published_datetime = Column(DateTime, nullable=False,
                                default=datetime.utcnow())

    ingest_filepath = Column(String, nullable=False)
    filename = Column(String, nullable=False)

    raw = Column(UnicodeText, nullable=False)
    url = Column(String, nullable=False)
    urn = Column(String, nullable=False)

    def __init__(self, ecsv):
        """serializer"""

        self.content_class = ecsv.extcsv['CONTENT']['Class']
        self.content_category = ecsv.extcsv['CONTENT']['Category']
        self.content_level = ecsv.extcsv['CONTENT']['Level']
        self.content_form = ecsv.extcsv['CONTENT']['Form']

        self.data_generation_date = ecsv.extcsv['DATA_GENERATION']['Date']
        self.data_generation_agency = ecsv.extcsv['DATA_GENERATION']['Agency']
        self.data_generation_version = \
            ecsv.extcsv['DATA_GENERATION']['Version']

        if 'ScientificAuthority' in ecsv.extcsv['DATA_GENERATION']:
            self.data_generation_scientific_authority = \
                ecsv.extcsv['DATA_GENERATION']['ScientificAuthority']

        self.platform_type = ecsv.extcsv['PLATFORM']['Type']
        self.platform_id = ecsv.extcsv['PLATFORM']['ID']
        self.platform_name = ecsv.extcsv['PLATFORM']['Name']
        self.platform_country = ecsv.extcsv['PLATFORM']['Country']

        if 'GAW_ID' in ecsv.extcsv['PLATFORM']:
            self.platform_gaw_id = ecsv.extcsv['PLATFORM']['GAW_ID']

        self.instrument_name = ecsv.extcsv['INSTRUMENT']['Name']
        self.instrument_model = ecsv.extcsv['INSTRUMENT']['Model']
        self.instrument_number = ecsv.extcsv['INSTRUMENT']['Number']

        self.timestamp_utcoffset = ecsv.extcsv['TIMESTAMP']['UTCOffset']
        self.timestamp_date = ecsv.extcsv['TIMESTAMP']['Date']

        if 'Time' in ecsv.extcsv['TIMESTAMP']:
            self.timestamp_time = ecsv.extcsv['TIMESTAMP']['Time']

        self.location = util.point2ewkt(ecsv.extcsv['LOCATION']['Longitude'],
                                        ecsv.extcsv['LOCATION']['Latitude'],
                                        ecsv.extcsv['LOCATION']['Height'])

        self.extcsv = ecsv.extcsv
        self.raw = ecsv._raw
        self.urn = self.get_urn()

    def get_urn(self):
        """generate data record URN"""

        urn_tokens = [
            'urn',
            self.content_class,
            self.content_category,
            self.data_generation_agency,
            self.platform_type,
            self.platform_id,
            self.instrument_name,
            self.instrument_model,
            self.instrument_number,
            self.data_generation_date.strftime('%Y-%m-%d'),
            self.data_generation_version,
        ]

        return ':'.join(map(str, urn_tokens)).lower()

    def __repr__(self):
        return 'DataRecord(%r, %r)' % (self.identifier, self.url)


@click.group()
def model():
    pass


@click.command()
@click.pass_context
def setup(ctx):
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
def teardown(ctx):
    """delete models"""

    from woudc_data_registry import config

    engine = create_engine(config.DATABASE_URL, echo=config.DEBUG)

    try:
        click.echo('Deleting models')
        base.metadata.drop_all(engine, checkfirst=False)
        click.echo('Done')
    except (OperationalError, ProgrammingError) as err:
        click.echo('ERROR: {}'.format(err))


@click.command()
@click.pass_context
def init_metadata(ctx):
    """add core metadata"""

    pass


model.add_command(setup)
model.add_command(teardown)
model.add_command(init_metadata)
