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
import os

import click
from geoalchemy2 import Geometry
from sqlalchemy import (Column, create_engine, Date, DateTime, Integer, String,
                        Time, UnicodeText)
from sqlalchemy.exc import DataError, OperationalError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

import parser

DB_TYPE = os.getenv('DB_TYPE', None)
DB_HOST = os.getenv('DB_HOST', None)
DB_PORT = os.getenv('DB_PORT', None)
DB_USERNAME = os.getenv('DB_USERNAME', None)
DB_PASSWORD = os.getenv('DB_PASSWORD', None)
DB_NAME = os.getenv('DB_NAME', None)

DATABASE = '{}://{}:{}@{}/{}'.format(
    DB_TYPE, DB_USERNAME, DB_PASSWORD, DB_HOST, DB_NAME)

base = declarative_base()


class DataRecord(base):
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
    location = Column(Geometry('POINTZ', dimension=3, srid=4326))
    timestamp_utcoffset = Column(String, nullable=False)
    timestamp_date = Column(Date, nullable=False)
    timestamp_time = Column(Time)

    # data management fields
    insert_datetime = Column(DateTime, default=datetime.utcnow())
    processed_datetime = Column(DateTime, default=datetime.utcnow())
    raw = Column(UnicodeText, nullable=False)
    url = Column(String, nullable=False)

    def __init__(self, extcsv):
        self.extcsv = extcsv.extcsv
        self.raw = extcsv._raw
        self.geom = self._to_wkt_point()

    def _to_wkt_point(self, srid=4326):
        latitude = self.extcsv['LOCATION']['Latitude']
        longitude = self.extcsv['LOCATION']['Longitude']
        height = self.extcsv['LOCATION']['Height']
        point = 'SRID={};POINT({} {} {})'.format(srid, longitude, latitude,
                                                 height)
        return point

    def __repr__(self):
        return 'DataRecord(%r, %r)' % (self.identifier, self.url)


@click.command()
@click.pass_context
def setup_db(ctx):
    engine = create_engine(DATABASE, echo=False)

    try:
        click.echo('Generating models')
        base.metadata.create_all(engine, checkfirst=False)
        click.echo('Done')
    except OperationalError as err:
        click.echo('ERROR: {}'.format(err))


#    engine = create_engine(DATABASE, echo=False)
#    Session = sessionmaker(bind=engine)
#    session = Session()
#    elif subcommand == 'insert':
#        extcsv_ = parser.ExtendedCSV('20040709.ECC.2Z.2ZL1.NOAA-CMDL.csv')
#        d1 = DataRecord(extcsv_)
#        try:
#            session.add(d1)
#            session.commit()
#        except DataError as err:
#            print(err)
#            session.rollback()
#    elif subcommand == 'query':
#        alldata = session.query(DataRecord).all()
#        for somedata in alldata:
#            if somedata.geom is not None:
#                print(session.scalar(somedata.geom.ST_AsText()))
#                print(session.scalar(somedata.geom.ST_Z()))
#
#    session.close()
