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
import logging

import click
import csv
import geoalchemy2
import json
from sqlalchemy import (Boolean, Column, create_engine, Date, DateTime,
                        Enum, ForeignKey, Integer, String, Time, UnicodeText,
                        UniqueConstraint)
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

from woudc_data_registry import registry, util

base = declarative_base()

LOGGER = logging.getLogger(__name__)

WMO_REGION_ENUM = Enum('I', 'II', 'III', 'IV', 'V', 'VI', 'Antarctica',
                       name='wmo_region')


class Geometry(geoalchemy2.types.Geometry):
    """
    multi-geometry class workaround
    TODO: remove when https://github.com/geoalchemy/geoalchemy2/issues/158
          is fixed
    """
    def get_col_spec(self):
        if self.geometry_type == 'GEOMETRY' and self.srid == 0:
            return self.name
        return '{}({}, {})'.format(self.name, self.geometry_type, self.srid)


class Country(base):
    """
    Data Registry Country

    https://www.wmo.int/cpdb/data/membersandterritories.json

    """

    __tablename__ = 'countries'

    identifier = Column(String, nullable=False, primary_key=True)
    country_name = Column(String, nullable=False, unique=True)
    french_name = Column(String, nullable=False, unique=True)
    wmo_region_id = Column(WMO_REGION_ENUM, nullable=False)
    regional_involvement = Column(WMO_REGION_ENUM, nullable=False)
    wmo_membership = Column(Date, nullable=False)
    link = Column(String, nullable=False)

    def __init__(self, dict_):
        self.identifier = dict_['id']
        self.country_name = dict_['country_name']
        self.french_name = dict_['french_name']
        self.wmo_region_id = dict_['wmo_region_id']
        self.regional_involvement = dict_['regional_involvement']
        self.wmo_membership = dict_['wmo_membership']
        self.link = dict_['link']

    def __repr__(self):
        return 'Country ({}, {})'.format(self.identifier, self.country_name)


class Contributor(base):
    """Data Registry Contributor"""

    __tablename__ = 'contributors'
    __table_args__ = (UniqueConstraint('acronym', 'project'),)

    identifier = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    acronym = Column(String, nullable=False)
    country_id = Column(String, ForeignKey('countries.identifier'),
                        nullable=False)
    project = Column(String, nullable=False, default='WOUDC')
    wmo_region = Column(WMO_REGION_ENUM, nullable=False)
    url = Column(String, nullable=False)
    email = Column(String, nullable=False)
    ftp_username = Column(String, nullable=False)
    active = Column(Boolean, nullable=False, default=True)

    last_validated_datetime = Column(DateTime, nullable=False,
                                     default=datetime.utcnow())

    location = Column(Geometry('POINT', srid=4326), nullable=False)

    # relationships
    country = relationship('Country', backref=__tablename__)

    def __init__(self, dict_):
        """serializer"""

        self.identifier = dict_['acronym']
        self.name = dict_['name']
        self.acronym = dict_['acronym']
        self.country_id = dict_['country_id']
        self.project = dict_['project']
        self.wmo_region = dict_['wmo_region']
        self.url = dict_['url']
        self.email = dict_['email']
        self.ftp_username = dict_['ftp_username']
        self.location = util.point2ewkt(dict_['x'], dict_['y'])

        if self.identifier != 'WOUDC':
            self.identifier = '{}.{}'.format(self.identifier, self.project)
        print(self.identifier)

    def __repr__(self):
        return 'Contributor ({}, {})'.format(self.identifier, self.name)


class Dataset(base):
    """Data Registry Dataset"""

    __tablename__ = 'datasets'

    identifier = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    slug = Column(String, nullable=False)

    def __init__(self, dict_):
        self.name = dict_['name']
        self.slug = dict_['slug']


class Station(base):
    """Data Registry Station"""

    __tablename__ = 'stations'
    __table_args__ = (UniqueConstraint('active_start_date', 'active_end_date',
                      'contributor_id', 'identifier'),)

    stn_type_enum = Enum('STN', 'SHP', name='type')

    identifier = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    contributor_id = Column(String, ForeignKey('contributors.identifier'),
                            nullable=True)
    stn_type = Column(stn_type_enum, nullable=False)
    gaw_id = Column(String, nullable=True)
    country_id = Column(String, ForeignKey('countries.identifier'),
                        nullable=False)
    wmo_region = Column(WMO_REGION_ENUM, nullable=True)
    active = Column(Boolean, nullable=False, default=True)
    active_start_date = Column(Date, nullable=False)
    active_end_date = Column(Date, nullable=True)

    last_validated_datetime = Column(DateTime, nullable=False,
                                     default=datetime.utcnow())

    # location = Column(Geometry('POINT', srid=4326), nullable=False)
    location = Column(Geometry(srid=0), nullable=False)

    # relationships
    country = relationship('Country', backref=__tablename__)
    contributor = relationship('Contributor', backref=__tablename__)

    def __init__(self, dict_):
        """serializer"""

        self.identifier = dict_['identifier']
        self.name = dict_['name']
        self.stn_type = dict_['stn_type']
        self.gaw_id = dict_['gaw_id']
        self.country_id = dict_['country_id']
        self.contributor_id = dict_['contributor_id']
        self.wmo_region = dict_['wmo_region']
        self.active_start_date = dict_['active_start_date']
        self.active_end_datet = dict_['active_end_date']
        self.location = util.point2ewkt(dict_['x'], dict_['y'], dict_['z'])

    def __repr__(self):
        return 'Station ({}, {})'.format(self.identifier, self.name)


class DataRecord(base):
    """Data Registry Data Record"""

    __tablename__ = 'data_records'

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

    location = Column(Geometry(geometry_type='POINT', srid=4326),
                      nullable=False)

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

    def get_waf_path(self, basepath):
        """generate WAF URL"""

        datasetdirname = '{}_{}_{}'.format(self.content_category,
                                           self.content_level,
                                           self.content_form)

        url_tokens = [
            basepath.rstrip('/'),
            'Archive-NewFormat',
            datasetdirname,
            'stn{}'.format(self.platform_id),
            self.instrument_name.lower(),
            self.timestamp_date.strftime('%Y'),
            self.filename
        ]

        return '/'.join(url_tokens)

    def to_geojson_dict(self):
        """return dict as a GeoJSON representation"""

        data = self.__dict__

        fields_to_remove = [
            '_sa_instance_state',
            'extcsv',
            'location',
            'ingest_filepath',
        ]

        geometry = util.point2geojsongeometry(
            data['extcsv']['LOCATION']['Longitude'],
            data['extcsv']['LOCATION']['Latitude'],
            data['extcsv']['LOCATION']['Height'])

        LOGGER.debug('removing internal / unwanted fields')
        for field_to_remove in fields_to_remove:
            data.pop(field_to_remove, None)

        feature = {
            'type': 'Feature',
            'geometry': geometry,
            'properties': data
        }

        return feature

    def __repr__(self):
        return 'DataRecord({}, {})'.format(self.identifier, self.url)


@click.group()
def manage():
    pass


@click.command()
@click.pass_context
def setup(ctx):
    """create models"""

    from woudc_data_registry import config

    engine = create_engine(config.WDR_DATABASE_URL, echo=config.WDR_DEBUG)

    try:
        click.echo('Generating models')
        base.metadata.create_all(engine, checkfirst=True)
        click.echo('Done')
    except (OperationalError, ProgrammingError) as err:
        click.echo('ERROR: {}'.format(err))


@click.command()
@click.pass_context
def teardown(ctx):
    """delete models"""

    from woudc_data_registry import config

    engine = create_engine(config.WDR_DATABASE_URL, echo=config.WDR_DEBUG)

    try:
        click.echo('Deleting models')
        base.metadata.drop_all(engine, checkfirst=True)
        click.echo('Done')
    except (OperationalError, ProgrammingError) as err:
        click.echo('ERROR: {}'.format(err))


@click.command()
@click.pass_context
@click.option('--datadir', '-d',
              type=click.Path(exists=True, resolve_path=True),
              help='Path to core metadata files')
def init(ctx, datadir):
    """initialize core system metadata"""

    import os

    if datadir is None:
        raise click.ClickException('Missing required data directory')

    countries = os.path.join(datadir, 'wmo-countries.json')
    contributors = os.path.join(datadir, 'contributors.csv')
    stations = os.path.join(datadir, 'stations.csv')
    datasets = os.path.join(datadir, 'datasets.csv')

    registry_ = registry.Registry()

    click.echo('Loading countries metadata')
    with open(countries) as jsonfile:
        countries_data = json.load(jsonfile)
        for row in countries_data['countries']:
            country_data = countries_data['countries'][row]
            if country_data['id'] == 'NUL':
                continue
            country = Country(country_data)
            registry_.save(country)
    # Antarctica is not a recognized country per se but is
    # provided in WOUDC
    antarctica = {
        'id': 'ATA',
        'country_name': 'Antarctica',
        'french_name': 'Antarctique',
        'wmo_region_id': 'Antarctica',
        'regional_involvement': 'Antarctica',
        'wmo_membership': '1970-01-01',
        'link': 'https://www.wmo.int/pages/prog/www/Antarctica/Purpose.html'
    }
    country = Country(antarctica)
    registry_.save(country)

    click.echo('Loading datasets metadata')
    with open(datasets) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            dataset = Dataset(row)
            registry_.save(dataset)

    click.echo('Loading contributors metadata')
    with open(contributors) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            contributor = Contributor(row)
            registry_.save(contributor)

    # load stations CSV
    click.echo('Loading stations metadata')
    with open(stations) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            station = Station(row)
            registry_.save(station)
    # load instruments CSV


manage.add_command(setup)
manage.add_command(teardown)
manage.add_command(init)
