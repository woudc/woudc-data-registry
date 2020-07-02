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

import datetime
import logging

import click
import csv
import json
import codecs
from sqlalchemy import (Boolean, Column, create_engine, Date, DateTime,
                        Float, Enum, ForeignKey, Integer, String, Time,
                        UniqueConstraint, ForeignKeyConstraint)
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

from woudc_data_registry import config, registry
from woudc_data_registry.search import SearchIndex, search
from woudc_data_registry.util import point2geojsongeometry

base = declarative_base()

LOGGER = logging.getLogger(__name__)

WMO_REGION_ENUM = Enum('I', 'II', 'III', 'IV', 'V', 'VI', 'the Antarctic',
                       'International Waters', name='wmo_region_id')


class Country(base):
    """
    Data Registry Country

    https://www.wmo.int/cpdb/data/membersandterritories.json

    """

    __tablename__ = 'countries'

    id_field = 'country_id'
    id_dependencies = []  # No ID dependencies

    country_id = Column(String, nullable=False, primary_key=True)
    name_en = Column(String, nullable=False, unique=True)
    name_fr = Column(String, nullable=False, unique=True)
    wmo_region_id = Column(WMO_REGION_ENUM, nullable=False)
    regional_involvement = Column(String, nullable=False)
    wmo_membership = Column(Date, nullable=True)
    link = Column(String, nullable=False)

    def __init__(self, dict_):
        self.country_id = dict_['id']
        self.name_en = dict_['country_name']
        self.name_fr = dict_['french_name']

        self.wmo_region_id = dict_['wmo_region_id']
        self.regional_involvement = dict_['regional_involvement']
        self.link = dict_['link']

        if 'wmo_membership' in dict_:
            try:
                wmo_membership_ = datetime.datetime.strptime(
                    dict_['wmo_membership'], '%Y-%m-%d').date()
            except ValueError as err:
                LOGGER.warning(err)
                wmo_membership_ = None
        else:
            wmo_membership_ = None

        self.wmo_membership = wmo_membership_

    @property
    def __geo_interface__(self):
        return {
            'id': self.country_id,
            'type': 'Feature',
            'geometry': None,
            'properties': {
                'identifier': self.country_id,
                'country_name_en': self.name_en,
                'country_name_fr': self.name_fr,
                'wmo_region_id': self.wmo_region_id,
                'wmo_membership': self.wmo_membership,
                'regional_involvement': self.regional_involvement,
                'link': self.link
            }
        }

    def __repr__(self):
        return 'Country ({}, {})'.format(self.country_id, self.name_en)


class Contributor(base):
    """Data Registry Contributor"""

    __tablename__ = 'contributors'
    __table_args__ = (UniqueConstraint('contributor_id'),
                      UniqueConstraint('acronym', 'project_id'))

    id_field = 'contributor_id'
    id_dependencies = ['acronym', 'project_id']

    contributor_id = Column(String, primary_key=True)

    name = Column(String, nullable=False)
    acronym = Column(String, nullable=False)
    country_id = Column(String, ForeignKey('countries.country_id'),
                        nullable=False)
    project_id = Column(String, ForeignKey('projects.project_id'),
                        nullable=False, default='WOUDC')

    wmo_region_id = Column(WMO_REGION_ENUM, nullable=False)
    url = Column(String, nullable=False)
    email = Column(String, nullable=False)
    ftp_username = Column(String, nullable=False)

    active = Column(Boolean, nullable=False, default=True)
    start_date = Column(Date, nullable=False)
    end_date = Column(Date, nullable=True)
    last_validated_datetime = Column(DateTime, nullable=False,
                                     default=datetime.datetime.utcnow())

    x = Column(Float, nullable=False)
    y = Column(Float, nullable=False)

    # relationships
    country = relationship('Country', backref=__tablename__)

    def __init__(self, dict_):
        """serializer"""

        self.country_id = dict_['country_id']
        self.project_id = dict_['project_id']

        self.name = dict_['name']
        self.acronym = dict_['acronym']

        self.generate_ids()

        self.wmo_region_id = dict_['wmo_region_id']
        self.url = dict_['url']
        self.email = dict_['email']
        self.ftp_username = dict_['ftp_username']

        try:
            if isinstance(dict_['start_date'], datetime.date):
                self.start_date = dict_['start_date']
            else:
                self.start_date = datetime.datetime.strptime(
                    dict_['start_date'], '%Y-%m-%d').date()
            if dict_['end_date'] is None \
               or isinstance(dict_['end_date'], datetime.date):
                self.end_date = dict_['end_date']
            elif dict_['end_date']:
                self.end_date = datetime.datetime.strptime(
                    dict_['end_date'], '%Y-%m-%d').date()
        except Exception as err:
            LOGGER.error(err)

        self.active = dict_.get('active', True)
        self.last_validated_datetime = datetime.datetime.utcnow()
        self.x = dict_['x']
        self.y = dict_['y']

    @property
    def __geo_interface__(self):
        return {
            'id': self.contributor_id,
            'type': 'Feature',
            'geometry': point2geojsongeometry(self.x, self.y),
            'properties': {
                'identifier': self.contributor_id,
                'acronym': self.acronym,
                'name': self.name,
                'project': self.project_id,
                'country_name_en': self.country.name_en,
                'country_name_fr': self.country.name_fr,
                'wmo_region_id': self.wmo_region_id,
                'url': self.url,
                'active': self.active,
                'start_date': self.start_date,
                'end_date': self.end_date,
                'last_validated_datetime': self.last_validated_datetime
            }
        }

    def __repr__(self):
        return 'Contributor ({}, {})'.format(self.contributor_id, self.name)

    def generate_ids(self):
        """Builds and sets class ID field from other attributes"""

        if all([hasattr(self, field) and getattr(self, field) is not None
                for field in self.id_dependencies]):
            components = [getattr(self, field)
                          for field in self.id_dependencies]
            self.contributor_id = ':'.join(map(str, components))


class Dataset(base):
    """Data Registry Dataset"""

    __tablename__ = 'datasets'

    id_field = 'dataset_id'
    id_dependencies = []  # No ID dependencies

    dataset_id = Column(String, primary_key=True)

    data_class = Column(String, nullable=False)

    def __init__(self, dict_):
        self.dataset_id = dict_['dataset_id']

        self.data_class = dict_['data_class']

    @property
    def __geo_interface__(self):
        return {
            'id': self.dataset_id,
            'type': 'Feature',
            'geometry': None,
            'properties': {
                'identifier': self.dataset_id,
                'data_class': self.data_class
            }
        }

    def __repr__(self):
        return 'Dataset ({})'.format(self.dataset_id)


class Instrument(base):
    """Data Registry Instrument"""

    __tablename__ = 'instruments'

    id_field = 'instrument_id'
    id_dependencies = ['name', 'model', 'serial',
                       'station_id', 'dataset_id', 'contributor']

    instrument_id = Column(String, primary_key=True)
    station_id = Column(String, ForeignKey('stations.station_id'),
                        nullable=False)
    dataset_id = Column(String, ForeignKey('datasets.dataset_id'),
                        nullable=False)
    deployment_id = Column(String, ForeignKey('deployments.deployment_id'),
                           nullable=False)

    name = Column(String, nullable=False)
    model = Column(String, nullable=False)
    serial = Column(String, nullable=False)

    start_date = Column(Date, nullable=False)
    end_date = Column(Date, nullable=True)

    x = Column(Float, nullable=False)
    y = Column(Float, nullable=False)
    z = Column(Float, nullable=False)

    # relationships
    station = relationship('Station', backref=__tablename__)
    dataset = relationship('Dataset', backref=__tablename__)
    deployment = relationship('Deployment', backref=__tablename__)

    def __init__(self, dict_):
        self.station_id = dict_['station_id']
        self.dataset_id = dict_['dataset_id']
        self.contributor = dict_['contributor']

        self.name = dict_['name']
        self.model = dict_['model']
        self.serial = dict_['serial']

        self.generate_ids()
        try:
            if isinstance(dict_['start_date'], datetime.date):
                self.start_date = dict_['start_date']
            else:
                self.start_date = datetime.datetime.strptime(
                    dict_['start_date'], '%Y-%m-%d').date()
            if dict_['end_date'] is None \
               or isinstance(dict_['end_date'], datetime.date):
                self.end_date = dict_['end_date']
            elif dict_['end_date']:
                self.end_date = datetime.datetime.strptime(
                    dict_['end_date'], '%Y-%m-%d').date()
        except Exception as err:
            LOGGER.error(err)

        self.x = dict_['x']
        self.y = dict_['y']
        self.z = dict_['z']

    @property
    def __geo_interface__(self):
        waf_basepath = config.WDR_WAF_BASEURL

        dataset_folder = '{}_1.0_1'.format(self.dataset_id)
        station_folder = '{}{}'.format(self.station.station_type.lower(),
                                       self.station_id)
        instrument_folder = self.name.lower()

        return {
            'id': self.instrument_id,
            'type': 'Feature',
            'geometry': point2geojsongeometry(self.x, self.y, self.z),
            'properties': {
                'identifier': self.instrument_id,
                'station_id': self.station_id,
                'station_name': self.station.station_name.name,
                'data_class': self.dataset.data_class,
                'dataset': self.dataset_id,
                'deployment_id': self.deployment_id,
                'name': self.name,
                'model': self.model,
                'serial': self.serial,
                'start_date': self.start_date,
                'end_date': self.end_date,
                'waf_url': '/'.join([waf_basepath, dataset_folder,
                                     station_folder, instrument_folder])
            }
        }

    def __repr__(self):
        return 'Instrument ({})'.format(self.instrument_id)

    def generate_ids(self):
        """Builds and sets class ID field from other attributes"""

        if all([hasattr(self, field) and getattr(self, field) is not None
                for field in self.id_dependencies]):
            components = [getattr(self, field)
                          for field in self.id_dependencies]
            self.instrument_id = ':'.join(map(str, components))

        # build the deployment id below
        temp = {self.station_id, self.contributor}
        self.deployment_id = ':'.join(temp)


class Project(base):
    """Data Registry Project"""

    __tablename__ = 'projects'

    id_field = 'project_id'
    id_dependencies = []  # No ID dependencies

    project_id = Column(String, primary_key=True)

    def __init__(self, dict_):
        self.project_id = dict_['project_id']

    @property
    def __geo_interface__(self):
        return {
            'id': self.project_id,
            'type': 'Feature',
            'geometry': None,
            'properties': {
                'identifier': self.project_id
            }
        }

    def __repr__(self):
        return 'Project ({})'.format(self.project_id)


class Station(base):
    """Data Registry Station"""

    __tablename__ = 'stations'
    __table_args__ = (UniqueConstraint('station_id'),)

    id_field = 'station_id'
    id_dependencies = []  # No ID dependencies
    stn_type_enum = Enum('STN', 'SHP', name='type')

    station_id = Column(String, primary_key=True)
    station_name_id = Column(String,
                             ForeignKey('station_names.station_name_id'),
                             nullable=False)
    station_type = Column(stn_type_enum, nullable=False)
    gaw_id = Column(String, nullable=True)
    country_id = Column(String, ForeignKey('countries.country_id'),
                        nullable=False)
    wmo_region_id = Column(WMO_REGION_ENUM, nullable=False)
    active = Column(Boolean, nullable=False, default=True)

    start_date = Column(Date, nullable=False)
    end_date = Column(Date, nullable=True)

    last_validated_datetime = Column(DateTime, nullable=False,
                                     default=datetime.datetime.utcnow())

    x = Column(Float, nullable=False)
    y = Column(Float, nullable=False)
    z = Column(Float, nullable=False)

    # relationships
    country = relationship('Country', backref=__tablename__)
    station_name = relationship('StationName', backref=__tablename__)

    def __init__(self, dict_):
        """serializer"""

        self.station_id = dict_['station_id']
        self.station_name_id = '{}:{}' \
            .format(self.station_id, dict_['station_name'])
        self.station_type = dict_['station_type']

        self._name = dict_['station_name']

        if dict_['gaw_id'] != '':
            self.gaw_id = dict_['gaw_id']

        self.country_id = dict_['country_id']
        self.wmo_region_id = dict_['wmo_region_id']

        try:
            if isinstance(dict_['start_date'], datetime.date):
                self.start_date = dict_['start_date']
            else:
                self.start_date = datetime.datetime.strptime(
                    dict_['start_date'], '%Y-%m-%d').date()
            if dict_['end_date'] is None \
               or isinstance(dict_['end_date'], datetime.date):
                self.end_date = dict_['end_date']
            elif dict_['end_date']:
                self.end_date = datetime.datetime.strptime(
                    dict_['end_date'], '%Y-%m-%d').date()
        except Exception as err:
            LOGGER.error(err)

        self.last_validated_datetime = datetime.datetime.utcnow()

        self.x = dict_['x']
        self.y = dict_['y']
        self.z = dict_['z']

    @property
    def name(self):
        if hasattr(self, '_name'):
            return self._name
        else:
            return self.station_name.name

    @property
    def __geo_interface__(self):
        gaw_baseurl = 'https://gawsis.meteoswiss.ch/GAWSIS/index.html#' \
                      '/search/station/stationReportDetails'
        gaw_pagename = '0-20008-0-{}'.format(self.gaw_id)

        return {
            'id': self.station_id,
            'type': 'Feature',
            'geometry': point2geojsongeometry(self.x, self.y, self.z),
            'properties': {
                'woudc_id': self.station_id,
                'gaw_id': self.gaw_id,
                'name': self.station_name.name,
                'type': self.station_type,
                'country_name_en': self.country.name_en,
                'country_name_fr': self.country.name_fr,
                'wmo_region_id': self.wmo_region_id,
                'active': self.active,
                'start_date': self.start_date,
                'end_date': self.end_date,
                'last_validated_datetime': self.last_validated_datetime,
                'gaw_url': '{}/{}'.format(gaw_baseurl, gaw_pagename)
            }
        }

    def __repr__(self):
        return 'Station ({}, {})'.format(self.station_id,
                                         self.station_name.name)


class StationName(base):
    """Data Registry Station Alternative Name"""

    __tablename__ = 'station_names'
    __table_args__ = (UniqueConstraint('station_name_id'),)

    id_field = 'station_name_id'
    id_dependencies = ['station_id', 'name']

    station_name_id = Column(String, primary_key=True)
    station_id = Column(String, nullable=False)
    name = Column(String, nullable=False)

    def __init__(self, dict_):
        self.station_id = dict_['station_id']
        self.name = dict_['name']

        self.generate_ids()

    def __repr__(self):
        return 'Station name ({}, {})'.format(self.station_id, self.name)

    def generate_ids(self):
        """Builds and sets class ID field from other attributes"""

        if all([hasattr(self, field) and getattr(self, field) is not None
                for field in self.id_dependencies]):
            components = [getattr(self, field)
                          for field in self.id_dependencies]
            self.station_name_id = ':'.join(map(str, components))


class Deployment(base):
    """Data Registry Deployment"""

    __tablename__ = 'deployments'
    __table_args__ = (UniqueConstraint('deployment_id'),)

    id_field = 'deployment_id'
    id_dependencies = ['station_id', 'contributor_id']

    deployment_id = Column(String, primary_key=True)
    station_id = Column(String, ForeignKey('stations.station_id'),
                        nullable=False)
    contributor_id = Column(String, ForeignKey('contributors.contributor_id'),
                            nullable=False)
    start_date = Column(Date, nullable=False)
    end_date = Column(Date, nullable=True)

    # relationships
    station = relationship('Station', backref=__tablename__)
    contributor = relationship('Contributor', backref=__tablename__)

    def __init__(self, dict_):
        """serializer"""

        self.station_id = dict_['station_id']
        self.contributor_id = dict_['contributor_id']

        self.generate_ids()

        try:
            if isinstance(dict_['start_date'], datetime.date):
                self.start_date = dict_['start_date']
            else:
                self.start_date = datetime.datetime.strptime(
                    dict_['start_date'], '%Y-%m-%d').date()
            if dict_['end_date'] is None \
               or isinstance(dict_['end_date'], datetime.date):
                self.end_date = dict_['end_date']
            elif dict_['end_date']:
                self.end_date = datetime.datetime.strptime(
                    dict_['end_date'], '%Y-%m-%d').date()
        except Exception as err:
            LOGGER.error(err)

    @property
    def __geo_interface__(self):
        if self.station is None:
            geom = None
        else:
            geom = point2geojsongeometry(self.station.x, self.station.y,
                                         self.station.z)
        return {
            'id': self.deployment_id,
            'type': 'Feature',
            'geometry': geom,
            'properties': {
                'identifier': self.deployment_id,
                'station_id': self.station_id,
                'station_type': self.station.station_type,
                'station_name': self.station.station_name.name,
                'station_country_en': self.station.country.name_en,
                'station_country_fr': self.station.country.name_fr,
                'contributor': self.contributor.acronym,
                'contributor_name': self.contributor.name,
                'contributor_project': self.contributor.project_id,
                'contributor_url': self.contributor.url,
                'start_date': self.start_date,
                'end_date': self.end_date
            }
        }

    def __repr__(self):
        return 'Deployment ({})'.format(self.deployment_id)

    def generate_ids(self):
        """Builds and sets class ID field from other attributes"""

        if all([hasattr(self, field) and getattr(self, field) is not None
                for field in self.id_dependencies]):
            components = [getattr(self, field)
                          for field in self.id_dependencies]
            self.deployment_id = ':'.join(map(str, components))


class DataRecord(base):
    """Data Registry Data Record"""

    __tablename__ = 'data_records'
    __table_args__ = (
       ForeignKeyConstraint(
           ['data_generation_agency', 'content_class'],
           ['contributors.acronym', 'contributors.project_id']),
    )

    id_field = 'data_record_id'
    id_dependencies = [
            'content_class',
            'content_category',
            'content_level',
            'content_form',
            'data_generation_agency',
            'platform_type',
            'station_id',
            'instrument_name',
            'instrument_model',
            'instrument_number',
            'timestamp_date',
            'data_generation_version'
        ]

    data_record_id = Column(String, primary_key=True)

    # Extended CSV core fields
    content_class = Column(String, ForeignKey('projects.project_id'),
                           nullable=False)
    content_category = Column(String, ForeignKey('datasets.dataset_id'),
                              nullable=False)
    content_level = Column(String, nullable=False)
    content_form = Column(String, nullable=False)

    data_generation_date = Column(Date, nullable=False)
    data_generation_agency = Column(String, nullable=False)
    data_generation_version = Column(String, nullable=False)
    data_generation_scientific_authority = Column(String, nullable=True)

    station_id = Column(String, ForeignKey('stations.station_id'),
                        nullable=False)
    instrument_id = Column(String, ForeignKey('instruments.instrument_id'),
                           nullable=False)

    x = Column(Float, nullable=False)
    y = Column(Float, nullable=False)
    z = Column(Float, nullable=True)

    timestamp_utcoffset = Column(String, nullable=False)
    timestamp_date = Column(Date, nullable=False)
    timestamp_time = Column(Time, nullable=True)

    number_of_observations = Column(Integer, nullable=True)

    # data management fields

    published = Column(Boolean, nullable=False, default=False)

    received_datetime = Column(DateTime, nullable=False,
                               default=datetime.datetime.utcnow())

    inserted_datetime = Column(DateTime, nullable=False,
                               default=datetime.datetime.utcnow())

    processed_datetime = Column(DateTime, nullable=False,
                                default=datetime.datetime.utcnow())

    published_datetime = Column(DateTime, nullable=False,
                                default=datetime.datetime.utcnow())

    ingest_filepath = Column(String, nullable=False)
    filename = Column(String, nullable=False)
    output_filepath = Column(String, nullable=False)

    url = Column(String, nullable=False)
    es_id = Column(String, nullable=False)

    # Relationships
    station = relationship('Station', backref=__tablename__)
    instrument = relationship('Instrument', backref=__tablename__)

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

        self._platform_type = ecsv.extcsv['PLATFORM']['Type']
        self._platform_name = ecsv.extcsv['PLATFORM']['Name']
        self._platform_country = ecsv.extcsv['PLATFORM']['Country']
        self._platform_gaw_id = ecsv.extcsv['PLATFORM'].get('GAW_ID', None)
        self.station_id = str(ecsv.extcsv['PLATFORM']['ID'])

        self._instrument_name = ecsv.extcsv['INSTRUMENT']['Name']
        self._instrument_model = str(ecsv.extcsv['INSTRUMENT']['Model'])
        self._instrument_number = str(ecsv.extcsv['INSTRUMENT']['Number'])
        self.instrument_id = ':'.join([
            self.instrument_name,
            self.instrument_model,
            self.instrument_number,
            self.station_id,
            self.content_category
        ])

        self.timestamp_utcoffset = ecsv.extcsv['TIMESTAMP']['UTCOffset']
        self.timestamp_date = ecsv.extcsv['TIMESTAMP']['Date']

        if 'Time' in ecsv.extcsv['TIMESTAMP']:
            self.timestamp_time = ecsv.extcsv['TIMESTAMP']['Time']

        self.x = ecsv.extcsv['LOCATION']['Longitude']
        self.y = ecsv.extcsv['LOCATION']['Latitude']
        self.z = ecsv.extcsv['LOCATION']['Height']

        self.generate_ids()

        self.extcsv = ecsv.extcsv
        self.number_of_observations = ecsv.number_of_observations()

    @property
    def platform_type(self):
        if hasattr(self, '_platform_type'):
            return self._platform_type
        else:
            return self.station.station_type

    @property
    def platform_name(self):
        if hasattr(self, '_platform_name'):
            return self._platform_name
        else:
            return self.station.name

    @property
    def platform_country(self):
        if hasattr(self, '_platform_country'):
            return self._platform_country
        else:
            return self.station.country_id

    @property
    def platform_gaw_id(self):
        if hasattr(self, '_platform_gaw_id'):
            return self._platform_gaw_id
        else:
            return self.station.gaw_id

    @property
    def instrument_name(self):
        if hasattr(self, '_instrument_name'):
            return self._instrument_name
        else:
            return self.instrument.name

    @property
    def instrument_model(self):
        if hasattr(self, '_instrument_model'):
            return self._instrument_model
        else:
            return self.instrument.model

    @property
    def instrument_number(self):
        if hasattr(self, '_instrument_number'):
            return self._instrument_number
        else:
            return self.instrument.serial

    def generate_ids(self):
        """Builds and sets class ID fields from other attributes"""

        self.data_record_id = self.get_urn()
        self.es_id = self.get_esid()

    def get_urn(self):
        """generate data record URN"""

        if all([hasattr(self, field) for field in self.id_dependencies]):
            tokens = [getattr(self, field) for field in self.id_dependencies]
            return ':'.join(map(str, tokens)).lower()
        else:
            return None

    def get_esid(self):
        """generate data record ES identifier"""

        dependencies = self.id_dependencies[:-1]

        if all([hasattr(self, field) for field in dependencies]):
            tokens = [getattr(self, field) for field in dependencies]
            return ':'.join(map(str, tokens)).lower()
        else:
            return None

    def get_waf_path(self, basepath):
        """generate WAF URL"""

        datasetdirname = '{}_{}_{}'.format(self.content_category,
                                           self.content_level,
                                           self.content_form)

        url_tokens = [
            basepath.rstrip('/'),
            'Archive-NewFormat',
            datasetdirname,
            '{}{}'.format(self.platform_type.lower(), self.station_id),
            self.instrument_name.lower(),
            self.timestamp_date.strftime('%Y'),
            self.filename
        ]

        return '/'.join(url_tokens)

    @property
    def __geo_interface__(self):
        return {
            'id': self.es_id,
            'type': 'Feature',
            'geometry': point2geojsongeometry(self.x, self.y, self.z),
            'properties': {
                'identifier': self.es_id,
                'content_class': self.content_class,
                'content_category': self.content_category,
                'content_level': self.content_level,
                'content_form': self.content_form,

                'data_generation_date': self.data_generation_date,
                'data_generation_agency': self.data_generation_agency,
                'data_generation_version': self.data_generation_version,
                'data_generation_scientific_authority': self.data_generation_scientific_authority,  # noqa

                'platform_type': self.platform_type,
                'platform_id': self.station_id,
                'platform_name': self.platform_name,
                'platform_country': self.platform_country,
                'platform_gaw_id': self.platform_gaw_id,

                'instrument_name': self.instrument_name,
                'instrument_model': self.instrument_model,
                'instrument_number': self.instrument_number,

                'timestamp_utcoffset': self.timestamp_utcoffset,
                'timestamp_date': self.timestamp_date,
                'timestamp_time': None if self.timestamp_time is None \
                    else self.timestamp_time.isoformat(),

                'published': self.published,
                'received_datetime': self.received_datetime,
                'inserted_datetime': self.inserted_datetime,
                'processed_datetime': self.processed_datetime,
                'published_datetime': self.published_datetime,

                'number_of_observations': self.number_of_observations,

                'ingest_filepath': self.ingest_filepath,
                'filename': self.filename,
                'output_filepath': self.output_filepath,

                'url': self.url
            }
        }

    def __repr__(self):
        return 'DataRecord({}, {})'.format(self.data_record_id, self.url)


def unpack_station_names(rows):
    """
    Collects CSV data on station names from the iterable <rows>
    and returns an iterable of station name records.

    Station names that are equivalent but appear separately in the input
    (e.g. the same name in multiple capitalizations and/or encodings)
    are corrected and start/end dates adjusted accordingly.

    :param rows: Iterable of rows of CSV input data.
    :returns: Iterable of station name records (dictionaries).
    """

    tracker = {}
    decode_hex = codecs.getdecoder('hex_codec')

    for row in rows:
        name = row['name']

        if name.startswith('\\x'):
            name = decode_hex(name[2:])[0].decode('utf-8')
            row['name'] = name
        if name not in tracker:
            tracker[name] = row

    return tracker.values()


@click.group('registry')
def registry__():
    """Registry"""
    pass


@click.group()
def admin():
    """System administration"""
    pass


@click.command()
@click.pass_context
def setup(ctx):
    """create models"""

    from woudc_data_registry import config

    engine = create_engine(config.WDR_DATABASE_URL, echo=config.WDR_DB_DEBUG)

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

    engine = create_engine(config.WDR_DATABASE_URL, echo=config.WDR_DB_DEBUG)

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
@click.option('--init-search-index', is_flag=True,
              help='Causes records to be stored in the search index as well')
def init(ctx, datadir, init_search_index):
    """initialize core system metadata"""

    import os

    if datadir is None:
        raise click.ClickException('Missing required data directory')

    wmo_countries = os.path.join(datadir, 'wmo-countries.json')
    countries = os.path.join(datadir, 'countries.json')
    contributors = os.path.join(datadir, 'contributors.csv')
    stations = os.path.join(datadir, 'stations.csv')
    ships = os.path.join(datadir, 'ships.csv')
    station_names = os.path.join(datadir, 'station-names.csv')
    datasets = os.path.join(datadir, 'datasets.csv')
    projects = os.path.join(datadir, 'projects.csv')
    instruments = os.path.join(datadir, 'instruments.csv')
    deployments = os.path.join(datadir, 'deployments.csv')

    registry_ = registry.Registry()

    project_models = []
    dataset_models = []
    country_models = []
    contributor_models = []
    station_models = []
    station_name_models = []
    instrument_models = []
    deployment_models = []

    click.echo('Loading WMO countries metadata')
    with open(wmo_countries) as jsonfile:
        countries_data = json.load(jsonfile)
        for row in countries_data['countries']:
            country_data = countries_data['countries'][row]
            if country_data['id'] != 'NUL':
                country = Country(country_data)
                country_models.append(country)

    click.echo('Loading local country updates metadata')
    with open(countries) as jsonfile:
        countries_data = json.load(jsonfile)
        for row in countries_data:
            country_data = countries_data[row]
            country = Country(country_data)
            country_models.append(country)

    click.echo('Loading datasets metadata')
    with open(datasets) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            dataset = Dataset(row)
            dataset_models.append(dataset)

    click.echo('Loading projects metadata')
    with open(projects) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            project = Project(row)
            project_models.append(project)

    click.echo('Loading contributors metadata')
    with open(contributors) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            contributor = Contributor(row)
            contributor_models.append(contributor)

    click.echo('Loading station names metadata')
    with open(station_names) as csvfile:
        reader = csv.DictReader(csvfile)
        records = unpack_station_names(reader)
        for obj in records:
            for field in obj:
                if obj[field] == '':
                    obj[field] = None
            station_name = StationName(obj)
            station_name_models.append(station_name)

    click.echo('Loading stations and ships metadata')
    with open(stations) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            station = Station(row)
            station_models.append(station)

    with open(ships) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            for field in row:
                if row[field] == '':
                    row[field] = None
            ship = Station(row)
            station_models.append(ship)

    click.echo('Loading instruments metadata')
    with open(instruments) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            instrument = Instrument(row)
            instrument_models.append(instrument)

    click.echo('Loading deployments metadata')
    with open(deployments) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            deployment = Deployment(row)
            deployment_models.append(deployment)

    click.echo('Storing projects in data registry')
    for model in project_models:
        registry_.save(model)
    click.echo('Storing datasets in data registry')
    for model in dataset_models:
        registry_.save(model)
    click.echo('Storing countries in data registry')
    for model in country_models:
        registry_.save(model)
    click.echo('Storing contributors in data registry')
    for model in contributor_models:
        registry_.save(model)
    click.echo('Storing station names in data registry')
    for model in station_name_models:
        registry_.save(model)
    click.echo('Storing stations in data registry')
    for model in station_models:
        registry_.save(model)
    click.echo('Storing instruments in data registry')
    for model in instrument_models:
        registry_.save(model)
    click.echo('Storing deployment records in data registry')
    for model in deployment_models:
        registry_.save(model)

    if init_search_index:
        search_index = SearchIndex()

        project_docs = [model.__geo_interface__ for model in project_models]
        dataset_docs = [model.__geo_interface__ for model in dataset_models]
        country_docs = [model.__geo_interface__ for model in country_models]
        station_docs = [model.__geo_interface__ for model in station_models]

        contributor_docs = \
            [model.__geo_interface__ for model in contributor_models]
        instrument_docs = \
            [model.__geo_interface__ for model in instrument_models]
        deployment_docs = \
            [model.__geo_interface__ for model in deployment_models]

        click.echo('Storing projects in search index')
        search_index.index(Project, project_docs)
        click.echo('Storing datasets in search index')
        search_index.index(Dataset, dataset_docs)
        click.echo('Storing countries in search index')
        search_index.index(Country, country_docs)
        click.echo('Storing contributors in search index')
        search_index.index(Contributor, contributor_docs)
        click.echo('Storing stations in search index')
        search_index.index(Station, station_docs)
        click.echo('Storing instruments in search index')
        search_index.index(Instrument, instrument_docs)
        click.echo('Storing deployments in search index')
        search_index.index(Deployment, deployment_docs)


@click.command('sync')
@click.pass_context
def sync(ctx):
    """Sync search index with data registry"""

    model_classes = [
        Project,
        Dataset,
        Country,
        Contributor,
        Station,
        Instrument,
        Deployment,
        DataRecord
    ]

    registry_ = registry.Registry()
    search_index = SearchIndex()

    search_index_config = config.EXTRAS.get('search_index', {})

    click.echo('Begin data registry backend sync on ', nl=False)
    for clazz in model_classes:
        plural_name = clazz.__tablename__
        plural_caps = ''.join(map(str.capitalize, plural_name.split('_')))

        enabled_flag = '{}_enabled'.format(plural_name)
        if not search_index_config.get(enabled_flag, True):
            click.echo('{} index frozen (skipping)'.format(plural_caps))
            continue

        click.echo('{}...'.format(plural_caps))

        registry_contents = registry_.query_full_index(clazz)
        registry_docs = [obj.__geo_interface__ for obj in registry_contents]

        click.echo('Sending models to search index...')
        search_index.index(clazz, registry_docs)
        click.echo('Purging excess models...')
        search_index.unindex_except(clazz, registry_docs)

    click.echo('Done')


search.add_command(sync)

registry__.add_command(setup)
registry__.add_command(teardown)
admin.add_command(init)

admin.add_command(registry__)
admin.add_command(search)
