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
# Copyright (c) 2024 Government of Canada
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
import copy

from pygeometa.core import read_mcf
from pygeometa.schemas.wmo_wcmp2 import WMOWCMP2OutputSchema

from sqlalchemy import (Boolean, Column, create_engine, Date, DateTime,
                        Float, Enum, ForeignKey, Integer, String, Time,
                        UniqueConstraint, ForeignKeyConstraint, ARRAY, Text,
                        inspect)
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

from elasticsearch.exceptions import (ConnectionError, RequestError)

from woudc_data_registry import config, registry
from woudc_data_registry.search import SearchIndex, search
from woudc_data_registry.util import (get_date, point2geojsongeometry,
                                      strftime_rfc3339)

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
                'wmo_membership': strftime_rfc3339(self.wmo_membership),
                'regional_involvement': self.regional_involvement,
                'link': self.link
            }
        }

    def __repr__(self):
        try:
            return f'Country ({self.country_id}, {self.name_en})'
        except AttributeError as e:
            return f'Error: Missing attributes - {e}'


class Contributor(base):
    """Data Registry Contributor"""

    __tablename__ = 'contributors'
    __table_args__ = (UniqueConstraint('contributor_id'),
                      UniqueConstraint('acronym', 'project_id'),)

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
                'start_date': strftime_rfc3339(self.start_date),
                'end_date': strftime_rfc3339(self.end_date),
                'last_validated_datetime':
                    strftime_rfc3339(self.last_validated_datetime)
            }
        }

    def __repr__(self):
        return f'Contributor ({self.contributor_id}, {self.name})'

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
    id_dependencies = ['dataset_name', 'dataset_level']

    dataset_id = Column(String, primary_key=True)

    data_class = Column(String, nullable=False)
    dataset_name = Column(String, nullable=False)
    dataset_level = Column(String, nullable=False)

    def __init__(self, dict_):
        self.data_class = dict_['data_class']
        self.dataset_level = str(float(dict_['dataset_level']))
        self.dataset_name = dict_['dataset_name']
        self.dataset_id = f"{self.dataset_name}_{self.dataset_level}"

    @property
    def __geo_interface__(self):
        return {
            'id': self.dataset_id,
            'type': 'Feature',
            'geometry': None,
            'properties': {
                'identifier': self.dataset_id,
                'data_class': self.data_class,
                'dataset_name': self.dataset_name,
                'dataset_level': self.dataset_level
            }
        }

    def __repr__(self):
        return f'Dataset ({self.dataset_id})'


class Instrument(base):
    """Data Registry Instrument"""

    __tablename__ = 'instruments'

    id_field = 'instrument_id'
    id_dependencies = ['name', 'model', 'serial',
                       'dataset_id', 'deployment_id']

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
        self.dataset_name = dict_['dataset_name']
        self.dataset_level = str(float(dict_['dataset_level']))
        self.dataset_form = str(dict_.get('dataset_form', '1'))  # noqa; defaults to '1' if empty/none/no Key found
        self.contributor = dict_['contributor']
        self.project = dict_['project']

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

        # dataset_form not saved in Instrument registry; defaults to form 1
        self.dataset_form = '1'

        dataset_folder = f'{self.dataset_id}_{self.dataset_form}'
        station_folder = f'{self.station.station_type.lower()}{self.station_id}'  # noqa
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
                'dataset_id': self.dataset_id,
                'contributor_name': self.deployment.contributor.name,
                'name': self.name,
                'model': self.model,
                'serial': self.serial,
                'start_date': strftime_rfc3339(self.start_date),
                'end_date': strftime_rfc3339(self.end_date),
                'waf_url': '/'.join([waf_basepath, 'Archive-NewFormat',
                                     dataset_folder, station_folder,
                                     instrument_folder])
            }
        }

    def __repr__(self):
        return f'Instrument ({self.instrument_id})'

    def generate_ids(self):
        """Builds and sets class ID field from other attributes"""
        self.dataset_id = f"{self.dataset_name}_{self.dataset_level}"

        if hasattr(self, 'contributor') and hasattr(self, 'project'):
            self.deployment_id = ':'.join([self.station_id, self.contributor,
                                          self.project])

        if all([hasattr(self, field) and getattr(self, field) is not None
                for field in self.id_dependencies]):
            components = [getattr(self, field)
                          for field in self.id_dependencies]
            self.instrument_id = ':'.join(map(str, components))


class DiscoveryMetadata(base):
    """Data Registry Discovery Metadata"""

    __tablename__ = 'discovery_metadata'

    id_field = 'discovery_metadata_id'
    id_dependencies = []  # No ID dependencies

    discovery_metadata_id = Column(String, primary_key=True)
    _metadata = Column(String, nullable=False)

    def __init__(self, dict_):
        self.discovery_metadata_id = dict_['id']
        self._metadata = json.dumps(dict_)

    @property
    def __geo_interface__(self):
        return json.loads(self._metadata)

    def __repr__(self):
        return f'Discovery Metadata ({self.discovery_metadata_id})'


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
        return f'Project ({self.project_id})'


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
        self.station_name_id = f"{self.station_id}:{dict_['station_name']}"
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
        gaw_pagename = f'0-20008-0-{self.gaw_id}'

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
                'start_date': strftime_rfc3339(self.start_date),
                'end_date': strftime_rfc3339(self.end_date),
                'last_validated_datetime':
                    strftime_rfc3339(self.last_validated_datetime),
                'gaw_url': f'{gaw_baseurl}/{gaw_pagename}'
            }
        }

    def __repr__(self):
        try:
            return f'Station ({self.station_id}, {self.station_name.name})'
        except AttributeError as e:
            return f'Error: Missing attributes - {e}'


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
        return f'Station name ({self.station_id}, {self.name})'

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
                'country_name_en': self.station.country.name_en,
                'country_name_fr': self.station.country.name_fr,
                'contributor_acronym': self.contributor.acronym,
                'contributor_name': self.contributor.name,
                'contributor_project': self.contributor.project_id,
                'contributor_url': self.contributor.url,
                'start_date': strftime_rfc3339(self.start_date),
                'end_date': strftime_rfc3339(self.end_date)
            }
        }

    def __repr__(self):
        return f'Deployment ({self.deployment_id})'

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
    content_category = Column(String, nullable=False)
    content_level = Column(String, nullable=False)
    content_form = Column(String, nullable=False)

    data_generation_date = Column(Date, nullable=False)
    data_generation_agency = Column(String, nullable=False)
    data_generation_version = Column(String, nullable=False)
    data_generation_scientific_authority = Column(String, nullable=True)

    dataset_id = Column(String, ForeignKey('datasets.dataset_id'),
                        nullable=False)
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

        self.dataset_id = f"{self.content_category}_{self.content_level}"

        self.deployment_id = ':'.join([
            self.station_id,
            self.data_generation_agency,
            self.content_class
        ])

        self.instrument_id = ':'.join([
            self.instrument_name,
            self.instrument_model,
            self.instrument_number,
            self.dataset_id,
            self.deployment_id
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
    def timestamp_utc(self):
        try:
            date = self.timestamp_date
            offset = datetime.datetime.strptime(
                    self.timestamp_utcoffset[1:len(self.timestamp_utcoffset)],
                    '%H:%M:%S').time()
            if self.timestamp_time is not None:
                time = self.timestamp_time
                dt = datetime.datetime.combine(date, time)
            else:
                dt = datetime.datetime.combine(
                        date, time=datetime.time(0, 0, 0))
            if self.timestamp_utcoffset[0] == '+':
                timestamp_utc = dt + datetime.timedelta(hours=offset.hour,
                                                        minutes=offset.minute,
                                                        seconds=offset.second)
            else:
                timestamp_utc = dt - datetime.timedelta(hours=offset.hour,
                                                        minutes=offset.minute,
                                                        seconds=offset.second)
            return timestamp_utc
        except Exception as err:
            LOGGER.error(err)
            return self.timestamp_date

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

        if 'UmkehrN14' in self.content_category:
            dataset_only = 'UmkehrN14'
        else:
            dataset_only = self.content_category

        datasetdirname = f'{dataset_only}_{self.content_level}_{self.content_form}'  # noqa

        url_tokens = [
            basepath.rstrip('/'),
            'Archive-NewFormat',
            datasetdirname, f'{self.platform_type.lower()}{self.station_id}',
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
                'dataset_id': self.dataset_id,

                'data_generation_date':
                    strftime_rfc3339(self.data_generation_date),
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
                'timestamp_date': strftime_rfc3339(self.timestamp_date),
                'timestamp_time': (None if self.timestamp_time is None
                                   else self.timestamp_time.isoformat()),
                'timestamp_utc': strftime_rfc3339(self.timestamp_utc),

                'published': self.published,
                'received_datetime': strftime_rfc3339(self.received_datetime),
                'inserted_datetime': strftime_rfc3339(self.inserted_datetime),
                'processed_datetime':
                    strftime_rfc3339(self.processed_datetime),
                'published_datetime':
                    strftime_rfc3339(self.published_datetime),

                'number_of_observations': self.number_of_observations,

                'ingest_filepath': self.ingest_filepath,
                'filename': self.filename,
                'output_filepath': self.output_filepath,

                'url': self.url
            }
        }

    def __repr__(self):
        return f'DataRecord({self.data_record_id}, {self.url})'


class Contribution(base):
    """Data Registry Contribution"""

    __tablename__ = 'contributions'

    id_field = 'contribution_id'
    id_dependencies = ['project_id', 'dataset_id', 'station_id',
                       'instrument_name']

    project_id = Column(String, ForeignKey('projects.project_id'),
                        nullable=False, default='WOUDC')
    contribution_id = Column(String, primary_key=True)
    dataset_id = Column(String, ForeignKey('datasets.dataset_id'),
                        nullable=False)
    station_id = Column(String, ForeignKey('stations.station_id'),
                        nullable=False)
    country_id = Column(String, ForeignKey('countries.country_id'),
                        nullable=False)
    instrument_name = Column(String, nullable=False)
    contributor_name = Column(String, nullable=False)

    start_date = Column(Date, nullable=False)
    end_date = Column(Date, nullable=True)

    station = relationship('Station', backref=__tablename__)
    country = relationship('Country', backref=__tablename__)
    dataset = relationship('Dataset', backref=__tablename__)

    def __init__(self, dict_):

        self.project_id = dict_['project_id']
        self.contribution_id = dict_['contribution_id']
        self.station_id = dict_['station_id']
        self.instrument_name = dict_['instrument_name']
        self.contributor_name = dict_['contributor_name']
        self.country_id = dict_['country_id']
        self.dataset_id = dict_['dataset_id']
        self.start_date = dict_['start_date']
        self.end_date = dict_['end_date']
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
        return {
            'id': self.contribution_id,
            'type': 'Feature',
            'geometry': point2geojsongeometry(self.station.x,
                                              self.station.y, self.station.z),
            'properties': {
                'identifier': self.contribution_id,
                'project_id': self.project_id,
                'dataset_id': self.dataset_id,
                'station_id': self.station_id,
                'name': self.station.station_name.name,
                'country_id': self.station.country_id,
                'country_name_en': self.station.country.name_en,
                'country_name_fr': self.station.country.name_fr,
                'instrument_name': self.instrument_name,
                'contributor_name': self.contributor_name,
                'start_date': self.start_date,
                'end_date': self.end_date
            }
        }

    def __repr__(self):
        return f'Contribution ({self.contribution_id})'

    def generate_ids(self):
        """Builds and sets class ID field from other attributes"""
        if all([hasattr(self, field) and getattr(self, field) is not None
                for field in self.id_dependencies]):
            components = [getattr(self, field)
                          for field in self.id_dependencies]
            self.contribution_id = ':'.join(map(str, components))


class Notification(base):
    """Data Registry News Item"""

    __tablename__ = 'notifications'

    id_field = 'notification_id'
    id_dependencies = ['title_en', 'published_date']

    notification_id = Column(String, primary_key=True)

    title_en = Column(String, nullable=False)
    title_fr = Column(String, nullable=False)

    description_en = Column(String, nullable=False)
    description_fr = Column(String, nullable=False)

    keywords_en = Column(String, nullable=False)
    keywords_fr = Column(String, nullable=False)

    published_date = Column(Date, nullable=False)
    banner = Column(Boolean, nullable=False, default=False)
    visible = Column(Boolean, nullable=False, default=True)

    x = Column(Float, nullable=False)
    y = Column(Float, nullable=False)

    def __init__(self, dict_):
        """serializer"""

        self.title_en = dict_['title_en']
        self.title_fr = dict_['title_fr']

        self.description_en = dict_['description_en']
        self.description_fr = dict_['description_fr']

        self.set_keywords_en(dict_['keywords_en'])
        self.set_keywords_fr(dict_['keywords_fr'])

        self.banner = dict_.get('banner', False)
        self.visible = dict_.get('visible', True)

        self.x = dict_['x']
        self.y = dict_['y']

        published_value = dict_['published']
        if isinstance(published_value, str):
            self.published_date = datetime.datetime.strptime(
                published_value[:10],
                '%Y-%m-%d')
        else:
            self.published_date = published_value

        # Normalize to start-of-day UTC for generating notification_id
        published_normalized = self.published_date.replace(hour=0,
                                                           minute=0,
                                                           second=0,
                                                           microsecond=0)
        self.notification_id = strftime_rfc3339(published_normalized)

    def get_keywords_en(self):
        if isinstance(self.keywords_en, str):
            return self.keywords_en.split(',')
        elif isinstance(self.keywords_en, list):
            return self.keywords_en
        else:
            LOGGER.error(
                "Unexpected type for self.keywords_en: %s",
                type(self.keywords_en).__name__
            )
            raise TypeError(
                f"Expected self.keywords_en to be str or list, "
                f"got {type(self.keywords_en).__name__}"
            )

    def set_keywords_en(self, keywords):
        self.keywords_en = ','.join(keywords)

    def get_keywords_fr(self):
        if isinstance(self.keywords_fr, str):
            return self.keywords_fr.split(',')
        elif isinstance(self.keywords_fr, list):
            return self.keywords_fr
        else:
            LOGGER.error(
                "Unexpected type for self.keywords_fr: %s",
                type(self.keywords_fr).__name__
            )
            raise TypeError(
                f"Expected self.keywords_fr to be str or list, "
                f"got {type(self.keywords_fr).__name__}"
            )

    def set_keywords_fr(self, keywords):
        self.keywords_fr = ','.join(keywords)

    @property
    def __geo_interface__(self):
        return {
            'id': self.notification_id,
            'type': 'Feature',
            'geometry': point2geojsongeometry(self.x, self.y),
            'properties': {
                'title_en': self.title_en,
                'title_fr': self.title_fr,
                'description_en': self.description_en,
                'description_fr': self.description_fr,
                'keywords_en': self.get_keywords_en(),
                'keywords_fr': self.get_keywords_fr(),
                'published_date': strftime_rfc3339(self.published_date),
                'banner': self.banner,
                'visible': self.visible
            }
        }

    def __repr__(self):
        return f'Notification ({self.notification_id})'


class PeerDataRecord(base):
    """Data Registry Peer Data Record"""

    __tablename__ = 'peer_data_records'
    __table_args__ = (UniqueConstraint('url'),)

    id_field = 'url'

    source = Column(String, nullable=False)
    measurement = Column(String, nullable=False)
    station_id = Column(String, nullable=False)
    station_name_id = Column(String,
                             ForeignKey('station_names.station_name_id'),
                             nullable=False)
    stn_type_enum = Enum('land', 'landFixed', 'landOnIce', name='stn_type')

    station_type = Column(stn_type_enum, nullable=False, default='land')
    contributor_id = Column(String, ForeignKey('contributors.contributor_id'),
                            nullable=True)
    gaw_id = Column(String, nullable=True)
    country_id = Column(String, ForeignKey('countries.country_id'),
                        nullable=False)
    instrument_type = Column(String, nullable=False)
    level = Column(String, nullable=False)
    pi = Column(String)
    pi_name = Column(String)
    pi_email = Column(String)

    url = Column(String, nullable=False, primary_key=True)

    start_datetime = Column(Date, nullable=False)
    end_datetime = Column(Date, nullable=False)

    x = Column(Float, nullable=False)
    y = Column(Float, nullable=False)
    z = Column(Float, nullable=False)

    # relationships
    station_name = relationship('StationName', backref=__tablename__)
    contributor = relationship('Contributor', backref=__tablename__)

    # data management fields

    published = Column(Boolean, nullable=False, default=True)

    received_datetime = Column(DateTime, nullable=False,
                               default=datetime.datetime.utcnow())

    inserted_datetime = Column(DateTime, nullable=False,
                               default=datetime.datetime.utcnow())

    processed_datetime = Column(DateTime, nullable=False,
                                default=datetime.datetime.utcnow())

    published_datetime = Column(DateTime, nullable=False,
                                default=datetime.datetime.utcnow())

    last_validated_datetime = Column(DateTime, nullable=False,
                                     default=datetime.datetime.utcnow())

    def __init__(self, dict_):
        """serializer"""

        self.source = dict_['source']
        self.measurement = dict_['measurement']

        self.contributor_id = dict_['contributor_id']
        self.station_id = dict_['station_id']
        self.station_name_id = f"{self.station_id}:{dict_['station_name']}"
        self.station_type = dict_['station_type']
        self.country_id = dict_['country_id']
        self.gaw_id = dict_.get('gaw_id')
        self.instrument_type = dict_['instrument_type']
        self.level = dict_['level']
        self.pi_name = dict_.get('pi_name')
        self.pi_email = dict_.get('pi_email')
        self.url = dict_['url']
        self._name = dict_['station_name']

        try:
            self.start_datetime = get_date(dict_['start_datetime'])
            self.end_datetime = get_date(dict_['end_datetime'])
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
    def es_id(self):
        if self.source == 'eubrewnet':
            return ''.join(self.url.split('%')[5:10])
        else:
            return '.'.join(self.url.split('/')[9:11])

    @property
    def contributor_url(self):
        if self.contributor is not None:
            return self.contributor.url

    @property
    def __geo_interface__(self):
        gaw_baseurl = 'https://gawsis.meteoswiss.ch/GAWSIS/index.html#' \
            '/search/station/stationReportDetails'
        gaw_pagename = f'0-20008-0-{self.gaw_id}'

        return {
            'id': self.es_id,
            'type': 'Feature',
            'geometry': point2geojsongeometry(self.x, self.y, self.z),
            'properties': {
                'identifier': self.es_id,
                'source': self.source,
                'measurement': self.measurement,
                'station_id': self.station_id,
                'station_name': self.name,
                'station_type': self.station_type,
                'gaw_url': f'{gaw_baseurl}/{gaw_pagename}',
                'gaw_id': self.gaw_id,
                'contributor_id': self.contributor_id,
                'contributor_url':
                self.contributor_url,
                'country_id': self.country_id,
                'instrument_type': self.instrument_type,
                'level': self.level,
                'start_datetime': strftime_rfc3339(self.start_datetime),
                'end_datetime': strftime_rfc3339(self.end_datetime),
                'last_validated_datetime':
                    strftime_rfc3339(self.last_validated_datetime),
                'url': self.url
            }
        }

    def __repr__(self):
        return f'PeerDataRecord({self.url})'


class UVIndex(base):
    """Data Registry UV Index"""

    __tablename__ = 'uv_index_hourly'

    id_field = 'uv_id'
    id_dependencies = ['instrument_id', 'observation_date',
                       'observation_time']

    uv_id = Column(String, primary_key=True)
    file_path = Column(String, nullable=False)
    url = Column(String, nullable=False)
    dataset_id = Column(String, ForeignKey('datasets.dataset_id'),
                        nullable=False)
    station_id = Column(String, ForeignKey('stations.station_id'),
                        nullable=False)
    country_id = Column(String, ForeignKey('countries.country_id'),
                        nullable=False)
    instrument_id = Column(String, ForeignKey('instruments.instrument_id'),
                           nullable=False)

    gaw_id = Column(String, nullable=True)

    solar_zenith_angle = Column(Float, nullable=True)
    uv_index = Column(Float, nullable=True)
    uv_daily_max = Column(Float, nullable=True)
    uv_index_qa = Column(String, nullable=True)

    observation_date = Column(Date, nullable=False)
    observation_time = Column(Time, nullable=False)
    observation_utcoffset = Column(String, nullable=True)

    x = Column(Float, nullable=True)
    y = Column(Float, nullable=True)
    z = Column(Float, nullable=True)

    # relationships
    station = relationship('Station', backref=__tablename__)
    instrument = relationship('Instrument', backref=__tablename__)
    dataset = relationship('Dataset', backref=__tablename__)

    def __init__(self, dict_):

        self.file_path = dict_['file_path']

        self.dataset_id = dict_['dataset_id']
        self.station_id = dict_['station_id']
        self.country_id = dict_['country_id']
        self.instrument_id = dict_['instrument_id']

        self.gaw_id = dict_['gaw_id']

        self.solar_zenith_angle = dict_['solar_zenith_angle']

        self.uv_index = dict_['uv_index']
        self.uv_daily_max = dict_['uv_daily_max']
        self.uv_index_qa = dict_['uv_index_qa']

        self.observation_date = dict_['observation_date']
        self.observation_time = dict_['observation_time']

        self.generate_ids()

        self.observation_utcoffset = dict_['observation_utcoffset']

        self.url = self.get_waf_path(dict_)

        self.x = dict_['x']
        self.y = dict_['y']
        self.z = dict_['z']

    @property
    def timestamp_utc(self):
        try:
            date = self.observation_date
            offset = datetime.datetime.strptime(
                    self.observation_utcoffset[
                        1:len(self.observation_utcoffset)
                    ],
                    '%H:%M:%S').time()
            time = self.observation_time
            dt = datetime.datetime.combine(date, time)
            if self.observation_utcoffset[0] == '+':
                timestamp_utc = dt + datetime.timedelta(hours=offset.hour,
                                                        minutes=offset.minute,
                                                        seconds=offset.second)
            else:
                timestamp_utc = dt - datetime.timedelta(hours=offset.hour,
                                                        minutes=offset.minute,
                                                        seconds=offset.second)
            return timestamp_utc
        except Exception as err:
            LOGGER.error(err)
            return self.observation_date

    def get_waf_path(self, dict_):
        """generate WAF url"""

        datasetdirname = f"{self.dataset_id}_{dict_['dataset_form']}"

        timestamp_date = datetime.datetime.strptime(
            dict_['timestamp_date'], '%Y-%m-%d').date()
        url_tokens = [
            config.WDR_WAF_BASEURL.rstrip('/'),
            'Archive-NewFormat',
            datasetdirname,
            '{}{}'.format(dict_['station_type'].lower(), self.station_id),
            dict_['instrument_name'].lower(),
            timestamp_date.strftime('%Y'),
            dict_['filename']
        ]

        return '/'.join(url_tokens)

    @property
    def __geo_interface__(self):
        gaw_baseurl = 'https://gawsis.meteoswiss.ch/GAWSIS/index.html#' \
            '/search/station/stationReportDetails'
        gaw_pagename = '0-20008-0-{}'.format(self.station.gaw_id)

        return {
            'id': self.uv_id,
            'type': 'Feature',
            'geometry': point2geojsongeometry(self.x, self.y, self.z),
            'properties': {
                'identifier': self.uv_id,
                'file_path': self.file_path,
                'dataset_id': self.dataset_id,
                'station_id': self.station_id,
                'station_name': self.station.station_name.name,
                'station_gaw_id': self.station.gaw_id,
                'station_gaw_url': f'{gaw_baseurl}/{gaw_pagename}',
                'contributor_name':
                self.instrument.deployment.contributor.name,
                'contributor_id':
                self.instrument.deployment.contributor.contributor_id,
                'contributor_url':
                self.instrument.deployment.contributor.url,
                'country_id': self.station.country.country_id,
                'country_name_en': self.station.country.name_en,
                'country_name_fr': self.station.country.name_fr,
                'gaw_id': self.gaw_id,
                'solar_zenith_angle': self.solar_zenith_angle,
                'observation_utcoffset': self.observation_utcoffset,
                'observation_date': strftime_rfc3339(self.observation_date),
                'observation_time': strftime_rfc3339(self.observation_time),
                'timestamp_utc': strftime_rfc3339(self.timestamp_utc),
                'instrument_name': self.instrument.name,
                'instrument_model': self.instrument.model,
                'instrument_serial': self.instrument.serial,
                'uv_index': self.uv_index,
                'uv_daily_max': self.uv_daily_max,
                'uv_index_qa': self.uv_index_qa,
                'url': self.url,
            }
        }

    def __repr__(self):
        return f'UV_Index ({self.uv_id})'

    def generate_ids(self):
        """Builds and sets class ID field from other attributes"""

        if all([hasattr(self, field) and getattr(self, field) is not None
                for field in self.id_dependencies]):
            components = [getattr(self, field)
                          for field in self.id_dependencies]
            self.uv_id = ':'.join(map(str, components))


class TotalOzone(base):
    """Data Registry TotalOzone model"""

    __tablename__ = 'totalozone'

    id_field = 'ozone_id'
    id_dependencies = ['instrument_id', 'daily_date', 'file_name']

    ozone_id = Column(String, primary_key=True)
    file_path = Column(String, nullable=False)
    file_name = Column(String, nullable=False)
    url = Column(String, nullable=False)
    dataset_id = Column(String, ForeignKey('datasets.dataset_id'),
                        nullable=False)
    station_id = Column(String, ForeignKey('stations.station_id'),
                        nullable=False)
    country_id = Column(String, ForeignKey('countries.country_id'),
                        nullable=False)
    instrument_id = Column(String, ForeignKey('instruments.instrument_id'),
                           nullable=False)

    gaw_id = Column(String, nullable=True)

    observation_date = Column(Date, nullable=False)
    daily_date = Column(Date, nullable=False)
    daily_wlcode = Column(String, nullable=True)
    daily_obscode = Column(String, nullable=True)
    daily_columno3 = Column(Float, nullable=False)
    daily_stdevo3 = Column(Float, nullable=True)
    daily_utc_begin = Column(String, nullable=True)
    daily_utc_end = Column(String, nullable=True)
    daily_utc_mean = Column(String, nullable=True)
    daily_nobs = Column(Float, nullable=True)
    daily_mmu = Column(String, nullable=True)
    daily_columnso2 = Column(Float, nullable=True)
    monthly_date = Column(Date, nullable=True)
    monthly_columno3 = Column(Float, nullable=True)
    monthly_stdevo3 = Column(Float, nullable=True)
    monthly_npts = Column(Float, nullable=True)

    x = Column(Float, nullable=True)
    y = Column(Float, nullable=True)
    z = Column(Float, nullable=True)

    # relationships
    station = relationship('Station', backref=__tablename__)
    instrument = relationship('Instrument', backref=__tablename__)
    dataset = relationship('Dataset', backref=__tablename__)

    def __init__(self, dict_):

        self.file_path = dict_['file_path']
        self.file_name = dict_['filename']

        self.dataset_id = dict_['dataset_id']
        self.station_id = dict_['station_id']
        self.country_id = dict_['country_id']
        self.instrument_id = dict_['instrument_id']

        self.observation_date = dict_['observation_date']

        self.daily_date = dict_['date']
        self.daily_wlcode = dict_['wlcode']
        self.daily_obscode = dict_['obscode']
        self.daily_columno3 = dict_['columno3']
        self.daily_stdevo3 = dict_['stddevo3']
        self.daily_utc_begin = dict_['utc_begin']
        self.daily_utc_end = dict_['utc_end']
        self.daily_utc_mean = dict_['utc_mean']
        self.daily_nobs = dict_['nobs']
        self.daily_mmu = dict_['mmu']
        self.daily_columnso2 = dict_['columnso2']

        self.monthly_date = dict_['monthly_date']
        self.monthly_columno3 = dict_['monthly_columno3']
        self.monthly_stdevo3 = dict_['monthly_stdevo3']
        self.monthly_npts = dict_['npts']

        self.url = self.get_waf_path(dict_)

        self.generate_ids()

        self.x = dict_['x']
        self.y = dict_['y']
        self.z = dict_['z']

    def get_waf_path(self, dict_):
        """generate WAF url"""

        datasetdirname = f"{self.dataset_id}_{dict_['dataset_form']}"
        timestamp_date = datetime.datetime.strptime(
            dict_['timestamp_date'], '%Y-%m-%d').date()
        url_tokens = [
            config.WDR_WAF_BASEURL.rstrip('/'),
            'Archive-NewFormat',
            datasetdirname,
            f"{dict_['station_type'].lower()}{self.station_id}",
            dict_['instrument_name'].lower(),
            timestamp_date.strftime('%Y'),
            self.file_name
        ]

        return '/'.join(url_tokens)

    @property
    def __geo_interface__(self):
        gaw_baseurl = 'https://gawsis.meteoswiss.ch/GAWSIS/index.html#' \
            '/search/station/stationReportDetails'
        gaw_pagename = f'0-20008-0-{self.station.gaw_id}'

        return {
            'id': self.ozone_id,
            'type': 'Feature',
            'geometry': point2geojsongeometry(self.x, self.y, self.z),
            'properties': {
                'identifier': self.ozone_id,
                'file_path': self.file_path,
                'dataset_id': self.dataset_id,
                'station_id': self.station_id,
                'station_name': self.station.station_name.name,
                'station_gaw_id': self.station.gaw_id,
                'station_gaw_url': f'{gaw_baseurl}/{gaw_pagename}',
                'contributor_name':
                self.instrument.deployment.contributor.name,
                'contributor_id':
                self.instrument.deployment.contributor.contributor_id,
                'contributor_url':
                self.instrument.deployment.contributor.url,
                'country_id': self.station.country.country_id,
                'country_name_en': self.station.country.name_en,
                'country_name_fr': self.station.country.name_fr,
                'gaw_id': self.gaw_id,
                'instrument_name': self.instrument.name,
                'instrument_model': self.instrument.model,
                'instrument_serial': self.instrument.serial,
                'observation_date': strftime_rfc3339(self.observation_date),
                'daily_date': strftime_rfc3339(self.daily_date),
                'daily_wlcode': self.daily_wlcode,
                'daily_obscode': self.daily_obscode,
                'daily_columno3': self.daily_columno3,
                'daily_stdevo3': self.daily_stdevo3,
                'daily_utc_begin': self.daily_utc_begin,
                'daily_utc_end': self.daily_utc_end,
                'daily_utc_mean': self.daily_utc_mean,
                'daily_nobs': self.daily_nobs,
                'daily_mmu': self.daily_mmu,
                'daily_columnso2': self.daily_columnso2,
                'monthly_date': strftime_rfc3339(self.monthly_date),
                'monthly_columno3': self.monthly_columno3,
                'monthly_stdevo3': self.monthly_stdevo3,
                'monthly_npts': self.monthly_npts,
                'url': self.url,
            }
        }

    def __repr__(self):
        return f'TotalOzone ({self.ozone_id})'

    def generate_ids(self):
        """Builds and sets class ID field from other attributes"""

        if all([hasattr(self, field) and getattr(self, field) is not None
                for field in self.id_dependencies]):
            components = [getattr(self, field)
                          for field in self.id_dependencies]
            self.ozone_id = ':'.join(map(str, components))


class OzoneSonde(base):
    """Data Registry OzoneSonde model"""

    __tablename__ = 'ozonesonde'

    id_field = 'ozone_id'
    id_dependencies = ['instrument_id', 'timestamp_date', 'file_name']

    ozone_id = Column(String, primary_key=True)
    file_path = Column(String, nullable=False)
    file_name = Column(String, nullable=False)
    url = Column(String, nullable=False)
    dataset_id = Column(String, ForeignKey('datasets.dataset_id'),
                        nullable=False)
    station_id = Column(String, ForeignKey('stations.station_id'),
                        nullable=False)
    country_id = Column(String, ForeignKey('countries.country_id'),
                        nullable=False)
    instrument_id = Column(String, ForeignKey('instruments.instrument_id'),
                           nullable=False)

    flight_integratedo3 = Column(String, nullable=True)
    flight_correctioncode = Column(String, nullable=True)
    flight_sondetotalo3 = Column(String, nullable=True)
    flight_correctionfactor = Column(String, nullable=True)
    flight_totalo3 = Column(String, nullable=True)
    flight_wlcode = Column(String, nullable=True)
    flight_obstype = Column(String, nullable=True)

    profile_pressure = Column(ARRAY(String), nullable=True)
    profile_o3partialpressure = Column(ARRAY(String), nullable=True)
    profile_temperature = Column(ARRAY(String), nullable=True)
    profile_windspeed = Column(ARRAY(String), nullable=True)
    profile_winddirection = Column(ARRAY(String), nullable=True)
    profile_levelcode = Column(ARRAY(String), nullable=True)
    profile_duration = Column(ARRAY(String), nullable=True)
    profile_gpheight = Column(ARRAY(String), nullable=True)
    profile_relativehumidity = Column(ARRAY(String), nullable=True)
    profile_sampletemperature = Column(ARRAY(String), nullable=True)

    timestamp_date = Column(Date, nullable=True)

    x = Column(Float, nullable=True)
    y = Column(Float, nullable=True)
    z = Column(Float, nullable=True)

    # relationships
    station = relationship('Station', backref=__tablename__)
    instrument = relationship('Instrument', backref=__tablename__)
    dataset = relationship('Dataset', backref=__tablename__)

    def __init__(self, dict_):

        self.file_path = dict_['file_path']
        self.file_name = dict_['filename']

        self.dataset_id = dict_['dataset_id']
        self.station_id = dict_['station_id']
        self.country_id = dict_['country_id']
        self.instrument_id = dict_['instrument_id']
        self.timestamp_date = dict_['timestamp_date']

        self.flight_integratedo3 = dict_['integratedo3']
        self.flight_correctioncode = dict_['correctioncode']
        self.flight_sondetotalo3 = dict_['sondetotalo3']
        self.flight_correctionfactor = dict_['correctionfactor']
        self.flight_totalo3 = dict_['totalo3']
        self.flight_wlcode = dict_['wlcode']
        self.flight_obstype = dict_['obstype']

        self.profile_pressure = dict_['profile_pressure']
        self.profile_o3partialpressure = dict_['o3partialpressure']
        self.profile_temperature = dict_['temperature']
        self.profile_windspeed = dict_['windspeed']
        self.profile_winddirection = dict_['winddirection']
        self.profile_levelcode = dict_['levelcode']
        self.profile_duration = dict_['duration']
        self.profile_gpheight = dict_['gpheight']
        self.profile_relativehumidity = dict_['relativehumidity']
        self.profile_sampletemperature = dict_['sampletemperature']

        self.url = self.get_waf_path(dict_)

        self.generate_ids()

        self.x = dict_['x']
        self.y = dict_['y']
        self.z = dict_['z']

    def get_waf_path(self, dict_):
        """generate WAF url"""

        datasetdirname = f"{self.dataset_id}_{dict_['dataset_form']}"
        timestamp_date = datetime.datetime.strptime(
            dict_['timestamp_date'], '%Y-%m-%d').date()
        url_tokens = [
            config.WDR_WAF_BASEURL.rstrip('/'),
            'Archive-NewFormat',
            datasetdirname,
            f"{dict_['station_type'].lower()}{self.station_id}",
            dict_['instrument_name'].lower(),
            timestamp_date.strftime('%Y'),
            self.file_name
        ]

        return '/'.join(url_tokens)

    @property
    def __geo_interface__(self):
        gaw_baseurl = 'https://gawsis.meteoswiss.ch/GAWSIS/index.html#' \
            '/search/station/stationReportDetails'
        gaw_pagename = f'0-20008-0-{self.station.gaw_id}'

        return {
            'id': self.ozone_id,
            'type': 'Feature',
            'geometry': point2geojsongeometry(self.x, self.y, self.z),
            'properties': {
                'identifier': self.ozone_id,
                'file_path': self.file_path,
                'dataset_id': self.dataset_id,
                'station_id': self.station_id,
                'station_name': self.station.station_name.name,
                'station_gaw_id': self.station.gaw_id,
                'station_gaw_url': f'{gaw_baseurl}/{gaw_pagename}',
                'contributor_name':
                self.instrument.deployment.contributor.name,
                'contributor_id':
                self.instrument.deployment.contributor.contributor_id,
                'contributor_url':
                self.instrument.deployment.contributor.url,
                'country_id': self.station.country.country_id,
                'country_name_en': self.station.country.name_en,
                'country_name_fr': self.station.country.name_fr,
                'pressure': self.profile_pressure,
                'o3partialpressure': self.profile_o3partialpressure,
                'temperature': self.profile_temperature,
                'instrument_name': self.instrument.name,
                'instrument_model': self.instrument.model,
                'instrument_serial': self.instrument.serial,
                'timestamp_date': strftime_rfc3339(self.timestamp_date),
                'url': self.url,
            }
        }

    def __repr__(self):
        return f'OzoneSonde ({self.ozone_id})'

    def generate_ids(self):
        """Builds and sets class ID field from other attributes"""

        if all([hasattr(self, field) and getattr(self, field) is not None
                for field in self.id_dependencies]):
            components = [getattr(self, field)
                          for field in self.id_dependencies]
            self.ozone_id = ':'.join(map(str, components))


class StationDobsonCorrections(base):
    """"""
    __tablename__ = 'station_dobson_corrections'

    id_field = "dobson_correction_id"
    id_dependencies = ['station_id', 'AD_correcting_source',
                       'CD_correcting_factor']

    # columns
    dobson_correction_id = Column(String, primary_key=True)
    station_id = Column(String(255), ForeignKey('stations.station_id'),
                        nullable=False)
    AD_corrected = Column(Boolean, nullable=False, default=False)
    CD_corrected = Column(Boolean, nullable=False, default=False)
    AD_correcting_source = Column(String(255), nullable=False)
    CD_correcting_source = Column(String(255), nullable=False)
    CD_correcting_factor = Column(String(255), nullable=False, default='cd')
    correction_comments = Column(Text, nullable=False)

    # relationshipts
    station = relationship('Station', backref=__tablename__)

    def __init__(self, dict_):
        self.station_id = dict_['station']
        self.AD_corrected = bool(dict_['AD_corrected'])
        self.CD_corrected = bool(dict_['CD_corrected'])
        self.AD_correcting_source = dict_['AD_correcting_source']
        self.CD_correcting_source = dict_['CD_correcting_source']
        self.CD_correcting_factor = dict_['CD_correcting_factor']
        self.correction_comments = dict_['correction_comments']

        self.generate_ids()

    @property
    def __geo_interface__(self):
        return {
            'id': self.dobson_correction_id,
            'type': 'Feature',
            'geometry': point2geojsongeometry(self.station.x, self.station.y,
                                              self.station.z),
            'properties': {
                'identifier': self.dobson_correction_id,
                'station_id': self.station_id,
                'AD_corrected': self.AD_corrected,
                'CD_corrected': self.CD_corrected,
                'AD_correcting_source': self.AD_correcting_source,
                'CD_correcting_source': self.CD_correcting_source,
                'CD_correcting_factor': self.CD_correcting_factor,
                'correction_comments': self.correction_comments
            }
        }

    def __repr__(self):
        return f'Station Dobson Correction ({self.dobson_correction_id})'

    def generate_ids(self):
        """Builds and sets class ID field from other attributes"""

        if all([hasattr(self, field) and getattr(self, field) is not None
                for field in self.id_dependencies]):
            components = [getattr(self, field)
                          for field in self.id_dependencies]
            self.dobson_correction_id = (
                f"{self.station_id}:AD-{components[1]}:CD-{components[2]}"
            )


def build_contributions(instrument_models):
    """function that forms contributions from other model lists"""

    # List to store the final contribution_models
    contribution_models = []

    # contribution dict used to check for duplicate contribution id
    contribution_dict = {}

    for instrument in instrument_models:

        # station info
        station_id = instrument.station.station_id
        # country info
        country_id = instrument.station.country.country_id

        # instrument info
        instrument_name = instrument.name
        start_date = instrument.start_date
        end_date = instrument.end_date
        dataset_id = instrument.dataset_id

        # now access the project from contributor
        contributor_name = instrument.deployment.contributor.name
        project_id = instrument.deployment.contributor.project_id

        # form the contribution id by combining the
        # strings present in Contributions dependencies
        contribution_id = ':'.join([project_id, dataset_id,
                                    station_id, instrument_name])

        # check if contribution id is in the index already
        if contribution_id in contribution_dict.keys():

            # if it is then update the start
            # and end date for that contribution id
            # since the dict points to the list can just update the dict
            # only update start date if less than the current start date
            # only update end date if greater than the current end date
            if start_date < contribution_dict[contribution_id].start_date:

                contribution_dict[contribution_id].start_date = start_date

            elif contribution_dict[contribution_id].end_date is not None \
                    and end_date is not None:

                if end_date > contribution_dict[contribution_id].end_date:

                    contribution_dict[contribution_id].end_date = end_date

            elif contribution_dict[contribution_id] is None \
                    and end_date is not None:

                contribution_dict[contribution_id].end_date = end_date

        else:

            # otherwise create a new contribution
            # create dictionary for creating object

            data = {'contribution_id': contribution_id,
                    'project_id': project_id,
                    'dataset_id': dataset_id,
                    'station_id': station_id,
                    'country_id': country_id,
                    'instrument_name': instrument_name,
                    'contributor_name': contributor_name,
                    'start_date': start_date,
                    'end_date': end_date,
                    }

            contribution = Contribution(data)
            contribution_models.append(contribution)
            contribution_dict[contribution_id] = contribution

    return contribution_models


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


@click.command('config')
@click.pass_context
def show_config(ctx):

    masked = None

    env_vars = [
        'WDR_LOGGING_LOGLEVEL',
        'WDR_LOGGING_LOGFILE',
        'WDR_DB_DEBUG',
        'WDR_DB_TYPE',
        'WDR_DB_HOST',
        'WDR_DB_PORT',
        'WDR_DB_USERNAME',
        'WDR_DB_PASSWORD',
        'WDR_DB_NAME',
        'WDR_SEARCH_TYPE',
        'WDR_SEARCH_URL',
        'WDR_WAF_BASEDIR',
        'WDR_WAF_BASEURL',
        'WDR_ERROR_CONFIG',
        'WDR_ALIAS_CONFIG',
        'WDR_EXTRA_CONFIG',
        'WDR_DATABASE_URL'
    ]

    for env_var in env_vars:
        if env_var == 'WDR_DB_PASSWORD':
            masked = '*' * len(getattr(config, env_var))
            s = '{env_var}: {masked}'
        elif env_var == 'WDR_DATABASE_URL' and config.WDR_DB_TYPE == 'postgresql':  # noqa
            value = config.WDR_DATABASE_URL.replace(config.WDR_DB_PASSWORD, masked)  # noqa
            s = f'{env_var}: {value}'
        else:
            s = f'{env_var}: {getattr(config, env_var)}'

        click.echo(s)


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
        click.echo(f'ERROR: {err}')


@click.command()
@click.pass_context
@click.option('--datadir', '-d',
              type=click.Path(exists=True, resolve_path=True),
              help='Path to core metadata files')
def setup_dobson_correction(ctx, datadir):
    """ Add the station Dobson correction table and ES index to the
    database and ES, respectively, that does not have it. """
    from woudc_data_registry import config
    import os
    from woudc_data_registry.search import MAPPINGS, SearchIndex

    click.echo("Setting up dobson correction table and index")

    registry_ = registry.Registry()

    engine = create_engine(config.WDR_DATABASE_URL, echo=config.WDR_DB_DEBUG)

    inspector = inspect(engine)
    if "station_dobson_corrections" in inspector.get_table_names():
        response = input(
            'Table already exists. Teardown and setup this table? (y/n): ')
        if response.lower() == 'y':
            click.echo('Deleting current StationDobsonCorrections table')
            registry_.session.query(StationDobsonCorrections).delete()
            registry_.save()
            try:
                click.echo(
                    f'Generating model: '
                    f'{StationDobsonCorrections.__tablename__}'
                )
                StationDobsonCorrections.__table__.create(
                    engine, checkfirst=True)
                click.echo('Done')
            except (OperationalError, ProgrammingError) as err:
                click.echo(f'ERROR: {err}')

            station_dobson_corrections = os.path.join(
                datadir, 'station_dobson_corrections.csv')

            station_dobson_corrections_models = []

            click.echo('Loading station dobson corrections items')
            with open(station_dobson_corrections) as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    station_dobson_corrections = StationDobsonCorrections(row)
                    station_dobson_corrections_models.append(
                        station_dobson_corrections)

            click.echo('Storing station dobson corrections items in registry')
            for model in station_dobson_corrections_models:
                registry_.save(model)
        else:
            click.echo("Skipping teardown of the "
                       "StationDobsonCorrections table")

    click.echo('Creating ES index for station dobson corrections')
    search_index = SearchIndex()

    index_name = search_index.generate_index_name(
        MAPPINGS['station_dobson_corrections']['index']
    )
    settings = {
                'mappings': {
                    'properties': {
                        'geometry': {
                            'type': 'geo_shape'
                        }
                    }
                },
                'settings': {
                    'index': {
                        'number_of_shards': 1,
                        'number_of_replicas': 0
                    }
                }
            }
    if 'properties' in MAPPINGS['station_dobson_corrections']:
        settings['mappings']['properties']['properties'] = {
            'properties': MAPPINGS['station_dobson_corrections']['properties']
        }

    try:
        if search_index.connection.indices.exists(index=index_name):
            response = input(
                'Index already exists. Teardown and setup this index? (y/n): '
            )
            if response.lower() == 'y':
                search_index.connection.indices.delete(
                    index=index_name, ignore=[400, 404]
                )
                search_index.connection.indices.create(
                    index=index_name, body=settings
                )
                click.echo('ES index created: station_dobson_corrections')
            else:
                click.echo("Skipping teardown of the "
                           "StationDobsonCorrections index")
    except (ConnectionError, RequestError) as err:
        click.echo(f'ERROR: {err}')

    click.echo("Done")


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
        click.echo(f'ERROR: {err}')


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
    countries = os.path.join(datadir, 'init', 'countries.json')
    contributors = os.path.join(datadir, 'contributors.csv')
    stations = os.path.join(datadir, 'stations.csv')
    ships = os.path.join(datadir, 'init', 'ships.csv')
    station_names = os.path.join(datadir, 'station-names.csv')
    datasets = os.path.join(datadir, 'datasets.csv')
    projects = os.path.join(datadir, 'projects.csv')
    instruments = os.path.join(datadir, 'instruments.csv')
    deployments = os.path.join(datadir, 'deployments.csv')
    notifications = os.path.join(datadir, 'notifications.csv')
    discovery_metadata = os.path.join(datadir, 'init', 'discovery-metadata')
    station_dobson_corrections = os.path.join(
        datadir, 'station_dobson_corrections.csv')

    registry_ = registry.Registry()

    project_models = []
    dataset_models = []
    country_models = []
    contributor_models = []
    station_models = []
    station_name_models = []
    instrument_models = []
    deployment_models = []
    contribution_models = []
    notification_models = []
    discovery_metadata_models = []
    station_dobson_corrections_models = []

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

    click.echo('Loading deployments metadata')
    with open(deployments) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            deployment = Deployment(row)
            deployment_models.append(deployment)

    click.echo('Loading instruments metadata')
    with open(instruments) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            instrument = Instrument(row)
            instrument_models.append(instrument)

    click.echo('Loading news items')
    with open(notifications) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            row['keywords_en'] = row['tags_en'].split(',')
            row['keywords_fr'] = row['tags_fr'].split(',')
            row['banner'] = row['banner'] == 't'
            row['visible'] = row['visible'] == 't'

            notification = Notification(row)
            notification_models.append(notification)

    click.echo('Loading discovery metadata items')
    for filename in os.listdir(discovery_metadata):
        if filename.endswith(".yml"):
            filepath = os.path.join(discovery_metadata, filename)
            yamldict = read_mcf(filepath)
            identifier = yamldict['metadata']['identifier']
            yamldict['metadata']['identifier'] = f"{identifier}_en"
            wmo_wcmp2_os = WMOWCMP2OutputSchema()
            jsondict = wmo_wcmp2_os.write(yamldict, stringify=False)
            jsondict['properties']['woudc:content_category'] = identifier
            end_date = jsondict['time']['interval'][1]
            if end_date in ("", ".."):
                jsondict['time']['interval'][1] = None
            discovery_metadata_ = DiscoveryMetadata(jsondict)
            discovery_metadata_models.append(discovery_metadata_)

            yamldict_fr = copy.deepcopy(yamldict)
            base_identifier = yamldict['metadata']['identifier']
            yamldict_fr['metadata']['identifier'] = (
                base_identifier.replace("en", "fr")
            )
            yamldict_fr['metadata']['language'] = 'fr'
            yamldict_fr['metadata']['language_alternate'] = 'en'
            jsondict_fr = wmo_wcmp2_os.write(yamldict_fr, stringify=False)
            end_date_fr = jsondict_fr['time']['interval'][1]
            if end_date_fr in ("", ".."):
                jsondict_fr['time']['interval'][1] = None
            discovery_metadata_fr = DiscoveryMetadata(jsondict_fr)
            discovery_metadata_models.append(discovery_metadata_fr)

    try:
        click.echo('Loading station dobson corrections items')
        with open(station_dobson_corrections) as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                station_dobson_corrections = StationDobsonCorrections(row)
                station_dobson_corrections_models.append(
                    station_dobson_corrections)
    except FileNotFoundError:
        click.echo(f"[WARNING] File not found: {station_dobson_corrections}. "
                   "Skipping loading of station dobson corrections.")

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
    click.echo('Storing deployment records in data registry')
    for model in deployment_models:
        registry_.save(model)
    click.echo('Storing instruments in data registry')
    for model in instrument_models:
        registry_.save(model)
    click.echo('Storing news items in data registry')
    for model in notification_models:
        registry_.save(model)
    click.echo('Storing discovery metadata items in data registry')
    for model in discovery_metadata_models:
        registry_.save(model)
    click.echo('Storing station dobson corrections items in data registry')
    for model in station_dobson_corrections_models:
        registry_.save(model)

    instrument_from_registry = registry_.query_full_index(Instrument)

    contribution_models = build_contributions(instrument_from_registry)

    click.echo('Storing contributions in data registry')
    for model in contribution_models:
        registry_.save(model)

    if init_search_index:
        search_index = SearchIndex()

        project_docs = [model.__geo_interface__ for model in project_models]
        dataset_docs = [model.__geo_interface__ for model in dataset_models]
        country_docs = [model.__geo_interface__ for model in country_models]
        station_docs = [model.__geo_interface__ for model in station_models]

        contributor_docs = \
            [model.__geo_interface__ for model in contributor_models]
        deployment_docs = \
            [model.__geo_interface__ for model in deployment_models]
        instrument_docs = \
            [model.__geo_interface__ for model in instrument_models]
        contribution_docs = \
            [model.__geo_interface__ for model in contribution_models]
        notification_docs = \
            [model.__geo_interface__ for model in notification_models]
        discovery_metadata_docs = \
            [model.__geo_interface__ for model in discovery_metadata_models]

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
        click.echo('Storing deployments in search index')
        search_index.index(Deployment, deployment_docs)
        click.echo('Storing instruments in search index')
        search_index.index(Instrument, instrument_docs)
        click.echo('Storing contributions in search index')
        search_index.index(Contribution, contribution_docs)
        click.echo('Storing news items in search index')
        search_index.index(Notification, notification_docs)
        click.echo('Storing discovery metadata items in search index')
        search_index.index(DiscoveryMetadata, discovery_metadata_docs)
        click.echo('Storing station dobson corrections items in search index')
        search_index.index(
            StationDobsonCorrections, station_dobson_corrections)


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
        DataRecord,
        Contribution,
        Notification,
        PeerDataRecord,
        DiscoveryMetadata,
        StationDobsonCorrections
    ]

    registry_ = registry.Registry()
    search_index = SearchIndex()

    search_index_config = config.EXTRAS.get('search_index', {})

    click.echo('Begin data registry backend sync on ', nl=False)
    for clazz in model_classes:
        plural_name = clazz.__tablename__
        plural_caps = ''.join(map(str.capitalize, plural_name.split('_')))
        registry_contents = []

        enabled_flag = f'{plural_name}_enabled'
        if not search_index_config.get(enabled_flag, True):
            click.echo(f'{plural_caps} index frozen (skipping)')
            continue

        click.echo(f'{plural_caps}...')
        if plural_caps == 'DataRecords':
            capacity = 10000
            for obj in registry_.session.query(clazz).yield_per(1):
                LOGGER.debug(f'Querying chunk of {clazz}')

                registry_contents.append(obj)
                if len(registry_contents) > capacity:
                    registry_docs = [item.__geo_interface__
                                     for item in registry_contents]
                    click.echo('Sending models to search index...')
                    search_index.index(clazz, registry_docs)
                    registry_contents.clear()

            registry_docs = [obj.__geo_interface__
                             for obj in registry_contents]
            click.echo('Sending models to search index...')
            search_index.index(clazz, registry_docs)
        else:
            registry_contents = registry_.query_full_index(clazz)
            registry_docs = \
                [obj.__geo_interface__ for obj in registry_contents]

            click.echo('Sending models to search index...')
            search_index.index(clazz, registry_docs)
        # click.echo('Purging excess models...')
        # search_index.unindex_except(product, registry_docs)

    click.echo('Done')


@click.command()
@click.pass_context
def product_sync(ctx):
    """Sync products to Elasticsearch"""

    products = [
        OzoneSonde,
        TotalOzone,
        UVIndex
    ]

    registry_ = registry.Registry()
    search_index = SearchIndex()

    search_index_config = config.EXTRAS.get('search_index', {})

    for product in products:
        plural_name = product.__tablename__
        plural_caps = ''.join(map(str.capitalize, plural_name.split('_')))

        enabled_flag = f'{plural_name}_enabled'
        if not search_index_config.get(enabled_flag, True):
            click.echo(f'{plural_caps} index frozen (skipping)')
            continue  # Skip to the next product in the loop

        click.echo(f'{plural_caps}...')

        registry_contents = []
        # Sync product to elasticsearch
        for obj in registry_.session.query(product).yield_per(1):
            LOGGER.debug(f'Querying chunk of {product}')

            registry_contents.append(obj)

            if plural_caps == 'Ozonesonde':
                capacity = 45
            else:
                capacity = 10000

            if len(registry_contents) > capacity:
                registry_docs = [item.__geo_interface__
                                 for item in registry_contents]
                click.echo('Sending models to search index...')
                search_index.index(product, registry_docs)
                registry_contents.clear()

        registry_docs = [obj.__geo_interface__ for obj in registry_contents]
        click.echo('Sending models to search index...')
        search_index.index(product, registry_docs)

    click.echo('Done')


admin.add_command(init)
admin.add_command(show_config)
admin.add_command(registry__)
admin.add_command(search)
admin.add_command(setup_dobson_correction)

registry__.add_command(setup)
registry__.add_command(teardown)

search.add_command(sync)
search.add_command(product_sync)
