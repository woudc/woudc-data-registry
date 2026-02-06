# -*- coding: utf-8 -*-
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

import json
import logging
from datetime import datetime

from woudc_data_registry.registry import Registry
from sqlalchemy import func
import woudc_data_registry.models


LOGGER = logging.getLogger(__name__)


def generate_metadata(woudc_yaml):
    """
    DEPRECATED
    generate list of MCF ConfigParser objects from WOUDC YAML
    """

    WOUDC_OWS = 'https://geo.woudc.org/ows'
    WOUDC_ARCHIVE = 'https://woudc.org/archive/'
    WOUDC_DATA = 'https://woudc.org/en/data/data-search-and-download'

    u1 = u2 = None
    dict_list = []
    geojson_list = []
    uri_pre = 'https://geo.woudc.org/def/data'

    topiccategory = 'climatologyMeteorologyAtmosphere'

    data_categories = woudc_yaml['data']['data_categories']

    # Go through each dataset collection
    for data_category in data_categories:
        for key in data_category.items():
            for datasetcollection in data_category[key]['datasetcollections']:
                for key1, value1 in datasetcollection.items():
                    # Find umkehr and duplicate for levels 1.0 and 2.0
                    for dataset in value1['datasets']:
                        if 'UmkehrN14' in dataset:
                            u1 = {'UmkehrN14_1.0':
                                  dataset['UmkehrN14']['levels'][0][1.0]
                                  }
                            u2 = {'UmkehrN14_2.0':
                                  dataset['UmkehrN14']['levels'][1][2.0]
                                  }
                            u1['UmkehrN14_1.0']['label_en'] = \
                                '%s (%s)' % \
                                (dataset['UmkehrN14']['label_en'],
                                 dataset['UmkehrN14']['levels'][0]
                                 [1.0]['label_en']
                                 )
                            u2['UmkehrN14_2.0']['label_fr'] = \
                                '%s (%s)' % \
                                (dataset['UmkehrN14']['label_en'],
                                 dataset['UmkehrN14']['levels'][1]
                                 [2.0]['label_fr']
                                 )
                            value1['datasets'].remove(dataset)
                            value1['datasets'].append(u1)
                            value1['datasets'].append(u2)

                    for dataset in value1['datasets']:
                        for key2, value2 in dataset.items():
                            # Metadata dictionary for current dataset
                            dataset_md = {"type": "Feature",
                                          "properties": {},
                                          "geometry": {}
                                          }

                            if key2 == 'UmkehrN14_1.0':
                                search_id = 'umkehrn14/1.0'
                            elif key2 == 'UmkehrN14_2.0':
                                search_id = 'umkehrn14/2.0'
                            else:
                                search_id = key2.lower().replace('-', '')
                                snapshot_id = search_id
                            uri = f'{uri_pre}/{key}/{key1}/{search_id}'
                            time_begin, time_end = \
                                value1['extent']['time'].split('/')
                            dataset_md["id"] = key2
                            dataset_md['properties']['abstract_en'] = \
                                value2['description_en']
                            dataset_md['properties']['abstract_fr'] = \
                                value2['description_fr']
                            dataset_md["properties"]['externalIds'] = [{
                                'value': value2['doi'],
                                'scheme': 'https://doi.org'
                            }]
                            dataset_md['properties']['keywords_en'] = \
                                value2['keywords_en']
                            dataset_md['properties']['keywords_fr'] = \
                                value2['keywords_fr']
                            dataset_md['time']['interval'] = [time_begin,
                                                              time_end]
                            dataset_md['geometry']['spatial_extent'] = \
                                [-180, -90, 180, 90]
                            dataset_md['properties']['title_en'] = \
                                value2['label_en']
                            dataset_md['properties']['title_fr'] = \
                                value2['label_fr']
                            dataset_md['properties']['topic_category'] = \
                                topiccategory
                            dataset_md['properties']['uri'] = uri

                            # names = []
                            if 'levels' in value2:
                                levels = []
                                for level in value2['levels']:
                                    for key3, value3 in level.items():
                                        curr_level = {
                                            'label_en':
                                                level[key3]['label_en'],
                                            'networks': []
                                        }
                                        networks = []
                                        for ntwk in value3['networks']:
                                            curr_network = {}
                                            for key4, value4 in ntwk.items():
                                                curr_network['label_en'] = \
                                                    value4['label_en']
                                            networks.append(curr_network)
                                            curr_level['networks'] = networks
                                    levels.append(curr_level)
                                dataset_md['properties']['levels'] = levels
                            else:  # umkehr
                                levels = []
                                if key2 == 'UmkehrN14_1.0':
                                    label_en = 'Level 1.0'
                                    search_id = 'umkehrn14-1'
                                    snapshot_id = 'umkehr1'
                                else:
                                    label_en = 'Level 2.0'
                                    search_id = 'umkehrn14-2'
                                    snapshot_id = 'umkehr2'
                                curr_level = {
                                            'label_en': label_en,
                                            'networks': []
                                        }
                                networks = []
                                for n in value2['networks']:
                                    curr_network = {}
                                    for key4, value4 in n.items():
                                        if 'instruments' in value4:
                                            curr_network['label_en'] = \
                                                value4['label_en']
                                        networks.append(curr_network)
                                curr_level['networks'] = networks
                                levels.append(curr_level)
                                dataset_md['properties']['levels'] = levels

                            if value2['waf_dir'] != 'none':
                                dataset_md['properties']['waf'] = {
                                    'url': f"{WOUDC_ARCHIVE}/Archive-NewFormat/{value2['waf_dir']}",  # noqa
                                    'linktype': 'WWW:LINK',
                                    'function': 'download',
                                    'label_en': value2['waf_dir'],
                                    'label_fr': value2['waf_dir'],
                                    'description_en':
                                        'Web Accessible Folder (WAF)',
                                    'description_fr':
                                        'Dossier accessible sur le web '
                                        '(WAF)'
                                }

                            dataset_md['properties']['dataset_snapshots'] = {
                                'url': f'{WOUDC_ARCHIVE}/Summaries/dataset-snapshots/{snapshot_id}.zip',  # noqa
                                'linktype': 'WWW:LINK',
                                'function': 'download',
                                'label_en': value2['label_en'],
                                'label_fr': value2['label_fr'],
                                'description_en':
                                    'Static dataset archive file',
                                'description_fr':
                                    "La donnée d'archive statique"
                            }

                            dataset_md['properties']['wms'] = {
                                'url': f'{WOUDC_OWS}?service=WMS&version=1.3.0&request=GetCapabilities',  # noqa
                                'linktype': 'OGC:WMS',
                                'function': 'download',
                                'label_en': key2,
                                'label_fr': key2,
                                'description_en': 'OGC Web Map Service (WMS)',
                                'description_fr': 'OGC Web Map Service (WMS)'
                            }

                            dataset_md['properties']['wfs'] = {
                                'url': f'{WOUDC_OWS}?service=WFS&version=1.1.0&request=GetCapabilities',  # noqa
                                'linktype': 'OGC:WFS',
                                'function': 'download',
                                'label_en': key2,
                                'label_fr': key2,
                                'description_en':
                                    'OGC Web Feature Service (WFS)',
                                'description_fr':
                                    'OGC Web Feature Service (WFS)'
                             }

                            dataset_md['properties']['search'] = {
                                'url': f'{WOUDC_DATA}?dataset={key2}',
                                'linktype': 'WWW:LINK',
                                'function': 'search',
                                'label_en': value2['label_en'],
                                'label_fr': value2['label_fr'],
                                'description_en':
                                    'Data Search / Download User Interface',
                                'description_fr':
                                    u'Interface de recherche et '
                                    'téléchargement de données'
                            }

                            geojson_list.append((key2, dataset_md))

    for dataset in geojson_list:
        discovery_metadata_id = dataset[0]
        metadata = json.dumps(dataset[1])
        metadata = metadata.replace('"', '\\"')
        curr_dict = {
            "discovery_metadata_id": discovery_metadata_id,
            "metadata": metadata
        }
        dict_list += [curr_dict]

    return dict_list


def update_extents():
    """
    Update metadata for each row in DiscoveryMetadata table

    :returns: void
    """
    [
      DataRecord, UVIndex, OzoneSonde, TotalOzone,
      DiscoveryMetadata, Instrument
    ] = [
      woudc_data_registry.models.DataRecord,
      woudc_data_registry.models.UVIndex,
      woudc_data_registry.models.OzoneSonde,
      woudc_data_registry.models.TotalOzone,
      woudc_data_registry.models.DiscoveryMetadata,
      woudc_data_registry.models.Instrument
    ]

    inputs = {
        'ozonesonde': {
            'content_category': 'OzoneSonde',
            'ins_id': OzoneSonde.instrument_id,
            'date_field': OzoneSonde.timestamp_date,
            'label_en': 'OzoneSonde',
            'levels': ['1.0'],
            'model': OzoneSonde
        },
        'totalozone': {
            'content_category': 'TotalOzone',
            'ins_id': TotalOzone.instrument_id,
            'date_field': TotalOzone.observation_date,
            'label_en': 'TotalOzone',
            'levels': ['1.0', '2.0'],
            'model': TotalOzone
        },
        'uv_index_hourly': {
            'content_category': 'uv_index_hourly',
            'ins_id': UVIndex.instrument_id,
            'date_field': UVIndex.observation_date,
            'label_en': 'UV-Index-Hourly',
            'levels': ['2.0'],
            'model': UVIndex
        }
    }
    # Connect to registry and update dataset metadata
    registry = Registry()
    # Select all rows from discovery_metadata
    instrument_link = "https://api.woudc.org/collections/instruments"
    discovery_metadata_list = registry.query_full_index(DiscoveryMetadata)
    curr_discovery_metadata = []
    for curr_dataset in discovery_metadata_list:
        curr_discovery_metadata += [(curr_dataset.discovery_metadata_id,
                                    curr_dataset._metadata)]
    LOGGER.debug('Updating Discovery Metadata table...')
    tables = ['ozonesonde', 'totalozone', 'uv_index_hourly', 'data_records']

    for input_table in tables:
        if input_table == 'data_records':  # update all other datasets
            # Select data records content categories
            categories = registry.query_distinct(DataRecord.content_category)
            LOGGER.debug(
                f"Categories to update discovery metadata: {categories}"
            )
            # Loop through each dataset and update the GeoJSON metadata
            for (discovery_metadata_id, md) in curr_discovery_metadata:
                LOGGER.info(
                    f"Updating discovery metadata for {discovery_metadata_id}"
                )
                md_loads = json.loads(md.replace('\\"', '"'))
                dataset_short = discovery_metadata_id.split('_')[0]
                LOGGER.debug(f"dataset_short: {dataset_short}")
                if dataset_short in categories:
                    query_values = {'content_category': dataset_short}
                    # Treat UmkehrN14 datasets as separate by level
                    if dataset_short == 'UmkehrN14':
                        content_level = discovery_metadata_id.split('_')[1]
                        query_values['content_level'] = content_level
                    extents = registry.query_extents(
                        DataRecord,
                        DataRecord.timestamp_date,
                        query_values
                    )[0]
                    md_loads['geometry'] = {
                        'type': 'Polygon',
                        'coordinates': [[
                            [extents[2], extents[3]],  # [minLon, minLat]
                            [extents[4], extents[3]],  # [maxLon, minLat]
                            [extents[4], extents[5]],  # [maxLon, maxLat]
                            [extents[2], extents[5]],  # [minLon, maxLat]
                            [extents[2], extents[3]]   # Close the bbox polygon
                        ]]
                    }
                    md_loads['time']['interval'] = [str(extents[0]),
                                                    str(extents[1])]
                    # Update 'updated' datetime
                    md_loads['updated'] = datetime.utcnow().strftime(
                        '%Y-%m-%dT%H:%M:%SZ'
                    )
                    # Update levels and networks
                    levels = registry.query_distinct_by_fields(
                                DataRecord.content_level,
                                DataRecord,
                                query_values
                    )
                    LOGGER.debug(f"levels: {levels}")
                    md_loads['properties']['levels'] = []  # reset levels
                    if 'levels' not in md_loads['properties']:
                        # Add levels field if it does not already exist
                        md_loads['properties']['levels'] = []
                    for curr_level in levels:
                        is_included = False
                        for level in md_loads['properties']['levels']:
                            if level['label_en'] == f'Level {curr_level}':
                                is_included = True
                        if not is_included:
                            if dataset_short.startswith(('TotalOzone',
                                                         'UmkehrN14')):
                                label_en = 'Other'
                            else:
                                label_en = discovery_metadata_id
                            # Add level item if it does not already exist
                            md_loads['properties']['levels'].append({
                                'label_en': f'Level {curr_level}',
                                'networks': [{
                                     'label_en': label_en
                                 }]
                            })
                        # Get distinct instruments for current level
                        subquery = registry.query_distinct_by_fields(
                                       DataRecord.instrument_id,
                                       DataRecord,
                                       query_values
                        )
                        instruments = registry.query_distinct_in(
                                       Instrument.name,
                                       Instrument.instrument_id,
                                       subquery
                        )
                        for level in md_loads['properties']['levels']:
                            if level['label_en'] == f'Level {curr_level}':
                                for ins in instruments:
                                    # check if instrument concepts
                                    #  is in metadata:
                                    if len(md_loads["properties"]["themes"
                                                                  ]) < 2:
                                        ins_concept = {"concepts": []}
                                        # link to API instruments collection
                                        ins_concept["scheme"] = instrument_link
                                        ins_concept["concepts"].append({
                                            "id": ins.lower()})
                                        md_loads["properties"][
                                            "themes"].append(ins_concept)
                                    else:
                                        if {"id": ins.lower()} not in md_loads[
                                            "properties"]["themes"][1][
                                                "concepts"]:
                                            ins_concept = md_loads[
                                                "properties"]["themes"
                                                              ][1]["concepts"]
                                            ins_concept.append({
                                                "id": ins.lower()})
                                for n in level['networks']:
                                    if 'instruments' in n.keys():
                                        n.pop('instruments', n['instruments'])
                    md_updated = json.dumps(md_loads)
                    # Update metadata in corresponding row
                    new_value = {'_metadata': md_updated}
                    registry.update_by_field(
                      new_value, DiscoveryMetadata,
                      'discovery_metadata_id', discovery_metadata_id
                    )
                else:
                    LOGGER.warn(f"{dataset_short} does not belong in the "
                                "dataset categories.\n"
                                "No metadata update applied for this dataset.")

        else:  # data products: 'ozonesonde', 'totalozone', 'uv_index_hourly'
            for (discovery_metadata_id, md) in curr_discovery_metadata:
                dataset_original = '_'.join(
                    discovery_metadata_id.split('_')[:-1]
                )
                if dataset_original == inputs[input_table]['content_category']:
                    break
            else:
                msg = (
                    "DiscoveryMetadata table does not contain this dataset: "
                    f"{inputs[input_table]['content_category']}. "
                    "Did you forget to initialize the database with data?"
                )
                LOGGER.error(msg)
                raise ValueError(msg)
            md_loads = json.loads(md.replace('\\"', '"'))
            LOGGER.info(
                f"Updating dataset_original: {dataset_original}\n"
                f"inputs[input_table]['model']: {inputs[input_table]['model']}"
            )

            # Update spatial/temporal extents
            extents = registry.query_extents(
               inputs[input_table]['model'], inputs[input_table]['date_field']
            )[0]
            md_loads['geometry'] = {
                'type': 'Polygon',
                'coordinates': [[
                    [extents[2], extents[3]],  # [minLon, minLat]
                    [extents[4], extents[3]],  # [maxLon, minLat]
                    [extents[4], extents[5]],  # [maxLon, maxLat]
                    [extents[2], extents[5]],  # [minLon, maxLat]
                    [extents[2], extents[3]]   # Close the bbox polygon
                ]]
            }
            md_loads['time']['interval'] = [str(extents[0]), str(extents[1])]
            # Update 'updated' datetime
            md_loads['updated'] = datetime.utcnow().strftime(
                '%Y-%m-%dT%H:%M:%SZ'
            )
            # Update levels and networks
            levels = inputs[input_table]['levels']
            if 'levels' not in md_loads['properties']:
                # Add levels field if it does not already exist
                md_loads['properties']['levels'] = []

            for curr_level in levels:
                is_included = False
                for level in md_loads['properties']['levels']:
                    if level['label_en'] == f'Level {curr_level}':
                        is_included = True
                if not is_included:
                    # Add level item if it does not already exist
                    md_loads['properties']['levels'].append(
                        {
                          'label_en': f'Level {curr_level}',
                          'networks': [{
                              'label_en': inputs[input_table]['label_en']
                          }]
                        }
                    )

            md_updated = json.dumps(md_loads)
            md_updated.replace('\\"', '"')
            # Update metadata in corresponding row
            new_value = {'_metadata': md_updated}
            registry.update_by_field(
                new_value, DiscoveryMetadata,
                'discovery_metadata_id', discovery_metadata_id
            )

    registry.close_session()
    return True


def update_date_submission_ranges(tables=None):
    """
    Update date ranges for each station, instrument, contributor, and
    deployment of the data submission

    :returns: void
    """
    registry = Registry()

    [
        Contributor, Deployment, Station, DataRecord, Instrument
    ] = [
        woudc_data_registry.models.Contributor,
        woudc_data_registry.models.Deployment,
        woudc_data_registry.models.Station,
        woudc_data_registry.models.DataRecord,
        woudc_data_registry.models.Instrument
    ]
    if (tables is None) or (tables is not None and 'Instrument' in tables):
        LOGGER.info('Updating Instrument date ranges')
        instruments_daterange = (
            registry.session.query(
                DataRecord.instrument_id,
                func.max(DataRecord.timestamp_date).label("max_timestamp"),
                func.min(DataRecord.timestamp_date).label("min_timestamp")
            ).group_by(DataRecord.instrument_id).all())

        # update instrument date ranges
        for inst in instruments_daterange:
            registry.update_by_field({'end_date': inst[1],
                                      'start_date': inst[2]
                                      }, Instrument, 'instrument_id', inst[0])

    if (tables is None) or (tables is not None and 'Contributor' in tables):
        LOGGER.info('Updating Contributor date ranges')
        contributors_daterange = (
            registry.session.query(
                DataRecord.data_generation_agency,
                func.max(DataRecord.timestamp_date).label("max_timestamp"),
                func.min(DataRecord.timestamp_date).label("min_timestamp")
            ).group_by(DataRecord.data_generation_agency).all())

        # update contributor date ranges
        for contr in contributors_daterange:
            registry.update_by_field({'end_date': contr[1],
                                      'start_date': contr[2]}, Contributor,
                                     'acronym', contr[0])

    if (tables is None) or (tables is not None and 'Station' in tables):
        LOGGER.info('Updating Stations date ranges')
        stations_daterange = (
            registry.session.query(
                DataRecord.station_id,
                func.max(DataRecord.timestamp_date).label("max_timestamp"),
                func.min(DataRecord.timestamp_date).label("min_timestamp")
            ).group_by(DataRecord.station_id).all())

        # update station date ranges
        for stn in stations_daterange:
            registry.update_by_field({'end_date': stn[1], 'start_date': stn[2]
                                      }, Station, 'station_id', stn[0])

    if (tables is None) or (tables is not None and 'Deployment' in tables):
        LOGGER.info('Updating Deployment date ranges')
        deployments_daterange = (
            registry.session.query(
                DataRecord.data_generation_agency,
                DataRecord.station_id,
                func.max(DataRecord.timestamp_date).label("max_timestamp"),
                func.min(DataRecord.timestamp_date).label("min_timestamp"),
                DataRecord.content_class
            ).group_by(DataRecord.data_generation_agency,
                       DataRecord.content_class,
                       DataRecord.station_id).all()
        )

        # get the contributor_id associated with the data_generation_agency
        # {deployment_id: {end_date: str , start_date: str}}
        deployments_updates = {}
        for deploy in deployments_daterange:
            agency = deploy[0]
            station_id = deploy[1]
            content_class = deploy[4]
            contributor_id = f"{agency}:{content_class}"
            # subquery for the deployment_id
            deployment_id = registry.session.query(
                Deployment.deployment_id
            ).filter(Deployment.contributor_id == contributor_id,
                     Deployment.station_id == station_id).scalar()
            deployments_updates[deployment_id] = {
                'end_date': deploy[2],
                'start_date': deploy[3]}
        # update deployment date ranges
        for deployment_id in deployments_updates:
            updated_dates = deployments_updates[deployment_id]
            registry.update_by_field(updated_dates, Deployment,
                                     'deployment_id', deployment_id)
