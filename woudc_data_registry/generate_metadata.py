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

from woudc_data_registry.registry import Registry
import woudc_data_registry.models


LOGGER = logging.getLogger(__name__)

WOUDC_OWS = 'https://geo.woudc.org/ows'
WOUDC_ARCHIVE = 'https://beta.woudc.org/archive/'
WOUDC_DATA = 'https://beta.woudc.org/en/data/data-search-and-download'


def generate_metadata(woudc_yaml):
    """generate list of MCF ConfigParser objects from WOUDC YAML"""

    u1 = u2 = None
    dict_list = []
    geojson_list = []
    uri_pre = 'https://geo.woudc.org/def/data'

    topiccategory = 'climatologyMeteorologyAtmosphere'

    data_categories = woudc_yaml['data']['data_categories']

    # Go through each dataset collection
    for data_category in data_categories:
        for key, value in data_category.items():
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
                                          "geometry": None
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
                            dataset_md["properties"]['doi'] = value2['doi']
                            dataset_md['properties']['keywords_en'] = \
                                value2['keywords_en']
                            dataset_md['properties']['keywords_fr'] = \
                                value2['keywords_fr']
                            dataset_md['properties']['temporal_begin'] = \
                                time_begin
                            dataset_md['properties']['temporal_end'] = \
                                time_end
                            dataset_md['properties']['spatial_extent'] = \
                                [-180, -90, 180, 90]
                            dataset_md['properties']['title_en'] = \
                                value2['label_en']
                            dataset_md['properties']['title_fr'] = \
                                value2['label_fr']
                            dataset_md['properties']['topic_category'] = \
                                topiccategory
                            dataset_md['properties']['uri'] = uri

                            names = []
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
                                                if 'instruments' in value4:
                                                    names = []
                                                    if isinstance(
                                                        value4['instruments'],
                                                        list
                                                    ):
                                                        for i in value4[
                                                         'instruments'
                                                        ]:
                                                            for key5, value5 \
                                                             in i.items():
                                                                if key5 in [
                                                                  'label_en',
                                                                  'label_fr'
                                                                 ]:
                                                                    pass
                                                                else:
                                                                    names\
                                                                      .append(
                                                                        key5
                                                                      )
                                                    curr_network[
                                                      'instruments'
                                                    ] = names
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
                                            names = []
                                            if isinstance(
                                              value4['instruments'],
                                              list
                                            ):
                                                for instrument in value4[
                                                  'instruments'
                                                ]:
                                                    for key5, value5 in \
                                                      instrument.items():
                                                        if key5 in [
                                                          'label_en',
                                                          'label_fr'
                                                        ]:
                                                            pass
                                                        else:
                                                            names.append(key5)
                                        curr_network['instruments'] = names
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
    # Update metadata for each row in DiscoveryMetadata table
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
    datasets_class_list = registry.query_full_index(DiscoveryMetadata)
    datasets = []
    for curr_dataset in datasets_class_list:
        datasets += [(curr_dataset.discovery_metadata_id,
                      curr_dataset._metadata)]
    LOGGER.debug('Updating Discovery Metadata table...')
    tables = ['ozonesonde', 'totalozone', 'uv_index_hourly', 'data_records']

    for input_table in tables:
        if input_table == 'data_records':
            # Select data records content categories
            categories = registry.query_distinct(DataRecord.content_category)
            # Loop through each dataset and update the geoJSON
            for (dataset, md) in datasets:
                md_loads = json.loads(md.replace('\\"', '"'))
                if dataset in categories:
                    # Update spatial/temporal extents
                    extents = registry.query_extents(
                                      DataRecord,
                                      DataRecord.timestamp_date,
                                      'content_category',
                                      dataset)[0]

                    md_loads['properties']['spatial_extent'] = [extents[2],
                                                                extents[3],
                                                                extents[4],
                                                                extents[5]]
                    md_loads['properties']['temporal_begin'] = str(extents[0])
                    md_loads['properties']['temporal_end'] = str(extents[1])
                    # Update levels and networks
                    values = {'content_category': dataset}
                    levels = registry.query_distinct_by_fields(
                                DataRecord.content_level,
                                DataRecord,
                                values
                    )
                    if 'levels' not in md_loads['properties']:
                        # Add levels field if it does not already exist
                        md_loads['properties']['levels'] = []
                    for curr_level in levels:
                        is_included = False
                        for level in md_loads['properties']['levels']:
                            if level['label_en'] == f'Level {curr_level}':
                                is_included = True
                        if not is_included:
                            if dataset.startswith(('TotalOzone', 'UmkehrN14')):
                                label_en = 'Other'
                            else:
                                label_en = dataset
                            # Add level item if it does not already exist
                            md_loads['properties']['levels'].append(
                                {'label_en': f'Level {curr_level}',
                                 'networks': [{
                                     'label_en': label_en,
                                     'instruments': []
                                 }]
                                 })
                        # Get distinct instruments for current level
                        values = {
                            'content_category': dataset,
                            'content_level': curr_level
                        }
                        subquery = registry.query_distinct_by_fields(
                                       DataRecord.instrument_id,
                                       DataRecord,
                                       values
                        )
                        instruments = registry.query_distinct_in(
                                       Instrument.name,
                                       Instrument.instrument_id,
                                       subquery
                        )
                        for level in md_loads['properties']['levels']:
                            if level['label_en'] == f'Level {curr_level}':
                                for ins in instruments:
                                    is_included = False
                                    otherIndex = [False, -1]
                                    for n in level['networks']:
                                        if n['label_en'] == 'Other':
                                            otherIndex = [True,
                                                          level[
                                                            'networks'
                                                          ].index(n)]
                                        if ins.lower() in n['instruments']:
                                            is_included = True
                                    if not is_included:
                                        if dataset.startswith(
                                                   ('TotalOzone', 'UmkehrN14')
                                        ):
                                            if ins.lower() in [
                                               'brewer', 'dobson', 'saoz']:
                                                level['networks'].append({
                                                    'label_en': ins,
                                                    'instruments': [
                                                      ins.lower()
                                                    ]
                                                })

                                            elif otherIndex[0]:
                                                level['networks'][
                                                    otherIndex[1]][
                                                    'instruments'
                                                ].append(ins.lower())
                                            else:
                                                level['networks'].append({
                                                    'label_en': 'Other',
                                                    'instruments': [
                                                        ins.lower()
                                                    ]
                                                })
                                        else:
                                            level['networks'][0][
                                              'instruments'
                                            ].append(ins.lower())
                                for network in level['networks']:
                                    # Remove any empty networks
                                    if len(network['instruments']) == 0:
                                        level['networks'].remove(network)

                    md_updated = json.dumps(md_loads)
                    md_updated = md_updated.replace('"', '\\"')
                    # Update metadata in corresponding row
                    new_value = {'_metadata': md_updated}
                    registry.update_by_field(
                      new_value, DiscoveryMetadata,
                      'discovery_metadata_id', dataset
                    )

        else:
            for (dataset, md) in datasets:
                dataset_original = '_'.join(dataset.split('_')[:-1])
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

            # Update spatial/temporal extents
            extents = registry.query_extents(
               inputs[input_table]['model'], inputs[input_table]['date_field']
            )[0]
            md_loads['properties']['spatial_extent'] = [extents[2],
                                                        extents[3],
                                                        extents[4],
                                                        extents[5]]
            md_loads['properties']['temporal_begin'] = str(extents[0])
            md_loads['properties']['temporal_end'] = str(extents[1])
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
                              'label_en': inputs[input_table]['label_en'],
                              'instruments': []
                          }]
                        }
                    )

                # Get distinct instruments for current level
                subquery = registry.query_distinct(
                               inputs[input_table]['ins_id']
                )
                instruments = registry.query_distinct_in(
                               Instrument.name,
                               Instrument.instrument_id,
                               subquery
                )
                for level in md_loads['properties']['levels']:
                    if level['label_en'] == f'Level {curr_level}':
                        for ins in instruments:
                            is_included = False
                            for n in level['networks']:
                                if ins.lower() in n['instruments']:
                                    is_included = True
                            if not is_included:
                                level['networks'][0]['instruments'].append(
                                    ins.lower()
                                )
                        for network in level['networks']:
                            # Remove any empty networks
                            if len(network['instruments']) == 0:
                                level['networks'].remove(network)

            md_updated = json.dumps(md_loads)
            # Update metadata in corresponding row
            new_value = {'_metadata': md_updated}
            registry.update_by_field(
                new_value, DiscoveryMetadata, 'discovery_metadata_id', dataset
            )

    registry.close_session()
    return True
