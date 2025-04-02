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

# Compute and persist UV Index from WOUDC archive

from datetime import datetime
from woudc_extcsv import ExtendedCSV
import logging
import os

from woudc_data_registry.models import TotalOzone, Instrument
from woudc_data_registry import registry
from woudc_data_registry.util import read_file
from woudc_data_registry.epicentre.metadata import add_metadata
from woudc_data_registry.processing import correct_instrument_value

LOGGER = logging.getLogger(__name__)


def execute(path, bypass):
    """
    Orchestrate TotalOzone table generation process
    """

    datasets = [
        '/'.join([path, 'TotalOzone_1.0_1']),
        '/'.join([path, 'TotalOzone_2.0_1']),
    ]

    registry_ = registry.Registry()

    LOGGER.info('erasing current totalozone table')
    registry_.session.query(TotalOzone).delete()
    registry_.save()

    # traverse directory of files
    previously_seen_instrument = []
    for dataset_path in datasets:
        for dirname, dirnames, filenames in os.walk(dataset_path):
            successful_files = []
            unsuccessful_files = []
            LOGGER.info(f'Parsing through directory: {dirname}')
            for filename in filenames:
                ipath = os.path.join(dirname, filename)
                contents = read_file(ipath)
                LOGGER.debug(f'Parsing extcsv {ipath}')

                try:
                    extcsv = ExtendedCSV(contents)
                except Exception as err:
                    msg = f'Unable to parse extcsv {ipath}: {err}'
                    LOGGER.error(msg)
                    unsuccessful_files.append(ipath)
                    continue

                # get metadata fields
                try:
                    agency = extcsv.extcsv['DATA_GENERATION']['Agency'][0]
                    dataset_name = extcsv.extcsv['CONTENT']['Category'][0]
                    dataset_level = extcsv.extcsv['CONTENT']['Level'][0]
                    dataset_form = extcsv.extcsv['CONTENT']['Form'][0]
                    project_id = extcsv.extcsv['CONTENT']['Class'][0]
                    station_type = extcsv.extcsv['PLATFORM']['Type'][0]
                    station_id = extcsv.extcsv['PLATFORM']['ID'][0]
                    country = extcsv.extcsv['PLATFORM']['Country'][0]
                    instrument_name = correct_instrument_value(
                        extcsv.extcsv['INSTRUMENT']['Name'][0], 'name')
                    instrument_model = correct_instrument_value(
                        extcsv.extcsv['INSTRUMENT']['Model'][0], 'model')
                    instrument_number = correct_instrument_value(
                        extcsv.extcsv['INSTRUMENT']['Number'][0], 'serial')
                    instrument_latitude = \
                        extcsv.extcsv['LOCATION']['Latitude'][0]
                    instrument_longitude = \
                        extcsv.extcsv['LOCATION']['Longitude'][0]
                    instrument_height = extcsv.extcsv['LOCATION']['Height'][0]
                    timestamp_date = extcsv.extcsv['TIMESTAMP']['Date'][0]
                except Exception as err:
                    msg = f'Unable to get metadata from extcsv {ipath}: {err}'
                    LOGGER.error(msg)
                    unsuccessful_files.append(ipath)
                    continue

                if len(station_id) < 3:
                    station_id = station_id.zfill(3)

                # get data fields
                try:
                    date = extcsv.extcsv['DAILY']['Date']
                    wlcode = extcsv.extcsv['DAILY']['WLCode']
                    obscode = extcsv.extcsv['DAILY']['ObsCode']
                    columno3 = extcsv.extcsv['DAILY']['ColumnO3']
                except Exception:
                    msg = 'Unable to parse TotalOzone table row from file'
                    LOGGER.error(msg)
                    continue

                stddevo3 = extcsv.extcsv.get('DAILY', {}).get('StdDevO3')
                utc_begin = extcsv.extcsv.get('DAILY', {}).get('UTC_Begin')
                utc_end = extcsv.extcsv.get('DAILY', {}).get('UTC_End')
                utc_mean = extcsv.extcsv.get('DAILY', {}).get('UTC_Mean')
                nobs = extcsv.extcsv.get('DAILY', {}).get('nObs')
                mmu = extcsv.extcsv.get('DAILY', {}).get('mMu')
                columnso2 = extcsv.extcsv.get('DAILY', {}).get('ColumnSO2')

                monthly_date = extcsv.extcsv.get('MONTHLY', {}).get('Date')
                npts = extcsv.extcsv.get('MONTHLY', {}).get('Npts')
                monthly_co3 = extcsv.extcsv.get('MONTHLY', {}).get('ColumnO3')
                monthly_stdevo3 = extcsv.extcsv.get(
                    'MONTHLY', {}).get('StdDevO3')

                # form ids for data insert
                dataset_id = f"{dataset_name}_{str(dataset_level)}"
                contributor_id = ':'.join([agency, project_id])
                deployment_id = ':'.join([station_id, contributor_id])
                instrument_id = ':'.join([instrument_name,
                                          instrument_model,
                                          instrument_number, dataset_id,
                                          deployment_id])

                # check if instrument is in registry
                exists = registry_.query_by_field(Instrument,
                                                  'instrument_id',
                                                  instrument_id, True)
                if (not exists and
                        instrument_id not in previously_seen_instrument):
                    # instrument not found. add it to registry
                    if bypass:
                        LOGGER.info('Skipping instrument addition check')
                        allow_add_instrument = True
                    else:
                        response = \
                            input(f'Instrument {instrument_id} not found. '
                                  'Add? (y/n) [n]: ')
                        allow_add_instrument = \
                            response.lower() in ['y', 'yes']

                    if allow_add_instrument:
                        instrument_ = {
                            'station_id': station_id,
                            'dataset_id': dataset_id,
                            'dataset_name': dataset_name,
                            'dataset_level': dataset_level,
                            'contributor': agency,
                            'project': project_id,
                            'name': instrument_name,
                            'model': instrument_model,
                            'serial': instrument_number,
                            'start_date': datetime.now(),
                            'x': instrument_longitude,
                            'y': instrument_latitude,
                            'z': instrument_height,
                        }
                        try:
                            add_metadata(Instrument, instrument_,
                                         True, False)
                        except ValueError as e:
                            LOGGER.error(f'Error adding instrument: {e}.'
                                         ' Skipping Insertion')
                    previously_seen_instrument.append(instrument_id)

                first = True
                success = 0
                for i in range(len(date)):
                    if conv(columno3[i]):
                        if first:
                            observation_date = conv(date[i])
                            first = False

                        try:
                            ins_data = {
                                'file_path': ipath,
                                'filename': filename,
                                'dataset_id': dataset_id,
                                'dataset_level': dataset_level,
                                'dataset_form': dataset_form,
                                'station_id': station_id,
                                'station_type': station_type,
                                'country_id': country,
                                'instrument_id': exists.instrument_id
                                if exists else instrument_id,
                                'instrument_name': instrument_name,
                                'observation_date': observation_date,
                                'date': conv(date[i]),
                                'wlcode': conv(wlcode[i]),
                                'obscode': conv(obscode[i]),
                                'columno3': conv(columno3[i]),
                                'stddevo3': conv(
                                    stddevo3[i]) if stddevo3 else None,
                                'utc_begin': conv(
                                    utc_begin[i]) if utc_begin else None,
                                'utc_end': conv(
                                    utc_end[i]) if utc_end else None,
                                'utc_mean': conv(
                                    utc_mean[i]) if utc_mean else None,
                                'nobs': conv(nobs[i]) if nobs else None,
                                'mmu': conv(mmu[i]) if mmu else None,
                                'columnso2': conv(
                                    columnso2[i]) if columnso2 else None,
                                'monthly_date': conv(
                                    monthly_date[0]) if monthly_date else None,
                                'npts': conv(npts[0]) if npts else None,
                                'monthly_columno3': conv(
                                    monthly_co3[0]) if npts else None,
                                'monthly_stdevo3': conv(
                                        monthly_stdevo3[0]
                                    ) if monthly_stdevo3 else None,
                                'timestamp_date': timestamp_date,
                                'x': instrument_longitude,
                                'y': instrument_latitude,
                                'z': instrument_height,
                            }
                            ozone_object = TotalOzone(ins_data)
                            registry_.save(ozone_object)
                            LOGGER.info(f'Inserted {ozone_object}')
                            success += 1
                        except Exception as err:
                            msg = (f'Unable to insert UV index {ipath}:'
                                   f' {err}')
                            LOGGER.error(msg)
                            continue

                if success > 0:
                    successful_files.append(ipath)
                else:
                    unsuccessful_files.append(ipath)

                pass_ratio = f'{len(successful_files)} / {len(filenames)}'
                LOGGER.info(f'Successful Files: {pass_ratio}')
                fail_ratio = f'{len(unsuccessful_files)} / {len(filenames)}'
                LOGGER.info(f'Unsuccessful Files: {fail_ratio}')

    LOGGER.debug('Done get_data().')


def conv(i):
    if i:
        return i
    else:
        return None


def generate_totalozone(archivedir, bypass):
    if archivedir is None:
        raise RuntimeError('Missing required on disk archive')
    LOGGER.info('Computing TotalOzone table...')
    execute(archivedir, bypass)
