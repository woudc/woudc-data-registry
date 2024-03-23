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
    for dataset in datasets:
        for dirname, dirnames, filenames in os.walk(dataset):
            for filename in filenames:
                ipath = os.path.join(dirname, filename)
                contents = read_file(ipath)
                LOGGER.debug(f'Parsing extcsv {ipath}')

                try:
                    extcsv = ExtendedCSV(contents)
                except Exception as err:
                    msg = f'Unable to parse extcsv {ipath}: {err}'
                    LOGGER.error(msg)
                    continue

                # get metadata fields
                try:
                    agency = extcsv.extcsv['DATA_GENERATION']['Agency'][0]
                    dataset_id = extcsv.extcsv['CONTENT']['Category'][0]
                    level = extcsv.extcsv['CONTENT']['Level'][0]
                    form = extcsv.extcsv['CONTENT']['Form'][0]
                    project_id = extcsv.extcsv['CONTENT']['Class'][0]
                    station_type = extcsv.extcsv['PLATFORM']['Type'][0]
                    station_id = extcsv.extcsv['PLATFORM']['ID'][0]
                    country = extcsv.extcsv['PLATFORM']['Country'][0]
                    instrument_name = extcsv.extcsv['INSTRUMENT']['Name'][0]
                    instrument_model = extcsv.extcsv['INSTRUMENT']['Model'][0]
                    instrument_number = \
                        extcsv.extcsv['INSTRUMENT']['Number'][0]
                    instrument_latitude = \
                        extcsv.extcsv['LOCATION']['Latitude'][0]
                    instrument_longitude = \
                        extcsv.extcsv['LOCATION']['Longitude'][0]
                    instrument_height = extcsv.extcsv['LOCATION']['Height'][0]
                    timestamp_date = extcsv.extcsv['TIMESTAMP']['Date'][0]
                except Exception as err:
                    msg = f'Unable to get metadata from extcsv {ipath}: {err}'
                    LOGGER.error(msg)
                    continue

                if len(station_id) < 3:
                    station_id = station_id.zfill(3)

                # get data fields
                try:
                    date = extcsv.extcsv['DAILY']['Date']
                    wlcode = extcsv.extcsv['DAILY']['WLCode']
                    obscode = extcsv.extcsv['DAILY']['ObsCode']
                    columno3 = extcsv.extcsv['DAILY']['ColumnO3']
                    stddevo3 = extcsv.extcsv['DAILY']['StdDevO3']
                    utc_begin = extcsv.extcsv['DAILY']['UTC_Begin']
                    utc_end = extcsv.extcsv['DAILY']['UTC_End']
                    utc_mean = extcsv.extcsv['DAILY']['UTC_Mean']
                    nobs = extcsv.extcsv['DAILY']['nObs']
                    mmu = extcsv.extcsv['DAILY']['mMu']
                    columnso2 = extcsv.extcsv['DAILY']['ColumnSO2']
                    monthly_date = extcsv.extcsv['MONTHLY']['Date']
                    npts = extcsv.extcsv['MONTHLY']['Npts']
                    monthly_co3 = extcsv.extcsv['MONTHLY']['ColumnO3']
                    monthly_stdevo3 = extcsv.extcsv['MONTHLY']['StdDevO3']
                except Exception:
                    msg = 'Unable to parse TotalOzone table row from file'
                    LOGGER.error(msg)
                    continue

                # form ids for data insert
                contributor_id = ':'.join([agency, project_id])
                deployment_id = ':'.join([station_id, contributor_id])
                instrument_id = ':'.join([instrument_name,
                                          instrument_model,
                                          instrument_number, dataset_id,
                                          deployment_id])
                # check if instrument is in registry
                exists = registry_.query_by_field(Instrument,
                                                  'instrument_id',
                                                  instrument_id)
                if not exists:
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
                        add_metadata(Instrument, instrument_,
                                     True, False)

                first = True
                for i in range(len(date)):
                    if conv(columno3[i]):
                        if first:
                            observation_date = conv(date[i])
                            first = False

                        ins_data = {
                            'file_path': ipath,
                            'filename': filename,
                            'dataset_id': dataset_id,
                            'dataset_level': level,
                            'dataset_form': form,
                            'station_id': station_id,
                            'station_type': station_type,
                            'country_id': country,
                            'instrument_id': instrument_id,
                            'instrument_name': instrument_name,
                            'observation_date': observation_date,
                            'date': conv(date[i]),
                            'wlcode': conv(wlcode[i]),
                            'obscode': conv(obscode[i]),
                            'columno3': conv(columno3[i]),
                            'stddevo3': conv(stddevo3[i]),
                            'utc_begin': conv(utc_begin[i]),
                            'utc_end': conv(utc_end[i]),
                            'utc_mean': conv(utc_mean[i]),
                            'nobs': conv(nobs[i]),
                            'mmu': conv(mmu[i]),
                            'columnso2': conv(columnso2[i]),
                            'monthly_date': conv(monthly_date[0]),
                            'npts': conv(npts[0]),
                            'monthly_columno3': conv(monthly_co3[0]),
                            'monthly_stdevo3': conv(monthly_stdevo3[0]),
                            'timestamp_date': timestamp_date,
                            'x': instrument_longitude,
                            'y': instrument_latitude,
                            'z': instrument_height,
                        }
                    ozone_object = TotalOzone(ins_data)
                    registry_.save(ozone_object)

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
