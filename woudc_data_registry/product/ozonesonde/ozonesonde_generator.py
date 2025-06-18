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

from woudc_data_registry.models import OzoneSonde, Instrument
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
        '/'.join([path, 'OzoneSonde_1.0_1']),
        '/'.join([path, 'OzoneSonde_1.0_2']),
    ]

    registry_ = registry.Registry()

    LOGGER.info('erasing current ozonesonde table')
    registry_.session.query(OzoneSonde).delete()
    registry_.save()

    count = 0
    previously_seen_instrument = []
    # traverse directory of files
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
                    instrument_model = \
                        correct_instrument_value(
                            extcsv.extcsv['INSTRUMENT']['Model'][0], 'model')
                    instrument_number = \
                        correct_instrument_value(
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
                flight_summary = extcsv.extcsv.get('FLIGHT_SUMMARY', {})
                profile = extcsv.extcsv.get('PROFILE', {})

                integratedo3 = flight_summary.get('IntegratedO3', {})

                correctioncode = flight_summary.get(
                    'WLCode') or [None] * len(integratedo3)
                sondetotalo3 = flight_summary.get(
                    'SondeTotalO3') or [None] * len(integratedo3)
                correctionfactor = flight_summary.get(
                    'CorrectionFactor') or [None] * len(integratedo3)
                totalo3 = flight_summary.get(
                    'TotalO3') or [None] * len(integratedo3)
                wlcode = flight_summary.get(
                    'WLCode') or [None] * len(integratedo3)
                obstype = flight_summary.get(
                    'ObsType') or [None] * len(integratedo3)
                flight_instrument = flight_summary.get(
                    'Instrument') or [None] * len(integratedo3)
                flight_number = flight_summary.get(
                    'Number') or [None] * len(integratedo3)

                profile_pressure = profile.get('Pressure', {})

                o3partialpressure = profile.get(
                    'O3PartialPressure') or [None] * len(profile_pressure)
                temperature = profile.get(
                    'Temperature') or [None] * len(profile_pressure)
                windspeed = profile.get(
                    'WindSpeed') or [None] * len(profile_pressure)
                winddirection = profile.get(
                    'WindDirection') or [None] * len(profile_pressure)
                levelcode = profile.get(
                    'LevelCode') or [None] * len(profile_pressure)
                duration = profile.get(
                    'Duration') or [None] * len(profile_pressure)
                gpheight = profile.get(
                    'GPHeight') or [None] * len(profile_pressure)
                relativehumidity = profile.get(
                    'RelativeHumidity') or [None] * len(profile_pressure)
                sampletemperature = profile.get(
                    'SampleTemperature') or [None] * len(profile_pressure)

                # replace empty strings in data with None
                for i in range(len(integratedo3)):
                    integratedo3[i] = conv(integratedo3[i])
                    correctioncode[i] = conv(correctioncode[i])
                    sondetotalo3[i] = conv(sondetotalo3[i])
                    correctionfactor[i] = conv(correctionfactor[i])
                    totalo3[i] = conv(totalo3[i])
                    wlcode[i] = conv(wlcode[i])
                    obstype[i] = conv(obstype[i])
                    flight_instrument[i] = conv(flight_instrument[i])
                    flight_number[i] = conv(flight_number[i])

                for i in range(len(profile_pressure)):
                    profile_pressure[i] = conv(profile_pressure[i])
                    o3partialpressure[i] = conv(o3partialpressure[i])
                    temperature[i] = conv(temperature[i])
                    windspeed[i] = conv(windspeed[i])
                    winddirection[i] = conv(winddirection[i])
                    levelcode[i] = conv(levelcode[i])
                    duration[i] = conv(duration[i])
                    gpheight[i] = conv(gpheight[i])
                    relativehumidity[i] = conv(relativehumidity[i])
                    sampletemperature[i] = conv(sampletemperature[i])
                    count = count + 1

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

                try:
                    instrument_height = float(instrument_height)
                except (TypeError, ValueError, OverflowError):
                    instrument_height = None

                success = 0
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
                        'integratedo3': integratedo3,
                        'correctioncode': correctioncode,
                        'sondetotalo3': sondetotalo3,
                        'correctionfactor': correctionfactor,
                        'totalo3': totalo3,
                        'wlcode': wlcode,
                        'obstype': obstype,
                        'flight_instrument': flight_instrument,
                        'flight_number': flight_number,
                        'profile_pressure': profile_pressure,
                        'o3partialpressure': o3partialpressure,
                        'temperature': temperature,
                        'windspeed': windspeed,
                        'winddirection': winddirection,
                        'levelcode': levelcode,
                        'duration': duration,
                        'gpheight': gpheight,
                        'relativehumidity': relativehumidity,
                        'sampletemperature': sampletemperature,
                        'timestamp_date': timestamp_date,
                        'x': instrument_longitude,
                        'y': instrument_latitude,
                        'z': instrument_height,
                    }
                    ozone_object = OzoneSonde(ins_data)
                    registry_.save(ozone_object)
                    LOGGER.info(f'Inserted {ozone_object}')
                    success += 1
                except Exception as err:
                    msg = (f'Unable to insert Ozonesonde {ipath}:'
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


def generate_ozonesonde(archivedir, bypass):
    if archivedir is None:
        raise RuntimeError('Missing required on disk archive')
    LOGGER.info('Computing OzoneSonde table...')
    execute(archivedir, bypass)
