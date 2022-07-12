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
# Copyright (c) 2021 Government of Canada
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

import csv
import logging
import os

from datetime import datetime
from woudc_extcsv import ExtendedCSV

from woudc_data_registry.models import UVIndex, Instrument
from woudc_data_registry import config, registry
from woudc_data_registry.util import read_file
from woudc_data_registry.epicentre.metadata import add_metadata

LOGGER = logging.getLogger(__name__)


def execute(path, formula_lookup, update, start_year, end_year, bypass):
    """
    Orchestrate uv-index generation process
    """

    datasets = [
        '/'.join([path, 'Spectral_1.0_1']),
        '/'.join([path, 'Broad-band_1.0_1']),
        '/'.join([path, 'Spectral_2.0_1']),
        '/'.join([path, 'Broad-band_2.0_1']),
    ]

    registry_ = registry.Registry()

    if not update:
        LOGGER.info('erasing current uv index')

        registry_.session.query(UVIndex).delete()
        registry_.save()

    # traverse directory of files
    for dataset in datasets:
        for dirname, dirnames, filenames in os.walk(dataset):
            # only ingest years within range for update command
            if update:
                split_dir = dirname.split('/')
                # determine if file year is valid for ingest
                valid_year = True
                for base in split_dir:
                    if base.isnumeric():
                        int_dir = int(base)
                        if end_year and int(end_year) < int_dir:
                            valid_year = False
                            break
                        elif start_year and int(start_year) > int_dir:
                            valid_year = False
                            break

                if not valid_year:
                    continue

            for filename in filenames:

                ipath = os.path.join(dirname, filename)
                contents = read_file(ipath)
                LOGGER.debug('Parsing extcsv {}'.format(ipath))

                try:
                    extcsv = ExtendedCSV(contents)
                except Exception as err:
                    msg = 'Unable to parse extcsv {}: {}'.format(ipath, err)
                    LOGGER.error(msg)
                    continue

                # get common fields
                try:
                    dataset = extcsv.extcsv['CONTENT']['Category'][0]
                    level = extcsv.extcsv['CONTENT']['Level'][0]
                    form = extcsv.extcsv['CONTENT']['Form'][0]
                    project_id = extcsv.extcsv['CONTENT']['Class'][0]
                    station_type = extcsv.extcsv['PLATFORM']['Type'][0]
                    station_id = extcsv.extcsv['PLATFORM']['ID'][0]
                    gaw_id = extcsv.extcsv['PLATFORM']['GAW_ID'][0]
                    country = extcsv.extcsv['PLATFORM']['Country'][0]
                    agency = extcsv.extcsv['DATA_GENERATION']['Agency'][0]
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
                    msg = 'Unable to get data from extcsv {}: {}'.format(
                        ipath, err)
                    LOGGER.error(msg)
                    continue

                if len(station_id) == 2:
                    station_id = station_id.zfill(3)

                station = '{}{}'.format(station_type.upper(), station_id)

                if station in formula_lookup.keys():
                    if dataset.lower() == 'spectral':
                        # find max set of table groupings
                        summary_table = 'GLOBAL_SUMMARY_NSF' \
                            if 'GLOBAL_SUMMARY_NSF' in extcsv.extcsv \
                            else 'GLOBAL_SUMMARY'

                        timestamp_count = extcsv.table_count('TIMESTAMP')
                        global_count = extcsv.table_count('GLOBAL')
                        summary_count = extcsv.table_count(summary_table)

                        try:
                            max_index = max(timestamp_count, global_count,
                                            summary_count)
                        except ValueError:
                            max_index = 1
                        try:
                            uv_packages = compute_uv_index(ipath, extcsv,
                                                           dataset, station,
                                                           instrument_name,
                                                           country,
                                                           formula_lookup,
                                                           max_index)
                        except Exception as err:
                            msg = 'Unable to compute UV for file {}: {}'.format(  # noqa
                                ipath, err)
                            LOGGER.error(msg)
                            continue
                    elif dataset.lower() == 'broad-band':
                        try:
                            uv_packages = compute_uv_index(ipath, extcsv,
                                                           dataset, station,
                                                           instrument_name,
                                                           country,
                                                           formula_lookup)
                        except Exception as err:
                            msg = 'Unable to compute UV for file {}: {}'.format(  # noqa
                                ipath, err)
                            LOGGER.error(msg)
                            continue
                    else:
                        msg = 'Unsupported dataset {}. Skipping.'.format(
                            dataset)
                        LOGGER.error(msg)

                    # form ids for data insert
                    contributor_id = ':'.join([agency, project_id])
                    deployment_id = ':'.join([station_id, contributor_id])
                    instrument_id = ':'.join([instrument_name,
                                              instrument_model,
                                              instrument_number, dataset,
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
                                input('Instrument {} not found. '
                                      'Add? (y/n) [n]: '
                                      .format(instrument_id))
                            allow_add_instrument = \
                                response.lower() in ['y', 'yes']

                        if allow_add_instrument:
                            instrument_ = {
                                'station_id': station_id,
                                'dataset_id': dataset,
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

                    # compute max daily uv index value
                    uv_max = None
                    for package in uv_packages:
                        if uv_max:
                            uv_max = max(package['uv'], uv_max)
                        else:
                            uv_max = package['uv']

                    # insert and save uv index model objects
                    for package in uv_packages:
                        ins_data = {
                            'file_path': ipath,
                            'filename': filename,
                            'dataset_id': dataset,
                            'dataset_level': level,
                            'dataset_form': form,
                            'station_id': station_id,
                            'station_type': station_type,
                            'country_id': country,
                            'instrument_id': instrument_id,
                            'instrument_name': instrument_name,
                            'gaw_id': gaw_id,
                            'solar_zenith_angle': package['zen_angle'],
                            'timestamp_date': timestamp_date,
                            'observation_date': package['date'],
                            'observation_time': package['time'],
                            'observation_utcoffset': package['utcoffset'],
                            'uv_index': package['uv'],
                            'uv_daily_max': uv_max,
                            'uv_index_qa': package['qa'],
                            'x': instrument_longitude,
                            'y': instrument_latitude,
                            'z': instrument_height,
                        }
                        uv_object = UVIndex(ins_data)
                        registry_.save(uv_object)

    LOGGER.debug('Done get_data().')


def compute_uv_index(ipath, extcsv, dataset, station,
                     instrument_name, country,
                     formula_lookup, max_index=1):
    """
    Compute UV index
    """

    LOGGER.debug('Executing compute_uv_index()...')

    uv_packages = []

    if all([
        dataset.lower() == 'spectral',
            any([
            'biospherical' in instrument_name.lower(),
            'brewer' in instrument_name.lower()
            ])
    ]):
        # get formula
        try:
            formula = (formula_lookup[station]
                                     [instrument_name.lower()]
                                     ['GLOBAL_SUMMARY'])
        except KeyError:
            formula = (formula_lookup[station]
                                     [instrument_name.lower()]
                                     ['GLOBAL_SUMMARY_NSF'])

        # Some spectral files are missing TIMESTAMP per each payload
        # Get and store first Date
        common_date = None

        for index in range(1, max_index + 1):
            package = {
                'uv': None,
                'date': None,
                'time': None,
                'utcoffset': None,
                'zen_angle': None,
                'qa': None
            }

            if index == 1:
                timestamp_t = 'TIMESTAMP'
                global_summary_t = 'GLOBAL_SUMMARY'
                global_summary_nsf_t = 'GLOBAL_SUMMARY_NSF'
            else:
                timestamp_t = '_'.join(['TIMESTAMP', str(index)])
                global_summary_t = '_'.join(['GLOBAL_SUMMARY', str(index)])
                global_summary_nsf_t = '_'.join(['GLOBAL_SUMMARY_NSF',
                                                 str(index)])

            # common spectral fields
            try:
                date = extcsv.extcsv[timestamp_t]['Date'][0]
                if index == 1:
                    common_date = date
                if any([
                    date is None,
                    date == ''
                ]):
                    # for stations without TIMESTAMP per GLOBAL_SUMMARY
                    date = common_date
                time = extcsv.extcsv[timestamp_t]['Time'][0]
                if any([
                    time is None,
                    time == ''
                ]):
                    # for stations without TIMESTAMP per GLOBAL_SUMMARY
                    time = extcsv.extcsv[global_summary_t]['Time'][0]
                utcoffset = extcsv.extcsv[timestamp_t]['UTCOffset'][0]
            except Exception as err:
                msg = 'Unable to get value from file {}: {}'.format(
                    ipath, err)
                LOGGER.error(msg)
                pass

            if instrument_name.lower() == 'biospherical':  # available in file
                try:
                    uv = extcsv.extcsv[global_summary_nsf_t]['UVIndex'][0]
                    try:
                        uv = float(uv)
                    except ValueError as err:
                        msg = ('Unable to make UVIndex: {} value into a float.'
                               ' Time: {}, file: {}: {}'.format(uv, time,
                                                                ipath, err))
                        LOGGER.error(msg)
                        pass
                except Exception as err:
                    msg = ('Unable to get {}.UVIndex'
                           ' from file: {}. Time: {}: {}'.format(
                               global_summary_nsf_t, ipath, time, err))
                    LOGGER.error(msg)
                    pass

                try:
                    zen_angle = extcsv.extcsv[global_summary_nsf_t]['SZA'][0]
                except Exception as err:
                    msg = ('Unable to get {}.SZA from file {}: {}'.format(
                           global_summary_nsf_t, ipath, err))
                    LOGGER.error(msg)
                    pass

            if instrument_name.lower() == 'brewer':
                try:
                    intcie = extcsv.extcsv[global_summary_t]['IntCIE'][0]
                    # convert sci not to float
                    try:
                        intcie_f = float(intcie)
                    except Exception as err:
                        msg = ('Unable to convert to float intcie:'
                               ' {}. File: {}. Time: {}: {}'.format(
                                   intcie, ipath, time, err))
                        LOGGER.error(msg)
                        continue
                    # compute
                    if '*' in formula:
                        uv = intcie_f * 25
                    elif '/' in formula:
                        uv = intcie_f / 40
                    else:
                        msg = 'Unknown formula: {}'.format(formula)
                        LOGGER.error(msg)
                        continue

                    try:
                        zen_angle = \
                            extcsv.extcsv[global_summary_t]['ZenAngle'][0]
                    except Exception as err:
                        msg = ('Unable to get {}.ZenAngle from file: {}'
                               'Time: {}: {}'.format(
                                  global_summary_t, ipath, time, err))
                        LOGGER.error(msg)
                        pass

                except Exception as err:
                    msg = ('Unable to get  {}.IntCIE from file: {}. Time: {}.'
                           ': {}'.format(global_summary_t, ipath, time, err))
                    LOGGER.error(msg)
                    continue

            qa_result = qa(country, uv)

            package['uv'] = uv
            package['date'] = date
            package['time'] = time
            package['utcoffset'] = utcoffset
            package['zen_angle'] = zen_angle
            package['qa'] = qa_result

            uv_packages.append(package)

    if all([
        dataset.lower() == 'broad-band',
        any([
            'biometer' in instrument_name.lower(),
            'kipp_zonen' in instrument_name.lower(),
            ])
    ]):

        try:
            if 'biometer' in instrument_name.lower():
                formula = formula_lookup[station]['biometer']['GLOBAL']
            if instrument_name.lower() == 'kipp_zonen':
                formula = formula_lookup[station]['kipp_zonen']['GLOBAL']
        except KeyError as err:
            msg = ('Unable to get broad-band formula for file {}: {}'.format(
                       ipath, err))
            LOGGER.error(msg)
            raise err

        # common broad-band fields
        try:
            date = extcsv.extcsv['TIMESTAMP']['Date'][0]
            utcoffset = extcsv.extcsv['TIMESTAMP']['UTCOffset'][0]
        except Exception as err:
            msg = 'Unable to get value from file {}: {}'.format(ipath, err)
            LOGGER.error(msg)
            raise err

        # get payload values
        try:
            times = extcsv.extcsv['GLOBAL']['Time']
        except Exception as err:
            msg = ('Unable to get GLOBAL.Time values from file {}: {}'
                   'Trying DIFFUSE.Time'.format(ipath, err))
            LOGGER.error(msg)
        # try DIFFUSE
        if times is None:
            try:
                times = extcsv.extcsv['DIFFUSE']['Time']
            except Exception as err:
                msg = 'Unable to get DIFFUSE.Time {}: {}'.format(ipath, err)
                LOGGER.error(msg)
                raise err

        # clean up times
        times = list(filter(lambda a: a != '', times))

        try:
            irradiances = extcsv.extcsv['GLOBAL']['Irradiance']
        except Exception as err:
            msg = ('Unable to get GLOBAL.Irradiance values from file {}:'
                   '{}. Trying DIFFUSE.Irradiance'.format(ipath, err))
            LOGGER.error(msg)
        # try DIFFUSE
        if irradiances is None:
            try:
                irradiances = extcsv.extcsv['DIFFUSE']['Irradiance']
            except Exception as err:
                msg = ('Unable to get DIFFUSE.Irradiance values from file'
                       '{}: {}'.format(ipath, err))
                LOGGER.error(msg)
                raise err

        for i in range(0, len(times)):
            package = {
                'uv': None,
                'date': None,
                'time': None,
                'utcoffset': None,
                'zen_angle': None,
                'qa': None
            }
            time = times[i]
            irradiance = irradiances[i]
            if '*' in formula:
                try:
                    irradiance_f = float(irradiance)
                except Exception:
                    msg = ('Unable to make float for irradiance:'
                           ' {}. Time: {}'.format(irradiance, time))
                    LOGGER.error(msg)
                    continue

                uv = irradiance_f * 40

                qa_result = qa(country, uv)

                package['uv'] = uv
                package['date'] = date
                package['time'] = time
                package['utcoffset'] = utcoffset
                package['zen_angle'] = None
                package['qa'] = qa_result

                uv_packages.append(package)

    LOGGER.debug('Done compute_uv_index().')

    return uv_packages


def qa(country, uv):
    """
    Do qa on uv-index value:
    Rule:
        0 <= uv < 12   --> pass (P)
        12 <= uv < 17  --> doubtful (D)
        >= 17            --> error (E)
        all else        --> not applicable (NA)
    """
    if not isinstance(uv, float):
        return 'NA'
    if 0 <= uv < 12:
        return 'P'
    elif 12 <= uv < 17:
        return 'D'
    elif uv >= 17:
        return 'E'
    else:
        return 'E'


def generate_uv_index(archivedir, update, start_year, end_year, bypass):
    if archivedir is None:
        raise RuntimeError('Missing required on disk archive')

    # load formula-lookup
    LOGGER.info('Loading formula lookup resource...')
    resource_dir = config.WDR_UV_INDEX_FORMULA_LOOKUP
    formula_lookup = {}
    csv_rows = csv.reader(open(resource_dir))

    for row in csv_rows:
        station_id = row[0].strip()
        brewer_formula = row[1].strip()
        biosphere_formula = row[5].strip()
        biometer_formula = row[9].strip()
        kipp_formula = row[12].strip()

        if station_id != '':
            if station_id not in formula_lookup.keys():
                formula_lookup[station_id] = {
                    'brewer': {},
                    'biospherical': {},
                    'biometer': {},
                    'kipp_zonen': {}
                    }
                if brewer_formula != '':
                    form_toks = brewer_formula.split('.')
                    table = form_toks[0]
                    formula = form_toks[1]
                    formula_lookup[station_id]['brewer'][table] = formula
                if biosphere_formula != '':
                    form_toks = biosphere_formula.split('.')
                    table = form_toks[0]
                    formula = form_toks[1]
                    formula_lookup[station_id]['biospherical'][table] = formula
                if biometer_formula != '':
                    form_toks = biometer_formula.split('.')
                    table = form_toks[0]
                    formula = form_toks[1]
                    formula_lookup[station_id]['biometer'][table] = formula
                if kipp_formula != '':
                    form_toks = kipp_formula.split('.')
                    table = form_toks[0]
                    formula = form_toks[1]
                    formula_lookup[station_id]['kipp_zonen'][table] = formula

    LOGGER.info('Loaded formula lookup resource.')

    LOGGER.info('Computing UV-index...')
    execute(archivedir, formula_lookup, update, start_year, end_year, bypass)
    LOGGER.info('Done.')
