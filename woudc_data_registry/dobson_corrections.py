#!/usr/bin/env python3

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
# Copyright (c) 2025 Government of Canada
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

import click
import logging

from datetime import datetime
import calendar
import os

from woudc_data_registry import config
from woudc_data_registry.registry import Registry
from woudc_data_registry.models import StationDobsonCorrections

LOGGER = logging.getLogger(__name__)

# Global Variable
csv_content = ''
TABLES = [
    'CONTENT', 'DATA_GENERATION', 'PLATFORM',
    'INSTRUMENT', 'LOCATION', 'TIMESTAMP', 'DAILY',
    'MONTHLY'
    ]


def parse_csv(in_path):
    """
    Parse the csv file by creating a list with all the tables.
    The list contains the tables in the csv.

    :param in_path: The path of the csv file
    """
    tables = {}
    current_table = None
    current_headers = None

    try:
        with open(in_path, 'r', encoding='utf-8', errors='ignore') as infile:
            for line in infile:
                i = 0
                line = line.strip()

                # Identify table titles (lines starting with '#')
                if line.startswith('#'):
                    # Start of a new table
                    current_table = line[1:].strip()
                    if current_table in TABLES and current_table not in tables:
                        tables[current_table] = []
                        current_headers = None

                elif line.startswith('*'):
                    # Empty line signifies the end of the current table
                    current_table = None

                elif line == '':
                    i += 1

                elif current_table:
                    # If headers are not set,
                    # set them as the first non-empty line
                    if current_headers is None:
                        current_headers = line.split(',')
                        tables[current_table].append(current_headers)
                    else:
                        # Add data rows to the current table
                        tables[current_table].append(line.split(','))
    except Exception as e:
        LOGGER.error("File unable to parse: %s", e)
    return tables


def parse_dat(file_path):
    """
    Parse the dat file by creating a list with.
    The list contains the data for the different DOY.

    :param in_path: The path of the dat file
    """
    data = {}
    table_started = False
    with open(file_path, 'r') as file:
        for line in file:
            # Strip whitespace
            line = line.strip()

            # Ignore comments
            if line.startswith("#"):
                continue

            # Detect the header line and start reading the table
            if line.startswith("DOY"):
                # headers = line.split()
                table_started = True
                continue

            # Read the table data
            if table_started and line:
                # split the line by whitespaces and make it into a dict where
                # the doy is the key and the rest of the line is the value
                row = line.split()
                data[row[0]] = row[1:]
    return data


def get_correct_factor(data, wavelength, doy):
    """
    Return the correction factor from the dat files.

    :param data: List of correction data from the dat file
    :param wavelength: String that determine which correction works
    :param doy: Day of year as an int
    """
    row = data.get(str(doy))
    if wavelength == 'AD':
        return float(row[2]), float(row[0])
    elif wavelength == 'CD':
        return float(row[4]), float(row[0])
    else:
        return 'Unknown Wavelength'


def custom_day_of_year(date):
    """
    This function returned the day of year for the date given and it
    makes sure that for every year march 1st is day 61. To leave space
    for february 29.

    :param date: datatime object
    """
    if not calendar.isleap(date.year) and date.month > 2:
        return int(date.strftime('%j')) + 1
    return int(date.strftime('%j'))


def find_new_directory(csv_file, weeklyingest):
    """
    Find the directory where to move the file to.
    For mass correction, the corrected files will go to
    {WDR_DOBSON_CORRECTION_OUTPUT}/TotalOzone_1.0_1_corrected/{stnXXX}/dobson/{year}/

    :param csv_file: Data in the csv file

    :param weeklyingest: True or false to place corrected files in the form

    {WDR_DOBSON_CORRECTION_OUTPUT}/TotalOzone_1.0_1_corrected/{acronym}/
    """
    year = ''
    directory = ''

    platform_content = csv_file.get('PLATFORM')
    timestamp_content = csv_file.get('TIMESTAMP')

    data_generation_content = csv_file.get('DATA_GENERATION')

    station_id = platform_content[1][
        platform_content[0].index('ID')] if platform_content else ''

    if timestamp_content:
        date_index = timestamp_content[0].index('Date')
        year = timestamp_content[1][date_index][:4]
    else:
        year = ''

    if data_generation_content:
        acronym = data_generation_content[1][
            data_generation_content[0].index('Agency')]
    else:
        acronym = ''

    if weeklyingest:
        directory = (
            f"{config.WDR_DOBSON_CORRECTION_OUTPUT}/"
            f"TotalOzone_1.0_1_corrected/{acronym.lower()}/"
        )
    else:
        directory = (
            f"{config.WDR_DOBSON_CORRECTION_OUTPUT}/"
            f"TotalOzone_1.0_1_corrected/stn{station_id}/dobson/{year}/"
        )

    # Check if the directory exists, if not, create it
    if not os.path.exists(directory):
        os.makedirs(directory)
        LOGGER.info(f"Directory {directory} created.")
    else:
        LOGGER.info(f"Directory {directory} already exists.")

    return directory


def find_dat_file(station_id):
    """
    To Correct the files, we have a list of dat files corresponding
    to different stations in the WOUDC_stations_TEMIS_combined folder
    and each dat file has a different set of correction factors.
    Depending on the csv file, we need to find the corresponding
    dat file that has the correction factors.

    :param station_id: string of the station id
    """
    dat_file_path = ''
    folder = config.WDR_DOBSON_STATION_TEMIS
    for root, dirs, files in os.walk(folder):
        for file in files:
            if (
                os.path.basename(file).lower().startswith(
                    station_id.lower() + '_'
                )
            ):
                dat_file_path = os.path.join(folder, os.path.basename(file))
                continue
    return dat_file_path


def fix_line_commas(line, comma_number):
    while line.count(',') < comma_number:
        line += ","
    return line


def correct_file(csv_file, csv_content, code, mode):
    """
    Correct the file using the data_file_path.

    :param csv_file_path: File that needs to be corrected.

    :param csv_content: Content of the file that needs to be corrected.

    :param code: Code to determine which correction to apply.

    :param mode: Mode of operation, either 'test' or 'ops'.

    :return: The corrected csv content.
    """
    platform_content = csv_file.get('PLATFORM')
    print(platform_content)
    station_id = platform_content[1][
        platform_content[0].index('ID')] if platform_content else ''

    print(f"Station Name: {station_id}")

    try:
        dat_file_path = find_dat_file(station_id.lstrip('0'))
        print(f"Dat File Path: {dat_file_path}")
        dat_file = parse_dat(dat_file_path)
    except FileNotFoundError:
        LOGGER.error(f"Dat file not found for station: {station_id}")
        return

    if 'DAILY' in csv_file:
        LOGGER.info('DAILY is in csv_file')
        data = csv_file['DAILY']
        # station_id = csv_file['PLATFORM'][1]
        print(station_id)
        # Getting the index of wlcode and columns so we can use them
        # to check the values of each line.
        wlcode_ind = data[0].index('WLCode')
        column03_ind = data[0].index('ColumnO3')

        # Get the first line to correct the files
        replacing_string = ','.join(data[0])
        # Count the numbers of commas to later correct the number of
        # commas in the data lines -> less errors later
        comma_number = replacing_string.count(',')
        if mode.lower() == 'test':
            replacement_string = (
                f"{replacing_string},CorrectionFactor,TeffClimate,"
                f"Coeff,CorrectedColumnO3,DeltaCorrectedColumnO3,"
                f"DeltaPercentageColumnO3"
            )
        elif mode.lower() == 'ops':
            replacement_string = f"{replacing_string},Coeff"

        # Replace the first line in the file
        csv_content = csv_content.replace(
                replacing_string, replacement_string)

        # Get code from StationDobsonCorrections table and see if
        # correction is required.
        # Get the Dobson correction code from the database
        registry = Registry()
        dobson_correction = registry.query_by_field(
            StationDobsonCorrections, 'station_id', station_id)
        if dobson_correction:
            AD_source = dobson_correction.AD_correcting_source
            CD_source = dobson_correction.CD_correcting_source
            CD_correcting_factor = dobson_correction.CD_correcting_factor
        correct_AD = (
            AD_source == 'ECCC' and code in ['AD', None]
        )
        correct_CD = (
            CD_source == 'ECCC' and code in ['CD', None]
        )
        registry.close_session()

        # Go through each line in the DAILY table
        for i in data[1:]:
            if len(i) < 3:
                continue

            date = datetime.strptime(i[0], '%Y-%m-%d')
            day_of_year = custom_day_of_year(date)

            # Get the columnO3 (the value that needs to be corrected)
            column03 = float(i[column03_ind])
            wlcode = float(i[wlcode_ind])

            Coeff = ''
            ColumnO3Corrected = ''

            # Get the correcting coefficient and corrected the columnO3
            if (wlcode in [0, 4] and correct_AD and
                    (code is None or code == 'AD')):
                Coeff, teff_climate = get_correct_factor(
                    dat_file, 'AD', day_of_year
                )
                ColumnO3Corrected = column03 * Coeff
                correction_factor = 'AD'
            elif (wlcode in [2, 6] and correct_CD and
                  (code is None or code == 'CD')):
                if CD_correcting_factor == 'CD':
                    Coeff, teff_climate = get_correct_factor(
                        dat_file, 'CD', day_of_year
                    )
                    correction_factor = 'CD'
                elif CD_correcting_factor == 'AD':
                    Coeff, teff_climate = get_correct_factor(
                        dat_file, 'AD', day_of_year
                    )
                    correction_factor = 'AD'
                ColumnO3Corrected = column03 * Coeff

            # Create the fixed lines
            replacing_string = ",".join(i)

            deltaColumnO3corrected = (
                ColumnO3Corrected - column03
            )
            deltaPercentageColumnO3 = (
                (deltaColumnO3corrected / column03) * 100
            )

            if mode == "test":
                replacement_string = (
                    f"{fix_line_commas(replacing_string, comma_number)},"
                    f"{correction_factor},{teff_climate},{Coeff},"
                    f"{ColumnO3Corrected},{deltaColumnO3corrected},"
                    f"{deltaPercentageColumnO3}"
                )
            elif mode == "ops":
                line = fix_line_commas(replacing_string, comma_number)

                colo3_updated_string = line.replace(
                    str(column03),
                    str(ColumnO3Corrected)
                )
                replacement_string = f"{colo3_updated_string},{Coeff}"

            # Correct the line in the file
            csv_content = csv_content.replace(
                replacing_string, replacement_string)
    return csv_content


def copy_file(file_path, csv_content, weeklyingest):
    """
    Copy the file to the new location with the corrected content.
    """
    # Get the directory to where you want to move the file
    destination_path = find_new_directory(parse_csv(file_path), weeklyingest)

    if file_path.endswith('.csv'):
        basename = os.path.basename(file_path)
        new_file_name = basename.replace(
            '.csv', '_dobson_correction_applied.csv'
        )
    else:
        new_file_name = (
            os.path.basename(file_path) +
            '_dobson_correction_applied.csv'
        )

    # Check if the file exists in the destination_path then remove it
    if os.path.exists(
        os.path.join(destination_path, new_file_name)
    ):
        os.remove(os.path.join(destination_path, new_file_name))
        old_file = os.path.join(destination_path, new_file_name)
        LOGGER.warning(f"File already exists, removed {old_file}")

    new_file_path = os.path.join(destination_path, new_file_name)
    with open(new_file_path, 'w') as f:
        f.write(csv_content)
        LOGGER.info(
                    f"File moved from {file_path} to {destination_path}"
                )


def controller(directory, code, mode, weeklyingest):
    # Walk through the directory and its subdirectories
    for root, dirs, files in os.walk(directory):
        LOGGER.debug(f"Working on directory: {root}")
        for file in files:
            file_path = os.path.join(root, file)
            if os.path.isfile(file_path):
                LOGGER.info(f"Working on file: {file_path}")
                csv_file = parse_csv(file_path)
                print(csv_file)

                if csv_file == {}:
                    LOGGER.error(f"{file} could not be corrected.")
                    continue

                try:
                    instrument = (
                        csv_file['INSTRUMENT'][1][0].lower() != 'dobson'
                    )
                    content = csv_file['CONTENT'][1][1].lower() != 'totalozone'
                    with open(file_path, 'r') as f:
                        csv_content = f.read()
                except Exception as e:
                    LOGGER.error(f"{file} could not be corrected: {e}")
                    continue

                if instrument or content:
                    LOGGER.info("{} not a dobson file".format(file))
                    LOGGER.info("Skipping {}".format(file))
                    continue

                # Correct file
                try:
                    csv_content = correct_file(csv_file, csv_content,
                                               code, mode)
                except Exception as e:
                    LOGGER.error(f"{file} could not be corrected: {e}")
                    continue

                LOGGER.info('Corrected File: {}'.format(file))

            try:
                copy_file(file_path, csv_content, weeklyingest)
            except TypeError as e:
                LOGGER.error(f"{file} could not be copied: {e}")
                continue


@click.group()
def correction():
    """Correcting data"""
    pass


@click.command()
@click.pass_context
@click.argument('directory', type=click.Path(exists=True))
@click.option('--code', type=click.Choice(['AD', 'CD']),
              default=None, show_default=True,
              help="Choose 'AD' or 'CD' for wavelength code correction.")
@click.option('--weeklyingest', is_flag=True,
              help="Enable the weekly ingest flag.")
@click.option('--mode', type=click.Choice(['test', 'ops']),
              help="Choose 'test' or 'ops' for the mode.")
def dobson_correction(ctx, directory, code, mode, weeklyingest):
    """ Correct columnO3 in TotalOZone Dobson files"""
    if mode is None:
        click.echo(
            "No mode selected. Use --mode option to specify "
            "'test' or 'ops'."
        )
        return
    if mode.lower() == 'test':
        click.echo("Test mode enabled.")
    elif mode.lower() == 'ops':
        click.echo("Ops mode enabled.")
    else:
        click.echo("No correction type selected.")
        return

    controller(directory, code, mode, weeklyingest)

    LOGGER.info("Done")


correction.add_command(dobson_correction, name='dobson-correction')
