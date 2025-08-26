# -*- coding: utf-8 -*-
"""
Created on Monday, 30.10.2023

@author: Karl Voglmeier, DWD (German weather service)
program reads all TEMIS Teff data, and calculates Teff climatology for each
station. Additionally, the absorption coefficient based on the SG16 dataset
(see Voglmeier et al. 2024) and based on climatological Teff are calculated,
and stored in a separate directory
"""

# -----------------------------------------------------------------------------
from __future__ import print_function
import os
import glob
import pandas as pd
import calendar

# -----------------------------------------------------------------------------

# definitions


# -----------------------------------------------------------------------------
# function & classes
def get_Temis_data(filename):
    """
    :param filename: filename of Temis data,
        downloaded from https://www.temis.nl/climate/efftemp/overpass.php
    :return: dataframe with Teff from Temis, in format which is required
        later on
    """
    # station_info = pd.read_csv(
    #     filename, header=None, delim_whitespace=True, skiprows=0, nrows=1
    # )
    # stationname = station_info[1][0]
    # lat = station_info[3][0]
    # lon = station_info[5][0]

    # read data
    # The data files give for each overpass date of observation:
    # date(UTC), time of observation: hour(UTC), effective temperature[K]
    # set header name
    headers = ["Datetime", "Hour", "Teff"]
    data = pd.read_csv(
        filename, header=None, delim_whitespace=True, skiprows=3, names=headers
    )

    # read metadata
    # Create an empty list to store the metadata
    metadata = ["# TEMIS dataset"]

    with open(filename, 'r') as file:
        for i in range(2):  # Read the first 3 lines
            # Read and remove any leading/trailing whitespace
            line = file.readline().strip()
            metadata.append(line)

    metadata = metadata + ["#", "#"]

    # convert Kelvin to degree C
    data.Teff = data.Teff - 273.15
    # sometimes there are duplicated rows, keep only first one
    data = data.drop_duplicates(keep="first")

    # make datetime
    # Teff is the average temperature at local noon
    data["Datetime"] = pd.to_datetime(data["Datetime"], format="%Y%m%d") + \
        pd.to_timedelta(12, "h")
    # keep only required columns
    data = data.loc[:, ["Datetime", "Teff"]]
    # make date
    data["Date"] = data["Datetime"].dt.date

    return (data, metadata)


def custom_day_of_year(row):
    if not calendar.isleap(row.year) and row.month > 2:
        # print(row, row.day_of_year + 1)
        return row.day_of_year + 1
    # print(row, row.day_of_year)
    return row.day_of_year


def calc_Teff_climatology(data, datetime_col="Datetime", Teff_col="Teff"):
    """
    function computes climatology of Teffs (1990-2020)
    expects df with daily values
    step1: calculation of daily climatology
    step2: rolling mean with 7 days
    :param data: Dataframe with daily Teff-data, and datetime, basis
        for calculation of climatology,
    expects column with name Datetime and column with name Teff
    :return: dataframe with climatology of Teff
    """
    # ------------------------------------------------------------------------
    # define some parameters
    climate_col_name = "Teff_climate"
    # crop data to required climate (1990 - 2020)
    startdate = "1990-01-01"
    enddate = "2020-01-01"
    # define window of rolling mean, required for Teff calculation
    rolling_days = 7
    # ------------------------------------------------------------------------

    # create new column with day of year
    data["DOY"] = data[datetime_col].apply(custom_day_of_year)

    pd.set_option('display.max_rows', None)

    # crop dataset to 1990-2020 time period
    data = data.loc[
        (data.Datetime >= startdate) & (data.Datetime < enddate), :
    ].copy()

    # set DOY 366 to 365, smoother rounding
    # data.loc[data.DOY == 366, "DOY"] = 365

    # create new dataframe with daily averages of all years
    # be sure, Teff-column is a float
    data[Teff_col] = data[Teff_col].astype(float)

    # Ensure 'Date' column is in datetime format
    data['Date'] = pd.to_datetime(data['Date'], errors='coerce')

    # Create a boolean column to check if the day is February 29,
    # irrespective of the year
    data['is_feb_29'] = (
        (data['Date'].dt.month == 2) & (data['Date'].dt.day == 29)
    )

    data['is_feb_29'] = data['is_feb_29'].astype(bool)

    # Calculate daily averages of all years
    data_year = data.groupby("DOY").mean(numeric_only=True)
    day_counts = data.groupby("DOY").size()
    data_year["Count"] = day_counts

    if data['is_feb_29'].any():
        # Example operation: Set the mean of a column to NaN for Feb 29
        data_year.loc[60, "Teff"] = (
            data_year.loc[59, "Teff"]
            + data_year.loc[60, "Teff"]
            + data_year.loc[61, "Teff"]
        ) / 3  # Modify as needed

    # print(data_year)

    # rename column, this is climate now
    data_year = data_year.rename(columns={Teff_col: climate_col_name})
    # keep index
    data_year_index = data_year.index.copy()

    # smooth Teff_series, copie one year 3 times to ensure nice smoothing at
    # the beginning / end of year
    # first, concatenate yearly averaged Teff series
    # afterwards, apply rolling mean
    data_year3 = pd.concat(
        [data_year, data_year, data_year],
        axis=0,
        ignore_index=True
    )
    data_year3[climate_col_name] = data_year3[climate_col_name].rolling(
        rolling_days, center=True, min_periods=1
    ).mean()
    data_year[climate_col_name] = data_year[climate_col_name].rolling(
        rolling_days, center=True, min_periods=1
    ).mean()
    # keep only the middle year, best rolling mean of the 3 years as
    # beginning / end of year is used
    data_year = data_year3[366:366+366].reset_index(drop=True)

    # reset index and keep DOY as column
    data_year.index = data_year_index
    data_year = data_year.reset_index()

    return data_year


def calc_absorption_coefficient(data):
    """
    Function calculates absorption coefficient based on the Bernhard slit
    approximation, and the SG16 ozone cross sections

    Parameters for the polynomial functions from Voglmeier et al. (2023)

    :param data: dataframe containing two columns, one for Day of year, one
        for effective ozone temperature Teff
    :return: dataframe with DOY, Teff, ozone absorption coefficient
    """
    # ------------------------------------------------------------------------
    # set coefficients for polynomial approximation of the ozone absorption
    # coefficient. see Voglmeier et al. (2023)
    poly_coef = {}
    poly_coef["AD"] = {}
    poly_coef["CD"] = {}
    poly_coef["AD"]["A0"] = 1.5156
    poly_coef["AD"]["A1"] = 2.4396e-03
    poly_coef["AD"]["A2"] = 1.0424e-05

    poly_coef["CD"]["A0"] = 4.9247e-01
    poly_coef["CD"]["A1"] = 1.0903e-03
    poly_coef["CD"]["A2"] = 4.8607e-06

    # temp_test = -46.3 # currently applied fixed Teff, only for test reason
    # ------------------------------------------------------------------------

    # calculate the ozone absorption coefficients
    data["O3_Abs_Coef_AD"] = (
        poly_coef["AD"]["A0"]
        + poly_coef["AD"]["A1"] * data["Teff_climate"]
        + poly_coef["AD"]["A2"] * data["Teff_climate"] ** 2
    )
    data["O3_Abs_Coef_CD"] = (
        poly_coef["CD"]["A0"]
        + poly_coef["CD"]["A1"] * data["Teff_climate"]
        + poly_coef["CD"]["A2"] * data["Teff_climate"] ** 2
    )

    # test for AD
    # poly_coef["AD"]["A0"] + poly_coef["AD"]["A1"] * temp_test
    # + poly_coef["AD"]["A2"] * temp_test ** 2
    # test for CD
    # poly_coef["CD"]["A0"] + poly_coef["CD"]["A1"] * temp_test
    # + poly_coef["CD"]["A2"] * temp_test ** 2

    return data


# **********************************************************
# main program for  __name__=="__main__":
# **********************************************************


if __name__ == "__main__":

    # ------------------------------------------------------------------------
    # directories
    # define base directory, where TEMIS Teff files are stored
    basedir = os.getcwd()
    Teff_dir = basedir + "/TEMIS_Teff/"
    WOUDC_stations_dir = basedir + "/WOUDC_stations_TEMIS_combined/"
    # ------------------------------------------------------------------------
    # files
    TEMIS_WOUDC_station_file = basedir + "/TEMIS_WOUDC_station_assignment.csv"
    # ------------------------------------------------------------------------

    # check if directory exists, if not, create it
    if not os.path.exists(WOUDC_stations_dir):
        # If it doesn't exist, create the directory
        os.mkdir(WOUDC_stations_dir)
        print(f"Directory '{WOUDC_stations_dir}' created.")
    else:
        print(f"Directory '{WOUDC_stations_dir}' already exists.")

    # ------------------------------------------------------------------------
    # define B&P ozone absorption coefficients
    o3_abs_BP_AD = 1.432
    o3_abs_BP_CD = 0.459

    # ------------------------------------------------------------------------
    # read aux file
    # read station assignment file (TEMIS -> WOUDC station), prepare metadata
    # of WOUDC station and write file for each reporting WOUDC station
    metadata_stations = pd.read_csv(
        TEMIS_WOUDC_station_file, header=0, sep=";")

    # ------------------------------------------------------------------------
    # get list of all Teff files
    all_files = glob.glob(Teff_dir + "*.dat")

    # iterate through files, read them, calculate Teff climatology and store
    # resulting data
    for file in all_files:
        # get directory name of file
        dirname = os.path.dirname(file)
        filename = os.path.basename(file)

        print("Working on file", file)

        # read TEMIS Teff data
        Teff_o3_Temis_temp, Teff_metadata = get_Temis_data(filename=file)

        # calculate 30-year climate
        Teff_o3_Temis_climatology = calc_Teff_climatology(
            data=Teff_o3_Temis_temp)

        # calculate ozone absorption coefficient, based on the Bernhard slit
        # approximation, and the SG16 ozone cross sections
        df_Teff_abs_coef = calc_absorption_coefficient(
            data=Teff_o3_Temis_climatology)

        # calculate correction factor, based on new absorption coefficient
        # and historically used absorption coefficients from B&P
        df_Teff_abs_coef["AD_correction_factor"] = (
            o3_abs_BP_AD / df_Teff_abs_coef["O3_Abs_Coef_AD"]
        )
        df_Teff_abs_coef["CD_correction_factor"] = (
            o3_abs_BP_CD / df_Teff_abs_coef["O3_Abs_Coef_CD"]
        )

        # reorder data
        df_Teff_abs_coef = df_Teff_abs_coef[
            [
                "DOY",
                "Teff_climate",
                "O3_Abs_Coef_AD",
                "AD_correction_factor",
                "O3_Abs_Coef_CD",
                "CD_correction_factor",
            ]
        ]

        # prepare output file
        df_Teff_abs_coef = df_Teff_abs_coef.rename(
            columns={"Teff_climate": "Teff_climate [°C]"}
        )

        # find all WOUDC stations which require the TEMIS data of this file
        filesearch = os.path.basename(file)
        metadata_stations_temp = metadata_stations.loc[
            metadata_stations["TEMIS_dataset_name"] == filesearch, :
        ]

        # ------------------------------------------------------------------------
        # Specify the desired number of decimal places in the output file
        decimal_places = 4

        # apply polynomial coefficients to calculate the "new" ozone
        # absorption coefficients based on SG16 and Bernhard slit approximation
        metadata_polycoef = [
            "# Calculation of Dobson Abs. Coef. based on SG16 (Weber et al.,",
            "# 2016) dataset and Bernhard et al. (2005) slit approximation",
            "# Abs_coef = A0 + A1 * Teff + A2 * Teff^2",
            "# AD: A0=1.5156, A1=2.4396e-03, A2=1.0424e-05",
            "# CD: A0=4.9247e-01, A1=1.0903e-03, A2=4.8607e-06",
            "#",
            "#"
        ]

        # ------------------------------------------------------------------------
        # iterate through WOUDC stations file, and create output files
        if metadata_stations_temp.shape[0] > 0:
            for index, row in metadata_stations_temp.iterrows():
                # create output name
                oname = (
                    WOUDC_stations_dir + "/" + str(row.platform_id) + "_"
                    + row.platform_name.replace("/", "_")
                    + "_TEMISfilename_"
                    + filename.replace("teff.dat", "teff_abscoef.dat")
                )
                # prepare metadata
                metadata_WOUDC = ["# WOUDC station information",
                                  "# Platform name: " + str(row.platform_name),
                                  "# Platform id: " + str(row.platform_id),
                                  "# Lat: " + str(row.Y),
                                  "# Lon: " + str(row.X),
                                  "#", "#"]

                # insert distance between TEMIS and WOUDC station in metadata
                distance_insert = (
                    "# Distance between WOUDC station and TEMIS location in km"
                    + ": "
                    + str(round(row.Distance))
                )
                Teff_metadata_temp = Teff_metadata.copy()
                Teff_metadata_temp.insert(3, distance_insert)

                # Open the file for writing
                with open(oname, 'w') as file:
                    # Write metadata to the file, one entry per line
                    for entry in metadata_WOUDC:
                        file.write(entry + '\n')
                    for entry in Teff_metadata_temp:
                        file.write(entry + '\n')
                    for entry in metadata_polycoef:
                        file.write(entry + '\n')

                # Append DataFrame data to the file
                df_Teff_abs_coef.to_csv(
                    oname,
                    index=False,
                    header=True,
                    sep="\t",
                    float_format=f'%.{decimal_places}f',
                    mode="a"
                )
