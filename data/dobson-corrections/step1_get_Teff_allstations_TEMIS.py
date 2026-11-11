# -*- coding: utf-8 -*-
"""
Created on Monday, 12.12.2022

@author: Karl Voglmeier, DWD (German Weather Service)
2023-10-30
program downloads all available Teff-data from TEMIS Teff website
"https://www.temis.nl/climate/efftemp/overpass.php"
"""

# -----------------------------------------------------------------------------
import os
import requests
from bs4 import BeautifulSoup


# -----------------------------------------------------------------------------

# definitions


# -----------------------------------------------------------------------------
# function & classes

# **********************************************************
# main program for  __name__=="__main__":
# **********************************************************


if __name__ == "__main__":

    # define path to TEMIS files
    URL = "https://www.temis.nl/climate/efftemp/overpass.php"
    ext = ".dat"

    # save files in subdirectory
    current_directory = os.getcwd()
    output_directory = current_directory + "/TEMIS_Teff/"

    # check if directory exists, if not, create it
    if not os.path.exists(output_directory):
        # If it doesn't exist, create the directory
        os.mkdir(output_directory)
        print(f"Directory '{output_directory}' created.")
    else:
        print(f"Directory '{output_directory}' already exists.")

    # get TEMIS data
    r = requests.get(URL)
    data = r.text
    soup = BeautifulSoup(data)

    for link in soup.find_all('a'):
        if link.get('href') is None:
            continue

        # only download files with .dat ending
        if not (".dat" in link.get('href')):
            continue

        print(link.get('href'))
        print("Download file")

        oname = output_directory + os.path.basename(link.get('href'))
        response_file = requests.get("https:" + link.get('href'))

        open(oname, "wb").write(response_file.content)
