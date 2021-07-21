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

import os, csv, logging, psycopg2
from urllib.request import urlopen

LOGGER = logging.getLogger(__name__)

PROCESS=dict(
  # WOUDC platform station lookup
  query="SELECT gaw_id,station_id,station_name_id,station_type from stations;",  
  station_alias='/woudc_data_registry/peer/Platform_name_variations.txt',
)

def load_station_metadata():
    """Load WOUDC station metadata lookup list."""
    # Query PostreSQL database station table for metadata    
    connection = psycopg2.connect(user=os.getenv('WDR_DB_USERNAME'),
                                      password=os.getenv('WDR_DB_PASSWORD'),
                                      host=os.getenv('WDR_DB_HOST'),
                                      port=os.getenv('WDR_DB_PORT'),
                                      database=os.getenv('WDR_DB_NAME'))
    cursor = connection.cursor()
    cursor.execute(PROCESS['query'])
    records = cursor.fetchall()

    cursor.close()
    connection.close()

    stations = {}
    for station_row in records:
        station_nameid = station_row[2]
        station_name_start = station_nameid.index(':') + 1   
        station_name = station_nameid[station_name_start:len(station_nameid)]

        if station_row[3]=='STN':
            station_type_curr = 'land'
        else:
            station_type_curr = 'water'    

        stations[station_name.lower()] = (
                station_type_curr, # station_type
                station_row[1], # station_id
                station_row[0]) # gaw_id

    return stations


def get_station_metadata(name, stations, variations):
    """Look up station metadata given a station name."""
    key = name 
    if key is not None:
        key = name.lower()
    if key in stations:
        return stations[key]
    elif variations is not None:
        msg = 'Station metadata not found for the station name: {}'.format(name)
        LOGGER.debug(msg)

        # If station metadata not found, retry station metadata
        # lookup using station name alias.
        alias = variations.get(name)
        if alias is not None:
            LOGGER.debug('Variation found for {}: {}'.format(name, alias))
        return get_station_metadata(alias, stations, None)
    else:
        LOGGER.debug('Variation not found for {}'.format(name))
        return (None, None, None)


def load_station_name_variations(path):
    """Load WOUDC station name variations with OSCAR names."""
    station_name_variations = {}
    if True:
        with open(path) as station_md_file:
            station_list = csv.reader(station_md_file, delimiter=',')

            # Skip header
            next(station_list)

            # Store station names as keys and (station type, station id) as values.
            for line in station_list:
                station_name_variations[line[0].lower()] = (line[1])
 
        return station_name_variations
    else:
        LOGGER.debug('Station name variations text not found at {}'.format(path))
        return station_name_variations



def config_ndacc(overwrite_flag):
    """Load lookup lists required for harvesting NDACC file index metadata."""
    lookup_lists = {}
    
    station_aliases_path = os.getcwd() + PROCESS['station_alias']

    lookup_lists['stations'] = load_station_metadata() 
    lookup_lists['name_variations'] = load_station_name_variations(station_aliases_path)

    return lookup_lists
