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

import os
import logging
import psycopg2

LOGGER = logging.getLogger(__name__)

PROCESS = dict(
  # WOUDC platform station lookup
  station_query="SELECT country_id,station_id,station_name_id from stations;",
  contributor_query="SELECT name,acronym from contributors"
)


def load_metadata():
    """Load WOUDC station and agency metadata lookup list."""
    # Query PostreSQL database station table for metadata
    connection = psycopg2.connect(user=os.getenv('WDR_DB_USERNAME'),
                                  password=os.getenv('WDR_DB_PASSWORD'),
                                  host=os.getenv('WDR_DB_HOST'),
                                  port=os.getenv('WDR_DB_PORT'),
                                  database=os.getenv('WDR_DB_NAME'))
    station_cursor = connection.cursor()
    station_cursor.execute(PROCESS['station_query'])
    station_records = station_cursor.fetchall()
    station_cursor.close()

    agency_cursor = connection.cursor()
    agency_cursor.execute(PROCESS['contributor_query'])
    agency_records = agency_cursor.fetchall()
    agency_cursor.close()

    connection.close()

    lookup_lists, stations, agencies = {}, {}, {}
    for station_row in station_records:
        station_nameid = station_row[2]
        station_name_start = station_nameid.index(':') + 1
        station_name = station_nameid[station_name_start:len(station_nameid)]

        stations[station_name.lower()] = (
                station_row[0],  # country_id
                station_row[1])  # station_id
    lookup_lists['stations'] = stations

    for agency_row in agency_records:
        agencies[agency_row[0]] = (agency_row[1])  # agency acronym
    lookup_lists['agencies'] = agencies

    return lookup_lists


def get_metadata(stn_name, pi_name, lookup_lists):
    """Look up station metadata given a station name."""
    stn_key = stn_name
    agency_key = pi_name
    stations, agencies = lookup_lists['stations'], lookup_lists['agencies']
    agency_acronym, country_id, station_id = None, None, None
    if stn_key is not None:
        stn_key = stn_name.lower()
    if stn_key in stations:
        country_id = stations[stn_key][0]
        station_id = stations[stn_key][1]
    if agency_key in stations:
        agency_acronym = agencies[agency_key]

    metadata = (agency_acronym, country_id, station_id)
    return metadata


def config_lookup(overwrite_flag):
    """Load lookup lists required for harvesting file index metadata."""
    lookup_lists = {}
    lookup_lists = load_metadata()

    return lookup_lists
