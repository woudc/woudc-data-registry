import os, csv, logging, psycopg2
from urllib.request import urlopen

LOGGER = logging.getLogger(__name__)

Process=dict(
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
    cursor.execute(Process['query'])
    records = cursor.fetchall()

    cursor.close()
    connection.close()

    stations = {}
    for station_row in records:
        station_nameid = station_row[2]
        station_name_start = station_nameid.index(':') + 1   
        station_name = station_nameid[station_name_start:len(station_nameid)]
        stations[station_name.lower()] = (
                station_row[3], # station_type
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
        msg = 'Station metadata not found for the station name: %s' % name
        LOGGER.debug(msg)

        # If station metadata not found, retry station metadata
        # lookup using station name alias.
        alias = variations.get(name)
        if alias is not None:
            LOGGER.debug('Variation found for %s: %s', name, alias)
        return get_station_metadata(alias, stations, None)
    else:
        LOGGER.debug('Variation not found for %s', name)
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
        LOGGER.debug('Station name variations text not found at %s', path)
        return station_name_variations



def config_ndacc(overwrite_flag):
    """Load lookup lists required for harvesting NDACC file index metadata."""
    lookup_lists = {}
    
    station_aliases_path = os.getcwd() + Process['station_alias']

    lookup_lists['stations'] = load_station_metadata() 
    lookup_lists['name_variations'] = load_station_name_variations(station_aliases_path)

    return lookup_lists
