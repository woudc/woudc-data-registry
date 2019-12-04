
from woudc_data_registry.models import Instrument
from woudc_data_registry.registry import Registry

REGISTRY = Registry()


def build_instrument(ecsv):
    """
    Creates and returns an Instrument instance from the contents of <ecsv>
    """

    name = ecsv.extcsv['INSTRUMENT']['Name']
    model = str(ecsv.extcsv['INSTRUMENT']['Model'])
    serial = str(ecsv.extcsv['INSTRUMENT']['Number'])
    station = str(ecsv.extcsv['PLATFORM']['ID'])
    dataset = ecsv.extcsv['CONTENT']['Category']
    location = [ecsv.extcsv['LOCATION'].get(f, None)
                for f in ['Longitude', 'Latitude', 'Height']]

    instrument_id = ':'.join([name, model, serial, station, dataset])
    model = {
        'identifier': instrument_id,
        'name': name,
        'model': model,
        'serial': serial,
        'station_id': station,
        'dataset_id': dataset,
        'x': location[0],
        'y': location[1],
        'z': location[2]
    }

    return Instrument(model)
