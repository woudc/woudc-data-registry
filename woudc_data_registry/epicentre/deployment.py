
from woudc_data_registry.models import Deployment


def build_deployment(ecsv):
    """
    Creates and returns a Deployment instance from the contents of <ecsv>
    """

    station = str(ecsv.extcsv['PLATFORM']['ID'])
    agency = ecsv.extcsv['DATA_GENERATION']['Agency']
    project = ecsv.extcsv['CONTENT']['Class']
    timestamp_date = ecsv.extcsv['TIMESTAMP']['Date']

    contributor_id = ':'.join([agency, project])
    deployment_id = ':'.join([station, agency, project])
    deployment_model = {
        'identifier': deployment_id,
        'station_id': station,
        'contributor_id': contributor_id,
        'start_date': timestamp_date,
        'end_date': timestamp_date
    }

    return Deployment(deployment_model)
