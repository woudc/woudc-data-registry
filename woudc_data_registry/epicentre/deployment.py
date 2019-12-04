
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


@click.group()
def deployment():
    """Deployment management"""
    pass


@click.command('list')
@click.pass_context
def list_(ctx):
    """List all deployments"""

    for c in get_metadata(Deployment):
        click.echo('{} @ {}'.format(c.contributor_id.ljust(15), c.station_id))


@click.command('show')
@click.pass_context
@click.argument('identifier', required=True)
def show(ctx, identifier):
    """Show deployment details"""

    r = get_metadata(Deployment, identifier)

    if len(r) == 0:
        click.echo('Deployment not found')
        return

    click.echo(json.dumps(r[0].__geo_interface__, indent=4,
                          default=json_serial))


@click.command('add')
@click.option('-id', '--identifier', 'identifier', required=True,
              help='identifier')
@click.option('-s', '--station', 'station', required=True, help='station')
@click.option('-c', '--contributor', 'contributor', required=True,
              help='contributor')
@click.option('--start', 'start_date', required=False, default=date.today(),
              help='deployment start date')
@click.option('--end', 'end_date', required=False, default=None,
              help='deployment end date')
@click.pass_context
def add(ctx, identifier, station, contributor, start_date, end_date):
    """Add a deployment"""

    deployment_ = {
        'identifier': identifier,
        'station_id': station,
        'contributor_id': contributor,
        'start_date': start_date,
        'end_date': end_date
    }

    add_metadata(Deployment, deployment_)
    click.echo('Deployment {} added'.format(identifier))


@click.command('update')
@click.option('-id', '--identifier', 'identifier', required=True,
              help='acronym')
@click.option('-n', '--name', 'name', help='name')
@click.option('-c', '--country', 'country', help='country')
@click.option('-w', '--wmo-region', 'wmo_region', help='WMO region')
@click.option('-u', '--url', 'url', help='URL')
@click.option('-e', '--email', 'email', help='email')
@click.option('-f', '--ftp-username', 'ftp_username', help='FTP username')
@click.option('-g', '--geometry', 'geometry', help='latitude,longitude')
@click.pass_context
@click.option('-id', '--identifier', 'identifier', required=True,
              help='identifier')
@click.option('-s', '--station', 'station', help='station')
@click.option('-c', '--contributor', 'contributor', help='contributor')
@click.option('--start', 'start_date', help='deployment start date')
@click.option('--end', 'end_date', help='deployment end date')
@click.pass_context
def update(ctx, identifier, station, contributor, start_date, end_date):
    """Update deployment information"""

    deployment_ = {}

    if station:
        deployment_['station_id'] = station
    if contributor:
        deployment_['contributor_id'] = contributor
    if start_date:
        deployment_['start_date'] = start_date
    if end_date:
        deployment_['end_date'] = end_date

    if len(deployment_.keys()) == 0:
        click.echo('No updates specified')
        return

    update_metadata(Deployment, identifier, deployment_)
    click.echo('Deployment {} updated'.format(identifier))


@click.command('delete')
@click.argument('identifier', required=True)
@click.pass_context
def delete(ctx, identifier):
    """Delete a deployment"""

    if len(get_metadata(Deployment, identifier)) == 0:
        click.echo('Contributor not found')
        return

    q = 'Are you sure you want to delete deployment {}?'.format(identifier)

    if click.confirm(q):  # noqa
        delete_metadata(Deployment, identifier)

    click.echo('Deployment {} deleted'.format(identifier))


deployment.add_command(list_)
deployment.add_command(show)
deployment.add_command(add)
deployment.add_command(update)
deployment.add_command(delete)
