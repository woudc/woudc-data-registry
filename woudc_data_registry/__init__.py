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
# Copyright (c) 2019 Government of Canada
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

from woudc_data_registry import config, epicentre
from woudc_data_registry.controller import data
from woudc_data_registry.epicentre.contributor import contributor
from woudc_data_registry.epicentre.deployment import deployment
from woudc_data_registry.epicentre.instrument import instrument
from woudc_data_registry.epicentre.notification import notification
from woudc_data_registry.peer import peer
from woudc_data_registry.epicentre.station import station
from woudc_data_registry.log import setup_logger
from woudc_data_registry.models import admin
from woudc_data_registry.product import product
from woudc_data_registry.dobson_corrections import correction

__version__ = '0.3.dev0'

setup_logger(config.WDR_LOGGING_LOGLEVEL, config.WDR_LOGGING_LOGFILE)


@click.group()
@click.version_option(version=__version__)
def cli():
    pass


cli.add_command(admin)
cli.add_command(contributor)
cli.add_command(data)
cli.add_command(deployment)
cli.add_command(epicentre.dataset)
cli.add_command(epicentre.project)
cli.add_command(instrument)
cli.add_command(notification)
cli.add_command(peer)
cli.add_command(product)
cli.add_command(station)
cli.add_command(correction)
