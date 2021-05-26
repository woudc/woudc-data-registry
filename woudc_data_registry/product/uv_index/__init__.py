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

import click
from woudc_data_registry.product.uv_index.uv_index_generator \
    import generate_uv_index


@click.group()
def uv_index():
    """UV Index management"""
    pass


@click.command()
@click.pass_context
@click.argument('srcdir', type=click.Path(exists=True, resolve_path=True,
                                          dir_okay=True, file_okay=True))
@click.option('--yes', '-y', 'bypass', is_flag=True, default=False,
              help='Bypass permission prompts while ingesting')
def generate(ctx, srcdir, bypass=False):
    """Generate UV index"""

    bypass_ = bypass

    if not bypass_:
        q = 'This command will erase and rebuild the UV index. Are you sure?'

        if click.confirm(q):
            bypass_ = True

    if bypass_:
        generate_uv_index(srcdir, False, None, None, bypass)


@click.command()
@click.pass_context
@click.argument('srcdir', type=click.Path(exists=True, resolve_path=True,
                                          dir_okay=True, file_okay=True))
@click.option('--yes', '-y', 'bypass', is_flag=True, default=False,
              help='Bypass permission prompts while ingesting')
@click.option('--start-year', '-sy', 'start_year', default=None,
              help='lower bound of date range inclusive')
@click.option('--end-year', '-ey', 'end_year', default=None,
              help='upper bound of date range inclusive')
def update(ctx, srcdir, start_year, end_year, bypass=False):
    """Only generate UV index within date range"""

    generate_uv_index(srcdir, True, start_year, end_year, bypass)


uv_index.add_command(generate)
uv_index.add_command(update)
