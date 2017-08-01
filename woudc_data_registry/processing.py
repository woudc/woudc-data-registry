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
# Copyright (c) 2017 Government of Canada
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

from datetime import datetime
import logging

from woudc_data_registry import registry
from woudc_data_registry.parser import (ExtendedCSV, MetadataValidationError,
                                        NonStandardDataError)
from woudc_data_registry.util import is_text_file, read_file

LOGGER = logging.getLogger(__name__)


class Process(object):
    """
    Generic processing definition

    - detect
    - parse
    - validate
    - verify
    - register
    - index
    """

    def __init__(self):
        """constructor"""

        self.status = None
        self.code = None
        self.message = None
        self.process_start = datetime.utcnow()
        self.process_end = None

    def process_data(self, infile, verify=False):
        """process incoming data record"""

        # detect incoming data file

        data_record = None

        LOGGER.info('Detecting file')
        if not is_text_file(infile):
            self.status = 'failed'
            self.code = 'NonStandardDataError'
            self.message = 'binary file detected'
            LOGGER.error('Unknown file: {}'.format(self.message))
            return False

        try:
            data_record = read_file(infile)
        except UnicodeDecodeError as err:
            self.status = 'failed'
            self.code = 'NonStandardDataError'
            self.message = err
            LOGGER.error('Unknown file: {}'.format(err))
            return False

        LOGGER.info('Parsing data record')
        dr = ExtendedCSV(data_record)

        try:
            LOGGER.info('Validating Extended CSV')
            dr.validate_metadata()
            LOGGER.info('Valid Extended CSV')
        except NonStandardDataError as err:
            self.status = 'failed'
            self.code = 'NonStandardDataError'
            self.message = err
            LOGGER.error('Invalid Extended CSV: {}'.format(err))
            return False
        except MetadataValidationError as err:
            self.status = 'failed'
            self.code = 'MetadataValidationError'
            self.message = err
            LOGGER.error('Invalid Extended CSV: {}'.format(err.errors))
            return False

        LOGGER.info('Verifying data record against registry')
        # verify:
        # - Extended CSV core fields against registry
        # - taxonomy/URI check
        # - duplicate data submitted
        # - new version of file

        if not verify:
            LOGGER.info('Saving Extended CSV to registry')
            self.process_end = datetime.utcnow()
            registry.save_data_record(dr)

        return True

    def index_data(self):
        """add data record to search index"""

        raise NotImplementedError()

    def unindex_data(self):
        """remove data record from search index"""

        raise NotImplementedError()
