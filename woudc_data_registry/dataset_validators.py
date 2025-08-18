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
# Copyright (c) 2024 Government of Canada
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

import logging
import statistics

from collections import OrderedDict


LOGGER = logging.getLogger(__name__)

DATASETS = ['Broad-band', 'Lidar', 'Multi-band', 'OzoneSonde', 'RocketSonde',
            'Spectral', 'TotalOzone', 'TotalOzoneObs', 'UmkehrN14_1.0',
            'UmkehrN14_2.0']


def get_validator(dataset, reporter):
    """
    Returns a DatasetValidator instance tied to <dataset>.
    If <dataset> is a valid data category but no special validator exists
    for it, returns a base validator that automatically succeeds.

    :param dataset: Name of a WOUDC data type.
    :param reporter: `ReportWriter` instance for error handling and logging.
    :returns: Validator class targetted to that data type.
    """

    if dataset == 'TotalOzone':
        return TotalOzoneValidator(reporter)
    elif dataset == 'TotalOzoneObs':
        return TotalOzoneObsValidator(reporter)
    elif dataset == 'Spectral':
        return SpectralValidator(reporter)
    elif dataset == 'UmkehrN14':
        return UmkehrValidator(reporter)
    elif dataset == 'Lidar':
        return LidarValidator(reporter)
    elif dataset in DATASETS:
        return DatasetValidator(reporter)
    else:
        raise ValueError(f'Invalid dataset {dataset}')


class DatasetValidator(object):
    """
    Superclass for Extended CSV validators of dataset-specific tables.
    Contains no checks of its own, so all files successfully validate.

    Is directly useful (without subclassing) for datasets that have no
    errors tied to their tables, and so never have dataset-specific errors.
    """

    def __init__(self, reporter):
        self.reports = reporter

        self.errors = []
        self.warnings = []

    def _add_to_report(self, error_code, line=None, **kwargs):
        """
        Submit a warning or error of code <error_code> to the report generator,
        with was found at line <line> in the input file. Uses keyword arguments
        to detail the warning/error message.

        Returns False iff the error is serious enough to abort parsing.
        """

        message, severe = self.reports.add_message(error_code, line, **kwargs)
        if severe:
            LOGGER.error(message)
            self.errors.append(message)
        else:
            LOGGER.warning(message)
            self.warnings.append(message)

        return not severe

    def check_all(self, extcsv):
        """
        Assess any dataset-specific tables inside <extcsv> for errors.
        Returns True iff no errors were encountered.

        :param extcsv: A parsed Extended CSV file of the appropriate dataset.
        :returns: `bool` of whether the file's dataset-specific tables
                  are error-free.
        """

        return True


class TotalOzoneValidator(DatasetValidator):
    """
    Dataset-specific validator for TotalOzone files.
    """

    def __init__(self, reporter):
        super(TotalOzoneValidator, self).__init__(reporter)

    def check_all(self, extcsv):
        """
        Assess any dataset-specific tables inside <extcsv> for errors.
        Returns True iff no errors were encountered.

        TotalOzone errors include improper formatting and ordering of
        dates in the #DAILY, #MONTHLY, and both #TIMESTAMP tables, and
        inconsistencies between #MONTHLY and the data it is derived from.

        :param extcsv: A parsed Extended CSV file of TotalOzone data.
        :returns: `bool` of whether the file's dataset-specific tables
                  are error-free.
        """

        LOGGER.info('Beginning TotalOzone-specific checks')

        time_series_ok = self.check_time_series(extcsv)
        timestamps_ok = self.check_timestamps(extcsv)
        monthly_ok = self.check_monthly(extcsv)

        LOGGER.info('TotalOzone-specific checks complete')
        return all([timestamps_ok, time_series_ok, monthly_ok])

    def check_time_series(self, extcsv):
        """
        Assess the ordering of Dates in the #DAILY table in <extcsv>.
        Returns True iff no errors were found.

        :param extcsv: A parsed Extended CSV file of TotalOzone data.
        :returns: `bool` of whether the ordering of #DAILY Dates is error-free.
        """

        LOGGER.debug('Assessing order of #DAILY.Date column')
        success = True

        timestamp1_date = extcsv.extcsv['TIMESTAMP']['Date']
        daily_startline = extcsv.line_num('DAILY') + 2

        dates_encountered = {}
        rows_to_remove = []

        extcsv.extcsv['DAILY'].pop('comments')
        daily_columns = zip(*extcsv.extcsv['DAILY'].values())

        is_string = False

        in_order = True
        prev_date = None
        for index, row in enumerate(daily_columns):
            line_num = daily_startline + index
            daily_date = row[0]

            if daily_date.year != timestamp1_date.year:
                if not self._add_to_report(232, line_num):
                    success = False

            if prev_date and daily_date < prev_date:
                in_order = False
            prev_date = daily_date

            if daily_date not in dates_encountered:
                dates_encountered[daily_date] = row
            elif row == dates_encountered[daily_date]:
                if not self._add_to_report(233, line_num, date=daily_date):
                    success = False
                rows_to_remove.append(index)
            elif not self._add_to_report(234, line_num, date=daily_date):
                success = False

        rows_to_remove.reverse()
        dateList = extcsv.extcsv['DAILY']['Date']
        for date in dateList:
            if isinstance(date, (str, int)):
                is_string = True
                if not self._add_to_report(231, daily_startline):
                    success = False
                break

        if not is_string:
            for index in rows_to_remove:
                for column in extcsv.extcsv['DAILY'].values():
                    column.pop(index)

            if not in_order:
                if not self._add_to_report(231, daily_startline):
                    success = False

                sorted_dates = sorted(extcsv.extcsv['DAILY']['Date'])
                sorted_daily = [dates_encountered[date_]
                                for date_ in sorted_dates]

                for field_num, field in \
                        enumerate(extcsv.extcsv['DAILY'].keys()):
                    column = list(map(lambda row: row[field_num],
                                      sorted_daily))
                    extcsv.extcsv['DAILY'][field] = column

        return success

    def check_timestamps(self, extcsv):
        """
        Assess the two required #TIMESTAMP tables in <extcsv> for errors
        and inconsistencies. Returns True iff no errors were found.

        :param extcsv: A parsed Extended CSV file of TotalOzone data.
        :returns: `bool` of whether the two #TIMESTAMP tables are error-free.
        """

        LOGGER.debug('Assessing #TIMESTAMP tables for similarity')
        success = True

        timestamp1_date = extcsv.extcsv['TIMESTAMP']['Date']
        timestamp1_time = extcsv.extcsv['TIMESTAMP'].get('Time')
        daily_dates = extcsv.extcsv['DAILY']['Date']

        if not daily_dates:
            LOGGER.warning('No observation dates')
            return False

        timestamp1_startline = extcsv.line_num('TIMESTAMP')
        timestamp1_valueline = timestamp1_startline + 2

        if timestamp1_date != daily_dates[0]:
            if not self._add_to_report(235, timestamp1_valueline):
                success = False
            extcsv.extcsv['TIMESTAMP']['Date'] = daily_dates[0]

        timestamp_count = extcsv.table_count('TIMESTAMP')
        if timestamp_count == 1:
            if not self._add_to_report(238):
                success = False

            utcoffset = extcsv.extcsv['TIMESTAMP']['UTCOffset']
            final_date = daily_dates[-1]

            timestamp2 = OrderedDict([
                ('UTCOffset', utcoffset),
                ('Date', final_date),
                ('Time', timestamp1_time)
            ])
            extcsv.extcsv['TIMESTAMP_2'] = timestamp2

        timestamp2_date = extcsv.extcsv['TIMESTAMP_2']['Date']
        timestamp2_time = extcsv.extcsv['TIMESTAMP_2']['Time']

        timestamp2_startline = extcsv.line_num('TIMESTAMP_2')
        timestamp2_valueline = None if timestamp2_startline is None \
            else timestamp2_startline + 2

        if timestamp2_date != daily_dates[-1]:
            if not self._add_to_report(236, timestamp2_valueline):
                success = False
            extcsv.extcsv['TIMESTAMP_2']['Date'] = daily_dates[-1]

        if timestamp2_time != timestamp1_time:
            if not self._add_to_report(226, timestamp2_valueline):
                success = False

        if timestamp_count > 2:
            timestamp3_startline = extcsv.line_num('TIMESTAMP_3')
            if not self._add_to_report(237, timestamp3_startline):
                success = False

            for ind in range(3, timestamp_count + 1):
                table_name = 'TIMESTAMP_' + str(ind)
                extcsv.remove_table(table_name)

        return success

    def check_monthly(self, extcsv):
        """
        Assess the correctness of the #MONTHLY table in <extcsv> in
        comparison with #DAILY. Returns True iff no errors were found.

        :param extcsv: A parsed Extended CSV file of TotalOzone data.
        :returns: `bool` of whether the #MONTHLY table is error-free.
        """

        LOGGER.debug('Assessing correctness of #MONTHLY table')
        success = True

        try:
            template_monthly = self.derive_monthly_from_daily(extcsv)
        except Exception as err:
            LOGGER.error(err)
            return False

        if 'MONTHLY' not in extcsv.extcsv:
            if not self._add_to_report(239):
                success = False
        else:
            present_monthly = extcsv.extcsv['MONTHLY']
            monthly_startline = extcsv.line_num('MONTHLY')
            monthly_valueline = monthly_startline + 2

            for field, derived_val in template_monthly.items():
                if field not in present_monthly:
                    if not self._add_to_report(240, monthly_valueline,
                                               field=field):
                        success = False
                elif present_monthly[field] != template_monthly[field]:
                    if not self._add_to_report(241, monthly_valueline,
                                               field=field):
                        success = False

        extcsv.extcsv['MONTHLY'] = template_monthly
        return success

    def derive_monthly_from_daily(self, extcsv):
        """
        Attempts to make a #MONTHLY table from the data found in #DAILY,
        and returns it as an OrderedDict if successful.

        If an error is encountered it is reported to the processing logs
        before an exception is raised.

        :param extcsv: A parsed Extended CSV file of TotalOzone data.
        :returns: An OrderedDict representing the derived #MONTHLY table.
        """

        LOGGER.debug('Regenerating #MONTHLY table from data')

        dates_column = extcsv.extcsv['DAILY']['Date']
        ozone_column = extcsv.extcsv['DAILY'].get('ColumnO3')

        daily_fieldline = extcsv.line_num('DAILY') + 1
        daily_valueline = daily_fieldline + 1

        if not ozone_column:
            self._add_to_report(121, daily_fieldline)
            msg = 'Cannot derive #MONTHLY table: #DAILY.ColumnO3 missing'
            raise Exception(msg)

        ozone_column = list(filter(bool, ozone_column))
        if len(ozone_column) == 0:
            self._add_to_report(230, daily_valueline)
            msg = 'Cannot derive #MONTHLY table: no ozone data in #DAILY'
            raise Exception(msg)

        first_date = dates_column[0]
        mean_ozone = round(statistics.mean(ozone_column), 1)
        stddev_ozone = 0 if len(ozone_column) == 1 \
            else round(statistics.stdev(ozone_column), 1)
        ozone_npts = len(ozone_column)

        monthly = OrderedDict([
            ('Date', first_date),
            ('ColumnO3', mean_ozone),
            ('StdDevO3', stddev_ozone),
            ('Npts', ozone_npts)
        ])
        return monthly


class TotalOzoneObsValidator(DatasetValidator):
    """
    Dataset-specific validator for TotalOzoneObs files.
    """

    def __init__(self, reporter):
        super(TotalOzoneObsValidator, self).__init__(reporter)

    def check_all(self, extcsv):
        """
        Assess any dataset-specific tables inside <extcsv> for errors.
        Returns True iff no errors were encountered.

        TotalOzoneObs errors include improper ordering of times in the
        #OBSERVATIONS tables.

        :param extcsv: A parsed Extended CSV file of TotalOzoneObs data.
        :returns: `bool` of whether the file's dataset-specific tables
                  are error-free.
        """

        LOGGER.info('Beginning TotalOzoneObs-specific checks')

        time_series_ok = self.check_time_series(extcsv)

        LOGGER.info('TotalOzoneObs-specific checks complete')
        return time_series_ok

    def check_time_series(self, extcsv):
        """
        Assess the ordering of Times in the #OBSERVATIONS table in <extcsv>.
        Returns True iff no errors were found.

        :param extcsv: A parsed Extended CSV file of TotalOzoneObs data.
        :returns: `bool` of whether the ordering of #OBSERVATIONS.Times
                  is error-free.
        """

        LOGGER.debug('Assessing order of #OBSERVATIONS.Time column')
        success = True

        extcsv.extcsv['OBSERVATIONS'].pop('comments')
        observations = zip(*extcsv.extcsv['OBSERVATIONS'].values())
        observations_valueline = extcsv.line_num('OBSERVATIONS') + 2

        times_encountered = {}
        rows_to_remove = []

        in_order = True
        prev_time = None
        for index, row in enumerate(observations):
            line_num = observations_valueline + index
            time = row[0]

            if isinstance(prev_time, (str, int, type(None))):
                pass
            elif isinstance(time, (str, int, type(None))):
                success = False
                return success
            else:
                if prev_time and time < prev_time:
                    in_order = False
            prev_time = time

            if time not in times_encountered:
                times_encountered[time] = row
            elif row == times_encountered[time]:
                if not self._add_to_report(243, line_num, time=time):
                    success = False
                rows_to_remove.append(index)
            elif not self._add_to_report(244, line_num, time=time):
                success = False

        rows_to_remove.reverse()
        for index in rows_to_remove:
            for column in extcsv.extcsv['OBSERVATIONS'].values():
                column.pop(index)

        if not in_order:
            if not self._add_to_report(242, observations_valueline):
                success = False

            sorted_times = sorted(extcsv.extcsv['OBSERVATIONS']['Time'])
            sorted_rows = [times_encountered[time] for time in sorted_times]

            for field_num, field in enumerate(extcsv.extcsv['OBSERVATIONS']):
                column = list(map(lambda row: row[field_num], sorted_rows))
                extcsv.extcsv['OBSERVATIONS'][field] = column

        return success


class SpectralValidator(DatasetValidator):
    """
    Dataset-specific validator for Spectral files.
    """

    def __init__(self, reporter):
        super(SpectralValidator, self).__init__(reporter)

    def check_all(self, extcsv):
        """
        Assess any dataset-specific tables inside <extcsv> for errors.
        Returns True iff no errors were encountered.

        Spectral errors include incorrect groupings of #TIMESTAMP, #GLOBAL,
        and #GLOBAL_SUMMARY tables such that the counts of each are different.

        :param extcsv: A parsed Extended CSV file of Spectral data.
        :returns: `bool` of whether the file's dataset-specific tables
                  are error-free.
        """

        LOGGER.info('Beginning Spectral-specific checks')

        groupings_ok = self.check_groupings(extcsv)

        LOGGER.info('Spectral-specific checks complete')
        return groupings_ok

    def check_groupings(self, extcsv):
        """
        Assess the numbers of #TIMESTAMP, #GLOBAL, and #GLOBAL_SUMMARY tables
        in the input file <extcsv>. Returns True iff no errors were found.

        :param extcsv: A parsed Extended CSV file of Spectral data.
        :returns: `bool` of whether the file is free of table grouping errors.
        """

        LOGGER.debug('Assessing #TIMESTAMP, #GLOBAL, #GLOBAL_SUMMARY'
                     ' table counts')
        success = True

        summary_table = 'GLOBAL_SUMMARY_NSF' \
            if 'GLOBAL_SUMMARY_NSF' in extcsv.extcsv \
            else 'GLOBAL_SUMMARY'

        timestamp_count = extcsv.table_count('TIMESTAMP')
        global_count = extcsv.table_count('GLOBAL')
        summary_count = extcsv.table_count(summary_table)

        if not timestamp_count == global_count == summary_count:
            if not self._add_to_report(123, summary_table=summary_table):
                success = False

        return success


class LidarValidator(DatasetValidator):
    """
    Dataset-specific validator for Lidar files.
    """

    def __init__(self, reporter):
        super(LidarValidator, self).__init__(reporter)

    def check_all(self, extcsv):
        """
        Assess any dataset-specific tables inside <extcsv> for errors.
        Returns True iff no errors were encountered.

        Lidar errors include incorrect groupings of #OZONE_PROFILE and
        and #OZONE_SUMMARY tables such that the counts of each are different.

        :param extcsv: A parsed Extended CSV file of Lidar data.
        :returns: `bool` of whether the file's dataset-specific tables
                  are error-free.
        """

        LOGGER.info('Beginning Lidar-specific checks')

        groupings_ok = self.check_groupings(extcsv)

        LOGGER.info('Lidar-specific checks complete')
        return groupings_ok

    def check_groupings(self, extcsv):
        """
        Assess the numbers of #OZONE_PROFILE and #OZONE_SUMMARY tables
        in the input file <extcsv>. Returns True iff no errors were found.

        :param extcsv: A parsed Extended CSV file of Lidar data.
        :returns: `bool` of whether the file is free of table grouping errors.
        """

        LOGGER.debug('Assessing #OZONE_PROFILE, #GLOBAL_SUMMARY table counts')
        success = True

        profile_count = extcsv.table_count('OZONE_PROFILE')
        summary_count = extcsv.table_count('OZONE_SUMMARY')

        if profile_count != summary_count:
            if not self._add_to_report(122):
                success = False

        return success


class UmkehrValidator(DatasetValidator):
    """
    Dataset-specific validator for Umkehr files.
    """

    def __init__(self, reporter):
        super(UmkehrValidator, self).__init__(reporter)

    def check_all(self, extcsv):
        """
        Assess any dataset-specific tables inside <extcsv> for errors.
        Returns True iff no errors were encountered.

        Umkehr errors include inconsistencies between the two #TIMESTAMP
        tables and improper ordering of dates within their data tables.

        :param extcsv: A parsed Extended CSV file of Umkehr data.
        :returns: `bool` of whether the file's dataset-specific tables
                  are error-free.
        """

        LOGGER.info('Beginning Umkehr-specific checks')

        time_series_ok = self.check_time_series(extcsv)
        timestamps_ok = self.check_timestamps(extcsv)

        LOGGER.info('Umkehr-specific checks complete')
        return timestamps_ok and time_series_ok

    def check_time_series(self, extcsv):
        """
        Assess the ordering of dates in the data table (#N14_VALUES or
        #C_PROFILE) in <extcsv>. Returns True iff no errors were found.

        :param extcsv: A parsed Extended CSV file of Umkehr data.
        :returns: `bool` of whether the ordering of observation dates
                  is error-free.
        """

        level = extcsv.extcsv['CONTENT']['Level']
        data_table = 'N14_VALUES' if level == 1.0 else 'C_PROFILE'

        LOGGER.debug(f'Assessing order of #{data_table}.Date column')
        success = True

        data_table_valueline = extcsv.line_num(data_table) + 2
        dates_encountered = {}
        rows_to_remove = []

        extcsv.extcsv[data_table].pop('comments')
        columns = zip(*extcsv.extcsv[data_table].values())

        in_order = True
        prev_date = None
        for index, row in enumerate(columns):
            line_num = data_table_valueline + index
            observation_date = row[0]

            if prev_date and observation_date < prev_date:
                in_order = False
            prev_date = observation_date

            if observation_date not in dates_encountered:
                dates_encountered[observation_date] = row
            elif row == dates_encountered[observation_date]:
                if not self._add_to_report(251, line_num, table=data_table,
                                           date=observation_date):
                    success = False
                rows_to_remove.append(index)
            elif not self._add_to_report(246, line_num, table=data_table,
                                         date=observation_date):
                success = False

        rows_to_remove.reverse()
        for index in rows_to_remove:
            for column in extcsv.extcsv[data_table].values():
                column.pop(index)

        if not in_order:
            if not self._add_to_report(245, data_table_valueline,
                                       table=data_table):
                success = False

            sorted_dates = sorted(extcsv.extcsv[data_table]['Date'])
            sorted_rows = [dates_encountered[date_] for date_ in sorted_dates]

            for fieldnum, field in enumerate(extcsv.extcsv[data_table].keys()):
                column = list(map(lambda row: row[fieldnum], sorted_rows))
                extcsv.extcsv[data_table][field] = column

        return success

    def check_timestamps(self, extcsv):
        """
        Assess the two required #TIMESTAMP tables in <extcsv> for errors
        and inconsistencies. Returns True iff no errors were found.

        :param extcsv: A parsed Extended CSV file of Umkehr data.
        :returns: `bool` of whether the two #TIMESTAMP tables are error-free.
        """

        LOGGER.debug('Assessing #TIMESTAMP tables for similarity')
        success = True

        level = extcsv.extcsv['CONTENT']['Level']
        data_table = 'N14_VALUES' if level == 1.0 else 'C_PROFILE'

        timestamp1_date = extcsv.extcsv['TIMESTAMP']['Date']
        timestamp1_time = extcsv.extcsv['TIMESTAMP'].get('Time')
        observation_dates = extcsv.extcsv[data_table]['Date']

        if not observation_dates:
            LOGGER.warning('No observation dates')
            return False

        timestamp1_startline = extcsv.line_num('TIMESTAMP')
        timestamp1_valueline = timestamp1_startline + 2

        if timestamp1_date != observation_dates[0]:
            if not self._add_to_report(247, timestamp1_valueline,
                                       table=data_table):
                success = False
            extcsv.extcsv['TIMESTAMP']['Date'] = observation_dates[0]

        timestamp_count = extcsv.table_count('TIMESTAMP')
        if timestamp_count == 1:
            if not self._add_to_report(249, table=data_table):
                success = False

            utcoffset = extcsv.extcsv['TIMESTAMP']['UTCOffset']
            final_date = observation_dates[-1]

            timestamp2 = OrderedDict([
                ('UTCOffset', utcoffset),
                ('Date', final_date),
                ('Time', timestamp1_time)
            ])
            extcsv.extcsv['TIMESTAMP_2'] = timestamp2

        timestamp2_date = extcsv.extcsv['TIMESTAMP_2']['Date']
        timestamp2_time = extcsv.extcsv['TIMESTAMP_2']['Time']

        timestamp2_startline = extcsv.line_num('TIMESTAMP_2')
        timestamp2_valueline = None if timestamp2_startline is None \
            else timestamp2_startline + 2

        if timestamp2_date != observation_dates[-1]:
            if not self._add_to_report(248, timestamp2_valueline,
                                       table=data_table):
                success = False

            extcsv.extcsv['TIMESTAMP_2']['Date'] = observation_dates[-1]

        if timestamp2_time != timestamp1_time:
            if not self._add_to_report(226, timestamp2_valueline):
                success = False

        if timestamp_count > 2:
            timestamp3_startline = extcsv.line_num('TIMESTAMP_3')
            if not self._add_to_report(237, timestamp3_startline):
                success = False

            for ind in range(3, timestamp_count + 1):
                table_name = 'TIMESTAMP_' + str(ind)
                extcsv.remove_table(table_name)

        return success
