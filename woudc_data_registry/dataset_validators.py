
import logging
import statistics

from collections import OrderedDict


LOGGER = logging.getLogger(__name__)

DATASETS = ['Broad-band', 'Lidar', 'Multi-band', 'OzoneSonde', 'RocketSonde',
            'Spectral', 'TotalOzone', 'TotalOzoneObs', 'UmkehrN14']


def get_validator(dataset):
    """
    Returns a DatasetValidator instance tied to <dataset>.
    If <dataset> is a valid data category but no special validator exists
    for it, returns a base validator that automatically succeeds.

    :param dataset: Name of a WOUDC data type.
    :returns: Validator class targetted to that data type.
    """

    if dataset == 'TotalOzone':
        return TotalOzoneValidator()
    elif dataset == 'Spectral':
        return SpectralValidator()
    elif dataset in DATASETS:
        return DatasetValidator()
    else:
        raise ValueError('Invalid dataset {}'.format(dataset))


class DatasetValidator:
    """
    Superclass for Extended CSV validators of dataset-specific tables.
    Contains no checks of its own, so all files successfully validate.

    Is directly useful (without subclassing) for datasets that have no
    errors tied to their tables, and so never have dataset-specific errors.
    """

    def __init__(self):
        self.errors = []
        self.warnings = []

    def _warning(self, error_code, line, message=None):
        """
        Record <message> as an error with code <error_code> that took place
        at line <line> in the input file.
        """

        LOGGER.warning(message)
        self.warnings.append((error_code, message, line))

    def _error(self, error_code, line, message=None):
        """
        Record <message> as an error with code <error_code> that took place
        at line <line> in the input file.
        """

        LOGGER.error(message)
        self.errors.append((error_code, message, line))

    def check_all(self, extcsv):
        """
        Assess any dataset-specific tables inside <extcsv> for errors.
        Returns True iff no errors were encountered.

        :param extcsv: A parsed Extended CSV file of the appropriate dataset.
        :returns: True iff the file's dataset-specific tables are error-free.
        """

        return True


class TotalOzoneValidator(DatasetValidator):
    """
    Dataset-specific validator for TotalOzone files.
    """

    def check_all(self, extcsv):
        """
        Assess any dataset-specific tables inside <extcsv> for errors.
        Returns True iff no errors were encountered.

        TotalOzone errors include improper formatting and ordering of
        dates in the #DAILY, #MONTHLY, and both #TIMESTAMP tables, and
        inconsistencies between #MONTHLY and the data it is derived from.

        :param extcsv: A parsed Extended CSV file of TotalOzone data.
        :returns: True iff the file's dataset-specific tables are error-free.
        """

        LOGGER.info('Beginning TotalOzone-specific checks')

        timestamps_ok = self.check_timestamps(extcsv)
        time_series_ok = self.check_time_series(extcsv)
        monthly_ok = self.check_monthly(extcsv)

        LOGGER.info('TotalOzone-specific checks complete')
        return all([timestamps_ok, time_series_ok, monthly_ok])

    def check_timestamps(self, extcsv):
        """
        Assess the two required #TIMESTAMP tables in <extcsv> for errors
        and inconsistencies. Returns True iff no errors were found.

        :param extcsv: A parsed Extended CSV file of TotalOzone data.
        :returns: True iff the two #TIMESTAMP tables are error-free.
        """

        LOGGER.debug('Assessing #TIMESTAMP tables for similarity')

        timestamp1_date = extcsv.extcsv['TIMESTAMP']['Date']
        timestamp1_time = extcsv.extcsv['TIMESTAMP'].get('Time', None)
        daily_dates = extcsv.extcsv['DAILY']['Date']

        timestamp1_start = extcsv.line_num('TIMESTAMP')
        timestamp1_values = timestamp1_start + 2

        if timestamp1_date != daily_dates[0]:
            msg = '#TIMESTAMP.Date before #DAILY does not equal' \
                  ' first date of #DAILY'
            self._warning(114, timestamp1_values, msg)

            extcsv.extcsv['TIMESTAMP']['Date'] = daily_dates[0]

        timestamp_count = extcsv.table_count('TIMESTAMP')
        if timestamp_count == 1:
            msg = '#TIMESTAMP table after #DAILY is missing,' \
                  ' deriving based on requirements'
            self._warning(117, None, msg)

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

        timestamp2_start = extcsv.line_num('TIMESTAMP_2')
        timestamp2_values = None if timestamp2_start is None \
            else timestamp2_start + 2

        if timestamp2_date != daily_dates[-1]:
            msg = '#TIMESTAMP.Date after #DAILY does not equal' \
                  ' last date of #DAILY'
            self._warning(115, timestamp2_values, msg)

            extcsv.extcsv['TIMESTAMP_2']['Date'] = daily_dates[-1]

        if timestamp2_time != timestamp1_time:
            msg = 'Inconsistent Time values between #TIMESTAMP tables'
            self._warning(118, timestamp2_values, msg)

        if timestamp_count > 2:
            msg = 'More than 2 #TIMESTAMP tables present; removing extras'
            line = extcsv.line_num('TIMESTAMP_3')
            self._warning(116, line, msg)

            for ind in range(3, timestamp_count + 1):
                table_name = 'TIMESTAMP_' + str(ind)
                extcsv.remove_table(table_name)

        return True

    def check_time_series(self, extcsv):
        """
        Assess the ordering of Dates in the #DAILY table in <extcsv>.
        Returns True iff no errors were found.

        :param extcsv: A parsed Extended CSV file of TotalOzone data.
        :returns: True iff the ordering of #DAILY Dates is error-free.
        """

        LOGGER.debug('Assessing order of #DAILY.Date column')

        timestamp1_date = extcsv.extcsv['TIMESTAMP']['Date']
        daily_start = extcsv.line_num('DAILY') + 2
        dates_encountered = {}

        daily_columns = zip(*extcsv.extcsv['DAILY'].values())
        sequence_ok = True

        in_order = True
        prev_date = None
        for line_num, row in enumerate(daily_columns, daily_start):
            date = row[0]

            if date.year != timestamp1_date.year:
                msg = '#DAILY.Date has a different year than #TIMESTAMP.Date'
                self._warning(42, line_num, msg)

            if prev_date and date < prev_date:
                in_order = False

            if date not in dates_encountered:
                dates_encountered[date] = row
            elif row == dates_encountered[date]:
                msg = 'Duplicate data ignored with non-unique #DAILY.Date'
                self._warning(47, line_num, msg)
            else:
                msg = '#Found multiple observations under #DAILY.Date {}' \
                      .format(date)
                self._error(48, line_num, msg)
                sequence_ok = False

        if not sequence_ok:
            return False
        elif not in_order:
            msg = '#DAILY.Date found in non-chronological order'
            self._warning(49, daily_start, msg)

            sorted_dates = sorted(extcsv.extcsv['DAILY']['Date'])
            sorted_daily = [dates_encountered[date] for date in sorted_dates]

            for field_num, field in enumerate(extcsv.extcsv['DAILY'].keys()):
                column = list(map(lambda row: row[field_num], sorted_daily))
                extcsv.extcsv['DAILY'][field] = column

        return True

    def check_monthly(self, extcsv):
        """
        Assess the correctness of the #MONTHLY table in <extcsv> in
        comparison with #DAILY. Returns True iff no errors were found.

        :param extcsv: A parsed Extended CSV file of TotalOzone data.
        :returns: True iff the #MONTHLY table is error-free.
        """

        LOGGER.debug('Assessing correctness of #MONTHLY table')

        try:
            template_monthly = self.derive_monthly_from_daily(extcsv)
        except Exception as err:
            print(err)
            return False

        if 'MONTHLY' not in extcsv.extcsv:
            msg = 'Missing #MONTHLY table derived according to requirements'
            self._warning(119, None, msg)
        else:
            present_monthly = extcsv.extcsv['MONTHLY']
            start_line = extcsv.line_num('MONTHLY')
            value_line = start_line + 2

            for field, derived_val in template_monthly.items():
                if field not in present_monthly:
                    msg = 'Missing value for #MONTHLY.{} derived according' \
                          ' to requirements'.format(field)
                    self._warning(121, value_line, msg)
                elif present_monthly[field] != template_monthly[field]:
                    msg = '#MONTHLY.{} value differs from derived value:' \
                          ' correcting'.format(field)
                    self._warning(120, value_line, msg)

        extcsv.extcsv['MONTHLY'] = template_monthly
        return True

    def derive_monthly_from_daily(self, extcsv):
        """
        Attempts to make a #MONTHLY table from the data found in #DAILY,
        and returns it as an OrderedDict if successful.

        If an error is encountered it is reported to the processing logs
        before an exception is raised.

        :param extcsv: A parsed Extended CSV file of TotalOzone data.
        :returns: An OrderedDict representing the derived #MONTHLY table.
        """

        LOGGER.debug('Renerating #MONTHLY table from data')

        dates_column = extcsv.extcsv['DAILY']['Date']
        ozone_column = extcsv.extcsv['DAILY'].get('ColumnO3', None)

        daily_fieldline = extcsv.line_num('DAILY') + 1
        daily_valueline = daily_fieldline + 1

        if not ozone_column:
            msg = 'Cannot derive #MONTHLY table: #DAILY.ColumnO3 missing'
            self._error(1000, daily_fieldline, msg)
            raise Exception(msg)

        ozone_column = list(filter(bool, ozone_column))
        if len(ozone_column) == 0:
            msg = 'No ozone data in #DAILY table'
            self._error(1000, daily_valueline, msg)
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



class SpectralValidator(DatasetValidator):
    """
    Dataset-specific validator for Spectral files.
    """

    def check_all(self, extcsv):
        """
        Assess any dataset-specific tables inside <extcsv> for errors.
        Returns True iff no errors were encountered.

        Spectral errors include incorrect groupings of #TIMESTAMP, #GLOBAL,
        and #GLOBAL_SUMMARY tables such that the counts of each are different.

        :param extcsv: A parsed Extended CSV file of Spectral data.
        :returns: True iff the file's dataset-specific tables are error-free.
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
        :returns: True iff the file is free of table grouping errors.
        """

        LOGGER.debug('Assessing #TIMESTAMP, #GLOBAL, #GLOBAL_SUMMARY'
                     ' table counts')

        global_summary_table = 'GLOBAL_SUMMARY_NSF' \
            if 'GLOBAL_SUMMARY_NSF' in extcsv.extcsv \
            else 'GLOBAL_SUMMARY'

        timestamp_count = extcsv.table_count('TIMESTAMP')
        global_count = extcsv.table_count('GLOBAL')
        global_summary_count = extcsv.table_count(global_summary_table)

        if not timestamp_count == global_count == global_summary_count:
            msg = 'Required Spectral tables #TIMESTAMP, #GLOBAL, and #{}' \
                  ' have uneven counts {}, {}, and {}: must be even counts' \
                  ' of each'.format(global_summary_table, timestamp_count,
                                    global_count, global_summary_count)
            self._warning(147, None, msg)

        return True
