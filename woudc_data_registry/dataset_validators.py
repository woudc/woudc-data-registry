
import logging


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

    if dataset in DATASETS:
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

    def check_all(self, extcsv):
        """
        Assess any dataset-specific tables inside <extcsv> for errors.
        Returns True iff no errors were encountered.

        :param extcsv: A parsed Extended CSV file of the appropriate dataset.
        :returns: True iff the file's dataset-specific tables are error-free.
        """

        return True
