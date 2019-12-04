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

import logging

from woudc_data_registry import registry
from woudc_data_registry.models import Contributor, Country
from woudc_data_registry.util import is_plural

LOGGER = logging.getLogger(__name__)
REGISTRY = registry.Registry()


def get_metadata(entity, identifier=None):
    """
    Get metadata objects

    :param entity: metadata entity
    :param identifier: identifier filter (default no filter)

    :returns: `list` of all matching objects
    """

    LOGGER.debug('Querying metadata objects {}'.format(entity))
    prop = getattr(entity, entity.id_field)
    if identifier is None:
        res = REGISTRY.session.query(entity).order_by(prop)
    else:
        LOGGER.debug('Quering identifier {}'.format(identifier))
        res = REGISTRY.session.query(entity).filter(
            prop == identifier).all()

    if isinstance(res, list):
        count = len(res)
    else:
        count = res.count()

    if is_plural(count):
        term = 'results'
    else:
        term = 'result'
    LOGGER.debug('Found {} {}'.format(count, term))

    return res


def add_metadata(entity, dict_):
    """
    Add a metadata object

    :param entity: metadata entity
    :param dict_: `dict` of properties to update

    :returns: `bool` of status/result
    """

    if 'country_id' in dict_:
        LOGGER.debug('Querying for matching country')
        results = REGISTRY.session.query(Country).filter(
            Country.country_name == dict_['country_id'])

        if results.count() == 0:
            msg = 'Invalid country: {}'.format(dict_['country_id'])
            LOGGER.error(msg)
            raise ValueError(msg)

        dict_['country_id'] = getattr(results[0], Country.id_field)

    if 'contributor_id' in dict_:
        LOGGER.debug('Querying for matching contributor')
        results = REGISTRY.session.query(Contributor).filter(
            Contributor.contributor_id == dict_['contributor_id'])

        if results.count() == 0:
            msg = 'Invalid contributor: {}'.format(dict_['contributor_id'])
            LOGGER.error(msg)
            raise ValueError(msg)

        dict_['contributor_id'] = getattr(results[0], Contributor.id_field)

    c = entity(dict_)
    REGISTRY.save(c)

    return True


def update_metadata(entity, identifier, dict_):
    """
    Update metadata object

    :param entity: metadata entity
    :param identifier: identifier filter (default no filter)
    :param dict_: `dict` of properties to update

    :returns: `bool` of status/result
    """

    LOGGER.debug('Updating metadata entity {}, identifier {}'.format(
        entity, identifier))
    prop = getattr(entity, entity.id_field)
    r = REGISTRY.session.query(entity).filter(
        prop == identifier).update(dict_)

    if r == 0:
        msg = 'identifier {} not found'.format(identifier)
        LOGGER.warning(msg)
        raise ValueError(msg)

    REGISTRY.save()


def delete_metadata(entity, identifier):
    """
    Delete metadata object

    :param entity: metadata entity
    :param identifier: identifier filter

    :returns: `bool` of status/result
    """

    LOGGER.debug('Updating metadata entity {}, identifier {}'.format(
        entity, identifier))
    prop = getattr(entity, entity.id_field)
    REGISTRY.session.query(entity).filter(prop == identifier).delete()

    REGISTRY.save()

    return True
