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

from datetime import date
import logging

from woudc_data_registry import registry, search
from woudc_data_registry.models import (Contributor, Country,
                                        Station, StationName)
from woudc_data_registry.util import is_plural

LOGGER = logging.getLogger(__name__)
REGISTRY = registry.Registry()
SEARCH_INDEX = search.SearchIndex()


def get_metadata(entity, identifier=None):
    """
    Get metadata objects

    :param entity: metadata entity
    :param identifier: identifier filter (default no filter)
    :returns: `list` of all matching objects
    """

    LOGGER.debug(f'Querying metadata objects {entity}')
    prop = getattr(entity, entity.id_field)
    if identifier is None:
        res = REGISTRY.session.query(entity).order_by(prop)
    else:
        LOGGER.debug(f'Quering identifier {identifier}')
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
    LOGGER.debug(f'Found {count} {term}')

    return res


def add_metadata(entity, dict_, save_to_registry=True, save_to_index=True):
    """
    Add a metadata object

    :param entity: A model class.
    :param dict_: Dictionary of model data to initialize the object.
    :param save_to_registry: `bool` of whether to load object
                             to the data registry.
    :param save_to_index: `bool` of whether to load object to the search index.
    :returns: The model object created.
    """

    if 'country_id' in dict_:
        LOGGER.debug('Querying for matching country')
        results = REGISTRY.session.query(Country).filter(
            Country.name_en == dict_['country_id'])

        if results.count() == 0:
            msg = f"Invalid country: {dict_['country']}"
            LOGGER.error(msg)
            raise ValueError(msg)

        dict_['country_id'] = getattr(results[0], Country.id_field)

    if 'contributor' in dict_:
        LOGGER.debug('Querying for matching contributor')
        results = REGISTRY.session.query(Contributor).filter(
            Contributor.contributor_id == f"{dict_['contributor']}:{dict_['project']}") # noqa

        if results.count() == 0:
            msg = (
                "Invalid contributor_id: "
                f"{dict_['contributor']}:{dict_['project']}"
            )
            LOGGER.error(msg)
            raise ValueError(msg)

    if 'station_name' in dict_ and 'station_id' in dict_:
        station_id, name = dict_['station_id'], dict_['station_name']
        name_id = ':'.join([station_id, name])

        if not get_metadata(StationName, name_id):
            add_metadata(StationName, {
                'station_id': station_id,
                'name': name,
                'first_seen': date.today()
            }, save_to_index=False)

    c = entity(dict_)
    if save_to_registry:
        REGISTRY.save(c)
    if save_to_index:
        SEARCH_INDEX.index(entity, c.__geo_interface__)

    return c


def update_metadata(entity, identifier, dict_,
                    save_to_registry=True, save_to_index=True):
    """
    Update metadata object

    :param entity: A model class.
    :param identifier: Identifier of target object.
    :param dict_: Dictionary of model data to initialize the object.
    :param save_to_registry: `bool` of whether to update object in
                             the data registry.
    :param save_to_index: `bool` of whether to update object in
                          the search index.
    :returns: `bool` of whether the operation was successful.
    """

    records = get_metadata(entity, identifier)

    if len(records) == 0:
        msg = f'identifier {identifier} not found'
        LOGGER.warning(msg)
        raise ValueError(msg)
    else:
        LOGGER.debug(f'Updating metadata entity {entity}, identifier {identifier}')  # noqa
        obj = records[0]

        if 'station_name' in dict_ and 'station_id' in dict_:
            station_id, name = dict_['station_id'], dict_['station_name']
            name_id = ':'.join([station_id, name])

            if not get_metadata(StationName, name_id):
                add_metadata(StationName, {
                    'station_id': station_id,
                    'name': name,
                    'first_seen': date.today()
                }, save_to_index=False)

            del dict_['station_name']
            dict_['station_name_id'] = name_id

        for field, value in dict_.items():
            setattr(obj, field, value)

        try:
            obj.generate_ids()
        except Exception as err:
            LOGGER.warning(f'Unable to generate IDS: {err}')

        if save_to_index and getattr(obj, entity.id_field) != identifier:
            SEARCH_INDEX.unindex(entity, identifier)

        if save_to_registry:
            REGISTRY.save(obj)
        if save_to_index:
            SEARCH_INDEX.index(entity, obj.__geo_interface__)
        return True


def delete_metadata(entity, identifier,
                    save_to_registry=True, save_to_index=True):
    """
    Delete metadata object

    :param entity: A model class.
    :param identifier: Data registry identifier of target object.
    :param save_to_registry: `bool` of whether changes should apply
                             to the data registry.
    :param save_to_index: `bool` of whether changes should apply
                          to the search_index.
    :returns: `bool` of whether the operation was successful.
    """

    LOGGER.debug(f'Updating metadata entity {entity}, identifier {identifier}')  # noqa

    prop = getattr(entity, entity.id_field)
    REGISTRY.session.query(entity).filter(prop == identifier).delete()

    if save_to_registry and entity == Station:
        REGISTRY.session.query(StationName) \
                        .filter(StationName.station_id == identifier) \
                        .delete()

    if save_to_registry:
        REGISTRY.session.commit()
    if save_to_index:
        SEARCH_INDEX.unindex(entity, identifier)

    return True
