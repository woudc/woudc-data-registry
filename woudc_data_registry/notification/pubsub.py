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

import base64
import hashlib
import requests
import json

import ssl

import uuid

import paho.mqtt.publish as publish
import paho.mqtt.client as paho_mqtt


import datetime
import logging

from woudc_data_registry import config

from woudc_data_registry.models import DataRecord

from woudc_data_registry.registry import Registry

LOGGER = logging.getLogger(__name__)


def publish_notification(hours):
    """Publish a notification to the MQTT broker for all data records
    ingested in the last `hours` hours.

    Args:
        hours (int): The number of hours to look back for new data records.
    """
    today = datetime.datetime.now()
    date_ = today - datetime.timedelta(hours=hours)
    date_ = date_.replace(minute=0, second=0, microsecond=0)
    registry = Registry()
    ingested_records = registry.query_by_field_range(
        DataRecord, "published_datetime", date_, today)
    LOGGER.info(f"{len(ingested_records)} records found earlier than {date_}")
    url_template = 'https://woudc.org/archive/Archive-NewFormat'
    responses = {}
    no_message = []
    for record in ingested_records:
        instrument = record.instrument_id.split(':')[0].lower()
        year = record.timestamp_date.year
        dataset = (
            f'{record.content_category}_{record.content_level}_'
            f'{record.content_form}'
        )
        ingest_filepath = record.ingest_filepath
        url = (
            f'{url_template}/{dataset}/stn{record.station_id}/'
            f'{instrument}/{year}/{record.filename}'
        )

        LOGGER.info(f'Found {url}')
        http_reponse = get_HTTP_HEAD_response(url)
        if http_reponse == 200:
            query = registry.query_distinct_by_fields(
                DataRecord.ingest_filepath, DataRecord, {
                    "ingest_filepath": ingest_filepath})
            if len(query) == 1:
                message = 'new record'
            elif len(query) > 1:
                message = 'update record'
            responses[ingest_filepath] = {
                'record': record,
                'status_code': http_reponse,
                'message': message
            }
        else:
            no_message.append(ingest_filepath)
    LOGGER.debug(f'{len(responses)} records found.')
    LOGGER.debug(f'No message: {no_message}')
    notifications = generate_geojson_payload(responses)
    LOGGER.debug('geoJSON Generated.')
    publish_to_MQTT_Broker(notifications)


def get_HTTP_HEAD_response(url):
    """
    Get HTTP HEAD response

    :param url: URL to be evaluated
    :returns: `requests.Response` object
    """

    try:
        response = requests.head(url)
        response.raise_for_status()
        LOGGER.info(f"HTTP Headers: {response}")
        LOGGER.info(f"Status Code: {response.status_code}")
        return response.status_code
    except requests.exceptions.RequestException as e:
        LOGGER.error(f"An error occurred while making a request to {url}: {e}")
        return '404'


def generate_geojson_payload(info):
    """
    Generate GeoJSON payload

    :returns: `dict` of GeoJSON payload
    """
    notifications = []
    for key in info:
        with open(config.WDR_MQTT_NOTIFICATION_TEMPLATE_PATH, 'r') as file:
            geojson = json.load(file)
        x = info[key]["record"].x
        y = info[key]["record"].y
        z = info[key]["record"].z
        if None in (x, y):
            LOGGER.error('x or y is None')
            geojson["geometry"] = None
        else:
            geojson["geometry"]["coordinates"] = [x, y]
            if z is not None:
                geojson["geometry"]["coordinates"].append(z)

        geojson["properties"]["pubtime"] = (
            info[key]['record'].published_datetime.strftime(
                '%Y-%m-%dT%H:%M:%SZ'
            )
        )

        geojson["properties"]["datetime"] = (
            info[key]['record'].timestamp_utc.strftime(
                '%Y-%m-%dT%H:%M:%SZ'
            )
        )

        with open(info[key]['record'].output_filepath, 'rb') as f:
            file_data = f.read()
            sha256_digest = hashlib.sha512(file_data).digest()
            b64_md5_hash = base64.b64encode(
                sha256_digest
            ).decode()
            geojson["properties"]["integrity"]["value"] = b64_md5_hash

        geojson["properties"]["data_id"] = info[key]["record"].data_record_id
        if info[key]["record"].content_category == 'UmkehrN14':
            geojson["properties"]["metadata_id"] = (
                (
                    f"urn:wmo:md:org-woudc:"
                    f"{info[key]['record'].dataset_id.lower()[:-2]}"
                )
            )
        else:
            geojson["properties"]["metadata_id"] = (
                f"urn:wmo:md:org-woudc:"
                f"{info[key]['record'].content_category.lower()}"
            )
        geojson["links"][0]["href"] = info[key]["record"].url
        geojson["id"] = str(uuid.uuid4())

        mqtt_dic = {
            'topic': provide_mqtt_path(geojson["properties"]["metadata_id"]),
            'payload': json.dumps(geojson),
            'qos': 1,
        }

        notifications.append(mqtt_dic)
    return notifications


def publish_to_MQTT_Broker(info):
    """
    Publish to MQTT Broker

    :param Info: `dict` of information to be published
    :returns: `bool` of whether the publish was successful
    """
    try:
        auth = {
            'username': config.WDR_MQTT_BROKER_USERNAME,
            'password': config.WDR_MQTT_BROKER_PASSWORD,
        }

        tls_config = {
            'ca_certs': '/etc/ssl/certs/ca-certificates.crt',
            'certfile': None,
            'keyfile': None,
            'cert_reqs': (
                ssl.CERT_REQUIRED if config.WDR_MQTT_CERT_VERIFY
                else ssl.CERT_NONE
            ),
            'tls_version': ssl.PROTOCOL_TLS,
            'ciphers': None,
        }

        publish.multiple(
            info,
            hostname=config.WDR_MQTT_BROKER_HOST,
            port=int(config.WDR_MQTT_BROKER_PORT),
            client_id=config.WDR_MQTT_CLIENT_ID,
            will=None,
            tls=tls_config,
            protocol=paho_mqtt.MQTTv5,
            keepalive=60,
            auth=auth
        )

        LOGGER.info(
            f"{len(info)} MQTT notifications published successful"
        )

    except Exception as e:
        LOGGER.error(f"MQTT error: {e}")


def provide_mqtt_path(metadata_id):
    """
    Provide MQTT path
    :param dataset: dataset name

    :returns: `str` of MQTT path
    """
    ozone_dataset = [
        'totalozone',
        'totalozoneobs',
        'lidar',
        'ozonesonde',
        'rocketsonde',
        'umkehrn14_1',
        'umkehrn14_2'
    ]

    uv_dataset = [
        'broadband',
        'multiband',
        'spectral',
    ]
    dataset = metadata_id.split(':')[-1].replace('-', '')
    if dataset in ozone_dataset:
        mqtt_path = (
            "origin/a/wis2/org-woudc/data/core/"
            "atmospheric-composition/observations"
            f"/gases/ozone/{dataset}"
        )
    elif dataset in uv_dataset:
        mqtt_path = (
            "origin/a/wis2/org-woudc/data/core/"
            "atmospheric-composition/observations/"
            f"radiation-latent-heat/ultraviolet-radiation/{dataset}"
        )
    else:
        LOGGER.error(
            f"Dataset {dataset} is not supported for MQTT path generation"
        )
        mqtt_path = None
    return mqtt_path
