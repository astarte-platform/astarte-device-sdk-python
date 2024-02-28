# This file is part of Astarte.
#
# Copyright 2023 SECO Mind Srl
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# SPDX-License-Identifier: Apache-2.0
"""
Contains useful wrappers for HTTPS requests.
"""
import base64
import json

import requests
from config import TestCfg
from dateutil import parser
from termcolor import cprint


def get_server_interface(test_cfg: TestCfg, interface: str, quiet: bool = False):
    """
    Wrapper for a GET request for the server returning the specified interface data.
    """
    request_body = (
        test_cfg.appengine_url
        + "/v1/"
        + test_cfg.realm
        + "/devices/"
        + test_cfg.device_id
        + "/interfaces/"
        + interface
    )
    headers = {"Authorization": "Bearer " + test_cfg.appengine_token}
    print(f"Sending HTTP GET request: {request_body}", flush=True)
    res = requests.get(request_body, headers=headers, timeout=1)
    if res.status_code != 200:
        if not quiet:
            cprint(res.text, "red", flush=True)
        raise requests.HTTPError("GET request failed.")

    return res.json()


def post_server_interface(
    test_cfg: TestCfg, interface: str, endpoint: str, data: dict, quiet: bool = False
):
    """
    Wrapper for a POST request for the server, uploading new values to an interface.
    """
    request_body = (
        test_cfg.appengine_url
        + "/v1/"
        + test_cfg.realm
        + "/devices/"
        + test_cfg.device_id
        + "/interfaces/"
        + interface
        + endpoint
    )
    json_data = json.dumps({"data": data}, default=str)
    headers = {
        "Authorization": "Bearer " + test_cfg.appengine_token,
        "Content-Type": "application/json",
    }
    print(f"Sending HTTP POST request: {request_body} {json_data}", flush=True)
    res = requests.post(url=request_body, data=json_data, headers=headers, timeout=1)
    if res.status_code != 200:
        if not quiet:
            cprint(res.text, "red", flush=True)
        raise requests.HTTPError("POST request failed.")


def delete_server_interface(test_cfg: TestCfg, interface: str, endpoint: str, quiet: bool = False):
    """
    Wrapper for a DELETE request for the server, deleting an endpoint.
    """
    request_body = (
        test_cfg.appengine_url
        + "/v1/"
        + test_cfg.realm
        + "/devices/"
        + test_cfg.device_id
        + "/interfaces/"
        + interface
        + endpoint
    )
    headers = {
        "Authorization": "Bearer " + test_cfg.appengine_token,
        "Content-Type": "application/json",
    }
    print(f"Sending HTTP DELETE request: {request_body}", flush=True)
    res = requests.delete(request_body, headers=headers, timeout=1)
    if res.status_code != 204:
        if not quiet:
            cprint(res.text, "red", flush=True)
        raise requests.HTTPError("DELETE request failed.")


def prepare_transmit_data(key, value):
    """
    Some data to be transmitted should be encoded to an appropriate type.
    """
    if key == "binaryblob_endpoint":
        return base64.b64encode(value).decode("utf-8")
    if key == "binaryblobarray_endpoint":
        return [base64.b64encode(v).decode("utf-8") for v in value]
    return value


def parse_received_data(data):
    """
    Some of the received data is not automatically parsed as Python types.
    Specifically, datetime and binaryblob should be converted manually from strings.
    """
    # Parse datetime from string to datetime
    if "datetime_endpoint" in data:
        data["datetime_endpoint"] = parser.parse(data["datetime_endpoint"])
    if ("datetimearray_endpoint" in data) and (data["datetimearray_endpoint"] != None):
        data["datetimearray_endpoint"] = [parser.parse(dt) for dt in data["datetimearray_endpoint"]]

    # Decode binary blob from base64
    if "binaryblob_endpoint" in data:
        data["binaryblob_endpoint"] = base64.b64decode(data["binaryblob_endpoint"])
    if ("binaryblobarray_endpoint" in data) and (data["binaryblobarray_endpoint"] != None):
        data["binaryblobarray_endpoint"] = [
            base64.b64decode(dt) for dt in data["binaryblobarray_endpoint"]
        ]
