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
Contains the tests for aggregated object datastreams.
"""

import base64
import copy
import time
from threading import Lock

from config import TestCfg
from dateutil import parser
from http_requests import (
    get_server_interface,
    post_server_interface,
    prepare_transmit_data,
)
from termcolor import cprint

from astarte.device.device import Device


def test_aggregate_from_device_to_server(device: Device, test_cfg: TestCfg, empty_array: bool):
    """
    Test for aggregated object datastreams in the direction from device to server
    """
    cprint(
        "\nSending device owned aggregates from device to server.",
        color="cyan",
        flush=True,
    )
    mock_data = {
        key: value if (type(value) is not list) else []
        for key, value in test_cfg.mock_data.items()
    } if empty_array else test_cfg.mock_data
    device.send_aggregate(test_cfg.interface_device_aggr, "/sensor_id", mock_data)

    time.sleep(1)

    cprint("\nChecking data stored on the server.", color="cyan", flush=True)
    json_res = get_server_interface(test_cfg, test_cfg.interface_device_aggr)
    parsed_res = json_res.get("data", {}).get("sensor_id")
    if not parsed_res:
        raise ValueError("Incorrectly formatted response from server")
    if isinstance(parsed_res, list):
        parsed_res = parsed_res[-1]

    # Remove timestamp
    parsed_res.pop("timestamp")

    # Parse longint from string to int
    parsed_res["longinteger_endpoint"] = int(parsed_res["longinteger_endpoint"])
    if parsed_res["longintegerarray_endpoint"] != None:
        parsed_res["longintegerarray_endpoint"] = [
            int(dt) for dt in parsed_res["longintegerarray_endpoint"]
        ]

    # Parse datetime from string to datetime
    parsed_res["datetime_endpoint"] = parser.parse(parsed_res["datetime_endpoint"])
    if parsed_res["datetimearray_endpoint"] != None:
        parsed_res["datetimearray_endpoint"] = [
            parser.parse(dt) for dt in parsed_res["datetimearray_endpoint"]
        ]

    # Decode binary blob from base64
    parsed_res["binaryblob_endpoint"] = base64.b64decode(parsed_res["binaryblob_endpoint"])
    if parsed_res["binaryblobarray_endpoint"] != None:
        parsed_res["binaryblobarray_endpoint"] = [
            base64.b64decode(dt) for dt in parsed_res["binaryblobarray_endpoint"]
        ]

    # Check received and sent data match
    mock_data = {
        key: value if (type(value) is not list) else None
        for key, value in test_cfg.mock_data.items()
    } if empty_array else test_cfg.mock_data
    if parsed_res != mock_data:
        raise ValueError("Incorrect data stored on server")


def test_aggregate_from_server_to_device(test_cfg: TestCfg, rx_data_lock: Lock, rx_data: dict, empty_array: bool):
    """
    Test for aggregated object datastreams in the direction from server to device
    """
    cprint(
        "\nSending server owned aggregates from server to device.",
        color="cyan",
        flush=True,
    )

    data = copy.deepcopy(test_cfg.mock_data)
    key = "binaryblob_endpoint"
    data[key] = prepare_transmit_data(key, data[key])
    key = "binaryblobarray_endpoint"
    data[key] = prepare_transmit_data(key, data[key])

    data = {
        key: value if (type(value) is not list) else []
        for key, value in data.items()
    } if empty_array else data

    post_server_interface(test_cfg, test_cfg.interface_server_aggr, "/sensor_id", data)

    time.sleep(1)

    cprint("\nChecking data received by the device.", color="cyan", flush=True)
    with rx_data_lock:
        if not rx_data.get(test_cfg.interface_server_aggr):
            raise ValueError(
                f"No data from this interface has been received {test_cfg.interface_server_aggr}"
            )
        parsed_rx_data = rx_data.get(test_cfg.interface_server_aggr).get("/sensor_id")

    # Make sure all the data has been correctly received
    mock_data = {
        key: value if (type(value) is not list) else []
        for key, value in test_cfg.mock_data.items()
    } if empty_array else test_cfg.mock_data
    if parsed_rx_data != mock_data:
        raise ValueError("Incorrectly formatted response from server")
