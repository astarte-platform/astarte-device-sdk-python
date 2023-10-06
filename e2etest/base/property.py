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
Contains the tests for individual properties.
"""

import time
from threading import Lock

from config import TestCfg
from http_requests import (
    delete_server_interface,
    get_server_interface,
    parse_received_data,
    post_server_interface,
    prepare_transmit_data,
)
from termcolor import cprint

from astarte.device import DeviceMqtt


def test_properties_from_device_to_server(device: DeviceMqtt, test_cfg: TestCfg):
    """
    Test for individual properties in the direction from device to server
    """
    cprint("\nSet device owned properties.", color="cyan", flush=True)
    for key, value in test_cfg.mock_data.items():
        device.send(test_cfg.interface_device_prop, "/sensor_id/" + key, value)
        time.sleep(0.005)

    time.sleep(1)

    cprint("\nChecking data stored on the server.", color="cyan", flush=True)
    json_res = get_server_interface(test_cfg, test_cfg.interface_device_prop)
    parsed_res = json_res.get("data", {}).get("sensor_id")
    if not parsed_res:
        cprint(json_res, "red", flush=True)
        raise ValueError("Incorrectly formatted response from server")

    parse_received_data(parsed_res)

    # Check received and sent data match
    if parsed_res != test_cfg.mock_data:
        cprint(parsed_res, "red", flush=True)
        raise ValueError("Incorrect data stored on server")

    # Unset all the properties
    cprint("\nUnset all the device owned properties.", color="cyan", flush=True)
    for key, _ in test_cfg.mock_data.items():
        device.unset_property(test_cfg.interface_device_prop, "/sensor_id/" + key)
        time.sleep(0.005)

    time.sleep(1)

    cprint("\nChecking data stored on the server.", color="cyan", flush=True)
    json_res = get_server_interface(test_cfg, test_cfg.interface_device_prop)
    parsed_res = json_res.get("data", {})

    # Check received and sent data match
    if parsed_res != {}:
        cprint(parsed_res, "red", flush=True)
        raise ValueError("Incorrect data stored on server")


def test_properties_from_server_to_device(test_cfg: TestCfg, rx_data_lock: Lock, rx_data: dict):
    """
    Test for individual properties in the direction from server to device
    """

    # pylint: disable=duplicate-code

    cprint(
        "\nSending server owned properties from server to device.",
        color="cyan",
        flush=True,
    )

    for key, value in test_cfg.mock_data.items():
        value = prepare_transmit_data(key, value)
        post_server_interface(test_cfg, test_cfg.interface_server_prop, "/sensor_id/" + key, value)
        time.sleep(0.005)

    time.sleep(1)

    cprint("\nChecking data received by the device.", color="cyan", flush=True)

    with rx_data_lock:
        if not rx_data.get(test_cfg.interface_server_prop):
            raise ValueError(
                f"No data from this interface has been received {test_cfg.interface_server_prop}"
            )
        parsed_rx_data = rx_data.get(test_cfg.interface_server_prop)

    if parsed_rx_data != {("/sensor_id/" + k): v for (k, v) in test_cfg.mock_data.items()}:
        cprint(parsed_rx_data, "red", flush=True)
        raise ValueError("Incorrectly formatted response from server")

    # Unset all the properties
    cprint("\nUnset all the server owned properties.", color="cyan", flush=True)
    for key, _ in test_cfg.mock_data.items():
        delete_server_interface(test_cfg, test_cfg.interface_server_prop, "/sensor_id/" + key)
        time.sleep(0.005)

    time.sleep(1)

    cprint("\nChecking data received by the device.", color="cyan", flush=True)

    with rx_data_lock:
        if not rx_data.get(test_cfg.interface_server_prop):
            raise ValueError(
                f"No data from this interface has been received {test_cfg.interface_server_prop}"
            )
        parsed_rx_data = rx_data.get(test_cfg.interface_server_prop)

    if parsed_rx_data != {"/sensor_id/" + k: None for k in test_cfg.mock_data}:
        cprint(parsed_rx_data, "red", flush=True)
        raise ValueError("Incorrectly formatted response from server")
