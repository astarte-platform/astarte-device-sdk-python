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
Contains the tests for individual datastreams.
"""
import time
from datetime import datetime, timezone
from threading import Lock

from config import TestCfg
from http_requests import (
    get_server_interface,
    parse_received_data,
    post_server_interface,
    prepare_transmit_data,
)
from termcolor import cprint

from astarte.device.device import Device


def test_datastream_from_device_to_server(device: Device, test_cfg: TestCfg):
    """
    Test for individual datastreams in the direction from device to server
    """
    cprint(
        "\nSending device owned datastreams from device to server.",
        color="cyan",
        flush=True,
    )
    for key, value in test_cfg.mock_data.items():
        device.send(test_cfg.interface_device_data, "/" + key, value, datetime.now(tz=timezone.utc))
        time.sleep(0.005)

    time.sleep(1)

    cprint("\nChecking data stored on the server.", color="cyan", flush=True)
    json_res = get_server_interface(test_cfg, test_cfg.interface_device_data)
    parsed_res = {key: value.get("value") for key, value in json_res.get("data", {}).items()}
    if not parsed_res:
        cprint("Received: " + str(json_res), "red", flush=True)
        raise ValueError("Incorrectly formatted response from server")

    # Make sure all the keys have been correctly received
    if parsed_res.keys() != test_cfg.mock_data.keys():
        cprint("Expected: " + str(test_cfg.mock_data), "red", flush=True)
        cprint("Received: " + str(parsed_res), "red", flush=True)
        raise ValueError("Incorrectly formatted response from server")

    parse_received_data(parsed_res)

    # Check received and sent data match
    if parsed_res != test_cfg.mock_data:
        cprint("Expected: " + str(test_cfg.mock_data), "red", flush=True)
        cprint("Received: " + str(parsed_res), "red", flush=True)
        raise ValueError("Incorrect data stored on server")


def test_datastream_from_server_to_device(test_cfg: TestCfg, rx_data_lock: Lock, rx_data: dict):
    """
    Test for individual datastreams in the direction from server to device
    """
    cprint(
        "\nSending server owned datastreams from server to device.",
        color="cyan",
        flush=True,
    )

    for key, value in test_cfg.mock_data.items():
        value = prepare_transmit_data(key, value)
        post_server_interface(test_cfg, test_cfg.interface_server_data, "/" + key, value)
        time.sleep(0.005)

    time.sleep(1)

    cprint("\nChecking data received by the device.", color="cyan", flush=True)

    with rx_data_lock:
        if not rx_data.get(test_cfg.interface_server_data):
            raise ValueError(
                f"No data from this interface has been received {test_cfg.interface_server_data}"
            )
        parsed_rx_data = rx_data.get(test_cfg.interface_server_data)

    # Make sure all the data has been correctly received
    if parsed_rx_data != {("/" + k): v for (k, v) in test_cfg.mock_data.items()}:
        cprint(
            "Expected: " + str({("/" + k): v for (k, v) in test_cfg.mock_data.items()}),
            "red",
            flush=True,
        )
        cprint("Received: " + str(parsed_rx_data), "red", flush=True)
        raise ValueError("Incorrectly formatted response from server")
