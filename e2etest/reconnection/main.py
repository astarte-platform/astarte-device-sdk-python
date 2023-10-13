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
End to end testing framework.
Specifically designed to test persistency.
"""
import argparse
import asyncio
import importlib.util
import os
import pickle
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock, Thread

import requests
from termcolor import cprint

# Assuming this script is called from the root folder of this project.
prj_path = Path(os.getcwd())
if str(prj_path) not in sys.path:
    sys.path.insert(0, str(prj_path))

from astarte.device import (
    DeviceDisconnectedError,
    DeviceGrpc,
    DeviceMqtt,
    InterfaceNotFoundError,
)

config_path = Path.joinpath(Path.cwd(), "e2etest", "common", "config.py")
spec = importlib.util.spec_from_file_location("config", config_path)
config = importlib.util.module_from_spec(spec)
sys.modules["config"] = config
spec.loader.exec_module(config)

http_requests_path = Path.joinpath(Path.cwd(), "e2etest", "common", "http_requests.py")
spec = importlib.util.spec_from_file_location("http_requests", http_requests_path)
http_requests = importlib.util.module_from_spec(spec)
sys.modules["http_requests"] = http_requests
spec.loader.exec_module(http_requests)

from config import TestCfg
from http_requests import get_server_interface


def on_connected_cbk(_):
    """
    Callback for a connection event.
    """
    cprint("Device connected.", color="green", flush=True)


def on_data_received_cbk(_, name: str, path: str, payload: dict):
    """
    Callback for a data reception event.
    """
    cprint(f"Data received: {name}, {path}, {payload}.", color="red", flush=True)


def on_disconnected_cbk(_, reason: int):
    """
    Callback for a disconnection event.
    """
    if reason == 0:
        cprint(f"Device gracefully disconnected.", color="green", flush=True)
    else:
        cprint(f"Device disconnected because: {reason}.", color="red", flush=True)


def device_connect(device: DeviceMqtt):
    """
    Helper function to perform device connection.
    """
    time.sleep(0.5)

    device.connect()

    time.sleep(0.5)

    if not device.is_connected():
        cprint("\nConnection failed.", color="red", flush=True)
        sys.exit(1)


def device_disconnect(device: DeviceMqtt):
    """
    Helper function to perform device disconnection.
    """
    time.sleep(0.5)

    device.disconnect()

    time.sleep(0.5)

    if device.is_connected():
        cprint("\nDisconnection failed.", color="red", flush=True)
        sys.exit(1)


def test_add_and_remove_interface_while_disconnected(device: DeviceMqtt, test_cfg: TestCfg):
    """
    Test add and remove interface functionality while the device is disconnected.

    The device should be disconnected when calling whis function.
    """
    cprint("\nTesting add/remove interface while disconnected.", color="cyan", flush=True)

    device_connect(device)

    device.send(
        test_cfg.interface_device_data,
        "/booleanarray_endpoint",
        [False, True],
        datetime.now(tz=timezone.utc),
    )

    json_res = get_server_interface(test_cfg, test_cfg.interface_device_data)
    assert json_res["data"]["booleanarray_endpoint"]["value"] == [False, True]

    device_disconnect(device)

    device.remove_interface(test_cfg.interface_device_data)

    device_connect(device)

    try:
        device.send(
            test_cfg.interface_device_data,
            "/booleanarray_endpoint",
            [False, True],
            datetime.now(tz=timezone.utc),
        )
    except InterfaceNotFoundError:
        # Correct behaviour
        pass
    else:
        cprint("Exception not raised for send on removed interface.", color="red", flush=True)
        sys.exit(1)

    try:
        get_server_interface(test_cfg, test_cfg.interface_device_data, quiet=True)
    except requests.exceptions.HTTPError:
        # Correct behaviour
        pass
    else:
        cprint("Exception not raised for http get on removed interface.", color="red", flush=True)
        sys.exit(1)

    device_disconnect(device)

    device.add_interface_from_file(
        test_cfg.interfaces_fld.joinpath(
            "org.astarte-platform.python.e2etest.DeviceDatastream.json"
        )
    )

    device_connect(device)

    device.send(
        test_cfg.interface_device_data,
        "/booleanarray_endpoint",
        [True, True],
        datetime.now(tz=timezone.utc),
    )

    json_res = get_server_interface(test_cfg, test_cfg.interface_device_data)
    assert json_res["data"]["booleanarray_endpoint"]["value"] == [True, True]


def test_add_and_remove_interface_while_connected(device: DeviceMqtt, test_cfg: TestCfg):
    """
    Test add and remove interface functionality while the device is connected.

    The device should be connected when calling whis function.
    """
    cprint("\nTesting add/remove interface while connected.", color="cyan", flush=True)

    device.remove_interface(test_cfg.interface_device_data)

    time.sleep(0.5)

    try:
        device.send(
            test_cfg.interface_device_data,
            "/booleanarray_endpoint",
            [False, True],
            datetime.now(tz=timezone.utc),
        )
    except InterfaceNotFoundError:
        # Correct behaviour
        pass
    else:
        cprint("Exception not raised for send on removed interface.", color="red", flush=True)
        sys.exit(1)

    try:
        get_server_interface(test_cfg, test_cfg.interface_device_data, quiet=True)
    except requests.exceptions.HTTPError:
        # Correct behaviour
        pass
    else:
        cprint("Exception not raised for http get on removed interface.", color="red", flush=True)
        sys.exit(1)

    time.sleep(0.5)

    device.add_interface_from_file(
        test_cfg.interfaces_fld.joinpath(
            "org.astarte-platform.python.e2etest.DeviceDatastream.json"
        )
    )

    time.sleep(0.5)

    device.send(
        test_cfg.interface_device_data,
        "/booleanarray_endpoint",
        [False, False],
        datetime.now(tz=timezone.utc),
    )

    json_res = get_server_interface(test_cfg, test_cfg.interface_device_data)
    assert json_res["data"]["booleanarray_endpoint"]["value"] == [False, False]


def peek_database(persistency_dir: Path, device_id: str, interface_name: str):
    """
    Take a peek in the device database.
    """
    database_path = persistency_dir.joinpath(device_id, "caching", "astarte.db")
    properties = (
        sqlite3.connect(database_path)
        .cursor()
        .execute("SELECT * FROM properties WHERE interface=?", (interface_name,))
        .fetchall()
    )
    parsed_properties = []
    for interface, major, path, value in properties:
        parsed_properties += [(interface, major, path, pickle.loads(value))]
    return parsed_properties


def test_add_and_remove_property_interface_while_connected(
    persistency_dir: Path, device: DeviceMqtt, test_cfg: TestCfg
):
    """
    Test add and remove interface functionality while the device is connected specifically for a
    property.

    The device should be connected when calling whis function.
    """
    cprint("\nTesting add/remove property interface while connected.", color="cyan", flush=True)

    device.send(
        test_cfg.interface_device_prop,
        "/s12/booleanarray_endpoint",
        [True, False],
        None,
    )

    json_res = get_server_interface(test_cfg, test_cfg.interface_device_prop)
    assert json_res["data"]["s12"]["booleanarray_endpoint"] == [True, False]

    prop_in_database = peek_database(
        persistency_dir, test_cfg.device_id, test_cfg.interface_device_prop
    )
    expected_prop_in_database = [
        (
            "org.astarte-platform.python.e2etest.DeviceProperty",
            0,
            "/s12/booleanarray_endpoint",
            [True, False],
        )
    ]
    assert prop_in_database == expected_prop_in_database

    time.sleep(0.5)

    device.remove_interface(test_cfg.interface_device_prop)

    time.sleep(0.5)

    try:
        device.send(
            test_cfg.interface_device_prop,
            "/s12/booleanarray_endpoint",
            [False, True],
            None,
        )
    except InterfaceNotFoundError:
        # Correct behaviour
        pass
    else:
        cprint("Exception not raised for send on removed interface.", color="red", flush=True)
        sys.exit(1)

    try:
        get_server_interface(test_cfg, test_cfg.interface_device_prop)
    except requests.exceptions.HTTPError:
        # Correct behaviour
        pass
    else:
        cprint("Exception not raised for http get on removed interface.", color="red", flush=True)
        sys.exit(1)

    prop_in_database = peek_database(
        persistency_dir, test_cfg.device_id, test_cfg.interface_device_prop
    )
    assert prop_in_database == list()

    time.sleep(0.5)

    device.add_interface_from_file(
        test_cfg.interfaces_fld.joinpath("org.astarte-platform.python.e2etest.DeviceProperty.json")
    )

    time.sleep(0.5)

    device.send(
        test_cfg.interface_device_prop,
        "/s12/booleanarray_endpoint",
        [False, False],
        None,
    )

    json_res = get_server_interface(test_cfg, test_cfg.interface_device_prop)
    assert json_res["data"]["s12"]["booleanarray_endpoint"] == [False, False]

    prop_in_database = peek_database(
        persistency_dir, test_cfg.device_id, test_cfg.interface_device_prop
    )
    expected_prop_in_database = [
        (
            "org.astarte-platform.python.e2etest.DeviceProperty",
            0,
            "/s12/booleanarray_endpoint",
            [False, False],
        )
    ]
    assert prop_in_database == expected_prop_in_database


def main(cb_loop: asyncio.AbstractEventLoop, test_cfg: TestCfg):
    """
    Generate the device and run the end to end tests.
    """
    persistency_dir = Path.joinpath(Path.cwd(), "e2etest", "reconnection", "build")
    if not Path.is_dir(persistency_dir):
        os.makedirs(persistency_dir)

    if test_cfg.grpc_socket_port is None:
        device = DeviceMqtt(
            device_id=test_cfg.device_id,
            realm=test_cfg.realm,
            credentials_secret=test_cfg.credentials_secret,
            pairing_base_url=test_cfg.pairing_url,
            persistency_dir=persistency_dir,
            ignore_ssl_errors=False,
        )
    else:
        device = DeviceGrpc(
            server_addr=f"localhost:{test_cfg.grpc_socket_port}", node_uuid=test_cfg.grpc_node_uuid
        )

    device.add_interfaces_from_dir(test_cfg.interfaces_fld)
    device.set_events_callbacks(
        on_connected=on_connected_cbk,
        on_data_received=on_data_received_cbk,
        on_disconnected=on_disconnected_cbk,
        loop=cb_loop,
    )

    test_add_and_remove_interface_while_disconnected(device, test_cfg)

    time.sleep(0.5)

    test_add_and_remove_interface_while_connected(device, test_cfg)

    time.sleep(0.5)

    if test_cfg.grpc_socket_port is None:
        test_add_and_remove_property_interface_while_connected(persistency_dir, device, test_cfg)

        time.sleep(0.5)


def start_call_back_loop(loop: asyncio.AbstractEventLoop) -> None:
    """
    Start an asyncio event loop, used for the device call back.
    """
    asyncio.set_event_loop(loop)
    loop.run_forever()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--device_n", default=3, type=int)
    args = parser.parse_args()

    # Generate an async loop and thread
    call_back_loop = asyncio.new_event_loop()
    call_back_thread = Thread(target=start_call_back_loop, args=[call_back_loop], daemon=True)
    call_back_thread.start()

    try:
        main(call_back_loop, TestCfg(number=args.device_n))
    except Exception as e:
        call_back_loop.stop()
        call_back_thread.join(timeout=1)
        raise e
