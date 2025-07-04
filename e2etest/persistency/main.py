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
import tempfile
import time
from pathlib import Path
from threading import Lock, Thread

from termcolor import cprint

from astarte.device.interface import InterfaceOwnership

# Assuming this script is called from the root folder of this project.
prj_path = Path(os.getcwd())
if str(prj_path) not in sys.path:
    sys.path.insert(0, str(prj_path))

from astarte.device import DeviceMqtt

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

utils_path = Path.joinpath(Path.cwd(), "e2etest", "common", "utils.py")
spec = importlib.util.spec_from_file_location("utils", utils_path)
utils = importlib.util.module_from_spec(spec)
sys.modules["utils"] = utils
spec.loader.exec_module(utils)

from config import TestCfg
from http_requests import (
    delete_server_interface,
    get_server_interface,
    parse_received_data,
    post_server_interface,
    prepare_transmit_data,
)
from utils import peek_database, properties_to_tuples

rx_data_lock = Lock()
rx_data = {}


def on_connected_cbk(_):
    """
    Callback for a connection event.
    """
    cprint("\nDevice connected.", color="green", flush=True)


def on_data_received_cbk(_, name: str, path: str, payload: dict):
    """
    Callback for a data reception event.
    """
    with rx_data_lock:
        if not rx_data.get(name):
            rx_data[name] = {}
        rx_data[name][path] = payload


def on_disconnected_cbk(_, reason: int):
    """
    Callback for a disconnection event.
    """
    cprint(f"\nDevice disconnected because: {reason}.", color="red", flush=True)


def set_all_properties(device: DeviceMqtt, test_cfg: TestCfg):
    """
    Set all the device and server owned properties.
    """
    cprint("\nSet device owned properties.", color="cyan", flush=True)
    for key, value in test_cfg.mock_data.items():
        device.set_property(test_cfg.interface_device_prop, "/sensor_id/" + key, value)
        time.sleep(0.005)

    cprint("\nSet server owned properties.", color="cyan", flush=True)
    for key, value in test_cfg.mock_data.items():
        value = prepare_transmit_data(key, value)
        post_server_interface(test_cfg, test_cfg.interface_server_prop, "/sensor_id/" + key, value)
        time.sleep(0.005)


def unset_some_properties(device: DeviceMqtt, test_cfg: TestCfg):
    """
    Unset some of the device and server owned properties.
    """
    cprint("\nUnset some device owned properties.", color="cyan", flush=True)
    for key, _ in test_cfg.mock_data.items():
        if key not in ["datetime_endpoint", "booleanarray_endpoint"]:
            device.unset_property(test_cfg.interface_device_prop, "/sensor_id/" + key)
            time.sleep(0.005)

    cprint("\nUnset some server owned properties.", color="cyan", flush=True)
    for key, _ in test_cfg.mock_data.items():
        if key not in ["longinteger_endpoint", "stringarray_endpoint"]:
            delete_server_interface(test_cfg, test_cfg.interface_server_prop, "/sensor_id/" + key)
        time.sleep(0.005)


def shuffle_database(persistency_dir: Path, test_cfg: TestCfg):
    """
    Add and remove some properties from the database to create some differences with the
    Astarte instance.
    """
    database_path = persistency_dir.joinpath(test_cfg.device_id, "caching", "astarte.db")
    connection = sqlite3.connect(database_path)
    cursor = connection.cursor()
    cursor.execute(
        "DELETE FROM properties WHERE interface=? AND path=?",
        (test_cfg.interface_device_prop, "/sensor_id/datetime_endpoint"),
    )
    cursor.execute(
        "DELETE FROM properties WHERE interface=? AND path=?",
        (test_cfg.interface_server_prop, "/sensor_id/longinteger_endpoint"),
    )
    cursor.execute(
        "INSERT OR REPLACE INTO properties (interface, major, path, ownership, value) VALUES "
        "(?, ?, ?, ?, ?)",
        (
            test_cfg.interface_device_prop,
            0,
            "/sensor_id/integer_endpoint",
            InterfaceOwnership.DEVICE.value,
            pickle.dumps(66),
        ),
    )
    cursor.execute(
        "INSERT OR REPLACE INTO properties (interface, major, path, ownership, value) VALUES "
        "(?, ?, ?, ?, ?)",
        (
            test_cfg.interface_server_prop,
            0,
            "/sensor_id/boolean_endpoint",
            InterfaceOwnership.SERVER.value,
            pickle.dumps(True),
        ),
    )
    connection.commit()


def peek_astarte(test_cfg: TestCfg):
    """
    Get the set properties in the Astarte cluster.
    """
    server_data = {}
    cprint("\nReading data stored on the server.", color="cyan", flush=True)
    json_res = get_server_interface(test_cfg, test_cfg.interface_device_prop)
    server_data[test_cfg.interface_device_prop] = json_res.get("data", {}).get("sensor_id", {})
    parse_received_data(server_data[test_cfg.interface_device_prop])

    json_res = get_server_interface(test_cfg, test_cfg.interface_server_prop)
    server_data[test_cfg.interface_server_prop] = json_res.get("data", {}).get("sensor_id", {})
    parse_received_data(server_data[test_cfg.interface_server_prop])
    return server_data


def main(cb_loop: asyncio.AbstractEventLoop, test_cfg: TestCfg):
    """
    Generate the device and run the end to end tests.
    """
    with tempfile.TemporaryDirectory("astarte-sdk-python-e2e") as persistency_path:
        persistency_dir = Path(persistency_path)

        device = DeviceMqtt(
            device_id=test_cfg.device_id,
            realm=test_cfg.realm,
            credentials_secret=test_cfg.credentials_secret,
            pairing_base_url=test_cfg.pairing_url,
            persistency_dir=persistency_path,
            ignore_ssl_errors=False,
        )
        device.add_interfaces_from_dir(test_cfg.interfaces_fld)
        device.set_events_callbacks(
            on_connected=on_connected_cbk,
            on_data_received=on_data_received_cbk,
            on_disconnected=on_disconnected_cbk,
            loop=cb_loop,
        )
        device.connect()

        time.sleep(1)

        if not device.is_connected():
            print("Connection failed.", flush=True)
            sys.exit(1)

        assert peek_database(persistency_dir, test_cfg.device_id) == list()
        assert peek_astarte(test_cfg) == {
            test_cfg.interface_device_prop: {},
            test_cfg.interface_server_prop: {},
        }

        # Set all the properties to check properties are stored correctly
        set_all_properties(device, test_cfg)
        time.sleep(1)

        actual_db = peek_database(persistency_dir, test_cfg.device_id)
        expect_db_device = [
            (
                test_cfg.interface_device_prop,
                0,
                f"/sensor_id/{k}",
                InterfaceOwnership.DEVICE,
                v,
            )
            for k, v in test_cfg.mock_data.items()
        ]
        expect_db_server = [
            (
                test_cfg.interface_server_prop,
                0,
                f"/sensor_id/{k}",
                InterfaceOwnership.SERVER,
                v,
            )
            for k, v in test_cfg.mock_data.items()
        ]
        expect_db = expect_db_device + expect_db_server
        if actual_db != expect_db:
            print(f"Expectec database: {expect_db}", flush=True)
            print(f"Actual database: {actual_db}", flush=True)
        assert actual_db == expect_db
        assert all(data == test_cfg.mock_data for data in peek_astarte(test_cfg).values())
        actual_property_device = properties_to_tuples(device.get_device_props())
        assert expect_db_device == actual_property_device
        actual_property_server = properties_to_tuples(device.get_server_props())
        assert expect_db_server == actual_property_server

        # Unset some properties to check properties are removed from the database correctly
        unset_some_properties(device, test_cfg)
        time.sleep(1)

        actual_db = peek_database(persistency_dir, test_cfg.device_id)
        expect_db_device = [
            (
                test_cfg.interface_device_prop,
                0,
                f"/sensor_id/{k}",
                InterfaceOwnership.DEVICE,
                v,
            )
            for k, v in test_cfg.mock_data.items()
            if k in ["datetime_endpoint", "booleanarray_endpoint"]
        ]
        expect_db_server = [
            (
                test_cfg.interface_server_prop,
                0,
                f"/sensor_id/{k}",
                InterfaceOwnership.SERVER,
                v,
            )
            for k, v in test_cfg.mock_data.items()
            if k in ["longinteger_endpoint", "stringarray_endpoint"]
        ]
        expect_db = expect_db_device + expect_db_server
        if actual_db != expect_db:
            print(f"Expectec database: {expect_db}", flush=True)
            print(f"Actual database: {actual_db}", flush=True)
        assert actual_db == expect_db
        expect_astarte = {
            test_cfg.interface_device_prop: {
                "datetime_endpoint": test_cfg.mock_data["datetime_endpoint"],
                "booleanarray_endpoint": test_cfg.mock_data["booleanarray_endpoint"],
            },
            test_cfg.interface_server_prop: {
                "longinteger_endpoint": test_cfg.mock_data["longinteger_endpoint"],
                "stringarray_endpoint": test_cfg.mock_data["stringarray_endpoint"],
            },
        }
        assert all(v == expect_astarte[k] for k, v in peek_astarte(test_cfg).items())
        actual_property_device = properties_to_tuples(device.get_device_props())
        assert expect_db_device == actual_property_device
        actual_property_server = properties_to_tuples(device.get_server_props())
        assert expect_db_server == actual_property_server

        property = device.get_property(
            test_cfg.interface_server_prop, "/sensor_id/longinteger_endpoint"
        )
        assert property == test_cfg.mock_data["longinteger_endpoint"]

        # Disconnect the device from Astarte
        device.disconnect()
        time.sleep(1)

        # Remove/Add some set server/device properties from the database manually
        shuffle_database(persistency_dir, test_cfg)

        device_property_boleanarray = [True, False, True, False]
        expect_db = [
            (
                test_cfg.interface_device_prop,
                0,
                "/sensor_id/booleanarray_endpoint",
                InterfaceOwnership.DEVICE,
                device_property_boleanarray,
            ),
            (
                test_cfg.interface_server_prop,
                0,
                "/sensor_id/stringarray_endpoint",
                InterfaceOwnership.SERVER,
                ["hello", " world"],
            ),
            (
                test_cfg.interface_device_prop,
                0,
                "/sensor_id/integer_endpoint",
                InterfaceOwnership.DEVICE,
                66,
            ),
            (
                test_cfg.interface_server_prop,
                0,
                "/sensor_id/boolean_endpoint",
                InterfaceOwnership.SERVER,
                True,
            ),
        ]

        property = device.get_property(
            test_cfg.interface_device_prop,
            "/sensor_id/booleanarray_endpoint",
        )
        assert property == device_property_boleanarray
        none_property = device.get_property(
            test_cfg.interface_server_prop,
            "/sensor_id/longinteger_endpoint",
        )
        assert none_property is None

        assert peek_database(persistency_dir, test_cfg.device_id) == expect_db
        expect_astarte = {
            test_cfg.interface_device_prop: {
                "datetime_endpoint": test_cfg.mock_data["datetime_endpoint"],
                "booleanarray_endpoint": test_cfg.mock_data["booleanarray_endpoint"],
            },
            test_cfg.interface_server_prop: {
                "longinteger_endpoint": test_cfg.mock_data["longinteger_endpoint"],
                "stringarray_endpoint": test_cfg.mock_data["stringarray_endpoint"],
            },
        }
        assert all(v == expect_astarte[k] for k, v in peek_astarte(test_cfg).items())

        # Connect to synchronize the database content with Astarte
        device.connect()
        time.sleep(1)

        expect_db = [
            (
                "org.astarte-platform.python.e2etest.DeviceProperty",
                0,
                "/sensor_id/booleanarray_endpoint",
                InterfaceOwnership.DEVICE,
                [True, False, True, False],
            ),
            (
                "org.astarte-platform.python.e2etest.DeviceProperty",
                0,
                "/sensor_id/integer_endpoint",
                InterfaceOwnership.DEVICE,
                66,
            ),
            (
                "org.astarte-platform.python.e2etest.ServerProperty",
                0,
                "/sensor_id/longinteger_endpoint",
                InterfaceOwnership.SERVER,
                45543543534,
            ),
            (
                "org.astarte-platform.python.e2etest.ServerProperty",
                0,
                "/sensor_id/stringarray_endpoint",
                InterfaceOwnership.SERVER,
                ["hello", " world"],
            ),
        ]
        assert peek_database(persistency_dir, test_cfg.device_id) == expect_db
        expect_astarte = {
            test_cfg.interface_device_prop: {
                "integer_endpoint": 66,
                "booleanarray_endpoint": test_cfg.mock_data["booleanarray_endpoint"],
            },
            test_cfg.interface_server_prop: {
                "longinteger_endpoint": test_cfg.mock_data["longinteger_endpoint"],
                "stringarray_endpoint": test_cfg.mock_data["stringarray_endpoint"],
            },
        }
        assert all(v == expect_astarte[k] for k, v in peek_astarte(test_cfg).items())

        property = device.get_property(
            test_cfg.interface_device_prop,
            "/sensor_id/booleanarray_endpoint",
        )
        assert property == device_property_boleanarray
        none_property = device.get_property(
            test_cfg.interface_device_prop,
            "/sensor_id/boolean_endpoint",
        )
        assert none_property is None


def start_call_back_loop(loop: asyncio.AbstractEventLoop) -> None:
    """
    Start an asyncio event loop, used for the device call back.
    """
    asyncio.set_event_loop(loop)
    loop.run_forever()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--device_n", default=2, type=int)
    parser.add_argument("--mock_data_n", default=1, type=int)
    args = parser.parse_args()

    # Generate an async loop and thread
    call_back_loop = asyncio.new_event_loop()
    call_back_thread = Thread(target=start_call_back_loop, args=[call_back_loop], daemon=True)
    call_back_thread.start()

    try:
        main(
            call_back_loop,
            TestCfg(device_n=args.device_n, mock_data_n=args.mock_data_n),
        )
    except Exception as e:
        call_back_loop.stop()
        call_back_thread.join(timeout=1)
        raise e
