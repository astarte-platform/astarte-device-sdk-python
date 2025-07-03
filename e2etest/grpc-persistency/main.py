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
Specifically designed to test persistency while connected to the message hub.
"""
import argparse
import asyncio
import importlib.util
import os
import sys
import time
from pathlib import Path
from threading import Lock, Thread

from termcolor import cprint

from astarte.device.device_grpc import DeviceGrpc
from astarte.device.interface import InterfaceOwnership
from astarte.device.types import TypeAstarteData

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


def set_all_properties(device: DeviceGrpc, test_cfg: TestCfg):
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


def unset_some_properties(device: DeviceGrpc, test_cfg: TestCfg):
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


def get_interface_prop(device: DeviceGrpc, interface: str) -> dict[str, TypeAstarteData]:
    props: dict[str, TypeAstarteData] = {}
    stored_properties = device.get_interface_props(interface)

    for p in stored_properties:
        last_path_segment = p.path.split("/")[-1]
        props[last_path_segment] = p.value

    return props


def get_props(device: DeviceGrpc) -> dict[str, dict[str, TypeAstarteData]]:
    props: dict[str, dict[str, TypeAstarteData]] = {}
    stored_properties = device.get_all_props()

    for p in stored_properties:
        interface_dict: dict[str, TypeAstarteData] = props.get(p.interface, {})

        last_path_segment = p.path.split("/")[-1]
        interface_dict[last_path_segment] = p.value

        props[p.interface] = interface_dict

    return props


def main(cb_loop: asyncio.AbstractEventLoop, test_cfg: TestCfg):
    """
    Generate the device and run the end to end tests.
    """

    device = DeviceGrpc(
        server_addr=f"localhost:{test_cfg.grpc_socket_port}",
        node_uuid=test_cfg.grpc_node_uuid,
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

    device.unset_property(test_cfg.interface_device_prop, "/sensor_id/integer_endpoint")
    device.unset_property(test_cfg.interface_device_prop, "/sensor_id/datetime_endpoint")
    device.unset_property(test_cfg.interface_device_prop, "/sensor_id/booleanarray_endpoint")

    assert peek_astarte(test_cfg) == {
        test_cfg.interface_device_prop: {},
        test_cfg.interface_server_prop: {},
    }

    # Set all the properties to check properties are stored correctly
    set_all_properties(device, test_cfg)
    time.sleep(1)

    local_device_props = get_interface_prop(device, test_cfg.interface_device_prop)
    local_server_props = get_interface_prop(device, test_cfg.interface_server_prop)
    local_props = {
        test_cfg.interface_device_prop: local_device_props,
        test_cfg.interface_server_prop: local_server_props,
    }
    expect_astarte_props = peek_astarte(test_cfg)
    assert local_props == expect_astarte_props
    local_props_all = get_props(device)
    assert local_props == local_props_all

    # Unset some properties to check properties are removed from the database correctly
    unset_some_properties(device, test_cfg)
    time.sleep(1)

    local_props = {
        test_cfg.interface_device_prop: {
            "datetime_endpoint": test_cfg.mock_data["datetime_endpoint"],
            "booleanarray_endpoint": test_cfg.mock_data["booleanarray_endpoint"],
        },
        test_cfg.interface_server_prop: {
            "longinteger_endpoint": test_cfg.mock_data["longinteger_endpoint"],
            "stringarray_endpoint": test_cfg.mock_data["stringarray_endpoint"],
        },
    }
    assert all(v == local_props[k] for k, v in peek_astarte(test_cfg).items())

    property = device.get_property(
        test_cfg.interface_server_prop, "/sensor_id/longinteger_endpoint"
    )
    assert property == test_cfg.mock_data["longinteger_endpoint"]

    # Disconnect the device from Astarte
    device.disconnect()
    time.sleep(1)

    property = device.get_property(
        test_cfg.interface_device_prop,
        "/sensor_id/booleanarray_endpoint",
    )
    # no properties are returned when disconnected
    assert property is None
    none_property = device.get_property(
        test_cfg.interface_server_prop,
        "/sensor_id/longinteger_endpoint",
    )
    # no properties are returned when disconnected
    assert none_property is None
    assert device.get_all_props() == []
    assert device.get_interface_props(test_cfg.interface_server_prop) == []
    assert device.get_interface_props(test_cfg.interface_device_prop) == []
    assert device.get_device_props() == []
    assert device.get_server_props() == []

    # Connect to be able to retrieve properties again
    device.connect()
    time.sleep(1)

    device.set_property(test_cfg.interface_device_prop, "/sensor_id/integer_endpoint", 77)
    device.unset_property(test_cfg.interface_device_prop, "/sensor_id/datetime_endpoint")

    local_props = {
        test_cfg.interface_device_prop: {
            "integer_endpoint": 77,
            "booleanarray_endpoint": test_cfg.mock_data["booleanarray_endpoint"],
        },
        test_cfg.interface_server_prop: {
            "longinteger_endpoint": test_cfg.mock_data["longinteger_endpoint"],
            "stringarray_endpoint": test_cfg.mock_data["stringarray_endpoint"],
        },
    }
    assert all(v == local_props[k] for k, v in peek_astarte(test_cfg).items())

    # NOTE currently the message hub does not retrieve the properties from astarte when
    # a node reconnects so we skip this check

    # property = device.get_property(
    #    test_cfg.interface_device_prop,
    #    "/sensor_id/booleanarray_endpoint",
    # )
    # assert property == test_cfg.mock_data["booleanarray_endpoint"]
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
