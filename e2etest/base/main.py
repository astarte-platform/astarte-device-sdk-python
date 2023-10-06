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
"""
import asyncio
import importlib.util
import os
import sys
import time
from pathlib import Path
from threading import Lock, Thread

from termcolor import cprint

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

from aggregate import (
    test_aggregate_from_device_to_server,
    test_aggregate_from_server_to_device,
)
from config import TestCfg
from datastream import (
    test_datastream_from_device_to_server,
    test_datastream_from_server_to_device,
)
from property import (
    test_properties_from_device_to_server,
    test_properties_from_server_to_device,
)

rx_data_lock = Lock()
rx_data = {}


def on_connected_cbk(_):
    """
    Callback for a connection event.
    """
    cprint("Device connected.", color="green", flush=True)


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
    cprint(f"Device disconnected because: {reason}.", color="red", flush=True)


def main(cb_loop: asyncio.AbstractEventLoop, test_cfg: TestCfg):
    """
    Generate the device and run the end to end tests.
    """
    persistency_dir = Path.joinpath(Path.cwd(), "e2etest", "base", "build")
    if not Path.is_dir(persistency_dir):
        os.makedirs(persistency_dir)
    device = DeviceMqtt(
        device_id=test_cfg.device_id,
        realm=test_cfg.realm,
        credentials_secret=test_cfg.credentials_secret,
        pairing_base_url=test_cfg.pairing_url,
        persistency_dir=persistency_dir,
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

    test_datastream_from_device_to_server(device, test_cfg)

    time.sleep(1)

    test_datastream_from_server_to_device(test_cfg, rx_data_lock, rx_data)

    time.sleep(1)

    test_aggregate_from_device_to_server(device, test_cfg)

    time.sleep(1)

    test_aggregate_from_server_to_device(test_cfg, rx_data_lock, rx_data)

    time.sleep(1)

    test_properties_from_device_to_server(device, test_cfg)

    time.sleep(1)

    test_properties_from_server_to_device(test_cfg, rx_data_lock, rx_data)


def start_call_back_loop(loop: asyncio.AbstractEventLoop) -> None:
    """
    Start an asyncio event loop, used for the device call back.
    """
    asyncio.set_event_loop(loop)
    loop.run_forever()


if __name__ == "__main__":
    # Generate an async loop and thread
    call_back_loop = asyncio.new_event_loop()
    call_back_thread = Thread(target=start_call_back_loop, args=[call_back_loop], daemon=True)
    call_back_thread.start()

    try:
        main(call_back_loop, TestCfg(number=1))
    except Exception as e:
        call_back_loop.stop()
        call_back_thread.join(timeout=1)
        raise e
