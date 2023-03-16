# Copyright 2023 SECO Mind S.r.l.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
End to end testing framework.
"""
import os
import time
import asyncio
from pathlib import Path
from threading import Thread, Lock
from termcolor import cprint

from astarte.device import Device

from e2etest.config import TestCfg

from e2etest.datastream import test_datastream_from_device_to_server
from e2etest.datastream import test_datastream_from_server_to_device

from e2etest.property import test_properties_from_device_to_server
from e2etest.property import test_properties_from_server_to_device

from e2etest.aggregate import test_aggregate_from_device_to_server
from e2etest.aggregate import test_aggregate_from_server_to_device

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
    persistency_dir = Path.joinpath(Path.cwd(), "e2etest", "build")
    if not Path.is_dir(persistency_dir):
        os.makedirs(persistency_dir)
    device = Device(
        device_id=test_cfg.device_id,
        realm=test_cfg.realm,
        credentials_secret=test_cfg.credentials_secret,
        pairing_base_url=test_cfg.pairing_url,
        persistency_dir=persistency_dir,
        loop=cb_loop,
        ignore_ssl_errors=False,
    )
    device.add_interfaces_from_dir(test_cfg.interfaces_fld)
    device.on_connected = on_connected_cbk
    device.on_data_received = on_data_received_cbk
    device.on_disconnected = on_disconnected_cbk
    device.connect()

    time.sleep(1)

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
        main(call_back_loop, TestCfg())
    except Exception as e:
        call_back_loop.stop()
        call_back_thread.join(timeout=1)
        raise e
