# This file is part of Astarte.
#
# Copyright 2024 SECO Mind Srl
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

""" Astarte device example using the MQTT protocol

Example showing how to send/receive individual/aggregated datastreams and set/unset properties.

"""

import asyncio
import tempfile
import time
import tomllib
from pathlib import Path
from threading import Thread
from typing import Optional, Tuple

from transmit_data import (
    set_properties,
    stream_aggregates,
    stream_individuals,
    unset_properties,
)

from astarte.device import DeviceMqtt

_INTERFACES_DIR = Path(__file__).parent.joinpath("interfaces").absolute()
_CONFIGURATION_FILE = Path(__file__).parent.joinpath("config.toml").absolute()


def on_connected_cbk(_):
    """
    Callback for a connection event.
    """
    print("Device connected.")


def on_data_received_cbk(_: DeviceMqtt, interface_name: str, path: str, payload: dict):
    """
    Callback for a data reception event.
    """
    print(f"Received message for interface: {interface_name} and path: {path}.")
    print(f"    Payload: {payload}")


def on_disconnected_cbk(_, reason: int):
    """
    Callback for a disconnection event.
    """
    print("Device disconnected" + (f" because: {reason}." if reason else "."))


def _start_background_loop(_loop: asyncio.AbstractEventLoop) -> None:
    asyncio.set_event_loop(_loop)
    _loop.run_forever()


def _generate_async_loop() -> Tuple[asyncio.AbstractEventLoop, Thread]:
    _loop = asyncio.new_event_loop()
    other_thread = Thread(target=_start_background_loop, args=(_loop,), daemon=True)
    other_thread.start()
    return _loop, other_thread


def main(cb_loop: Optional[asyncio.AbstractEventLoop] = None):

    with open(_CONFIGURATION_FILE, "rb") as config_fp:
        config = tomllib.load(config_fp)
        _DEVICE_ID = config["DEVICE_ID"]
        _REALM = config["REALM"]
        _CREDENTIALS_SECRET = config["CREDENTIALS_SECRET"]
        _PAIRING_URL = config["PAIRING_URL"]
        _STREAM_INDIVIDUAL_DATA = config.get("STREAM_INDIVIDUAL_DATA", True)
        _STREAM_AGGREGATED_DATA = config.get("STREAM_AGGREGATED_DATA", True)
        _SET_PROPERTIES = config.get("SET_PROPERTIES", True)
        _UNSET_PROPERTIES = config.get("UNSET_PROPERTIES", True)
        _WAIT_FOR_INCOMING_S = config.get("WAIT_FOR_INCOMING_S", 0)

    # Creating a temporary directory
    with tempfile.TemporaryDirectory(prefix="python_sdk_examples_") as temp_dir:

        print("Creating and connecting the device.")
        # Instantiate the device
        device = DeviceMqtt(
            device_id=_DEVICE_ID,
            realm=_REALM,
            credentials_secret=_CREDENTIALS_SECRET,
            pairing_base_url=_PAIRING_URL,
            persistency_dir=temp_dir,
        )
        # Load all the interfaces
        device.add_interfaces_from_dir(_INTERFACES_DIR)
        # Set all the callback functions
        device.set_events_callbacks(
            on_connected=on_connected_cbk,
            on_data_received=on_data_received_cbk,
            on_disconnected=on_disconnected_cbk,
            loop=cb_loop,
        )
        # Connect the device
        device.connect()
        while not device.is_connected():
            pass

        time.sleep(1)

        if _STREAM_INDIVIDUAL_DATA:
            print("Streaming individual data.")
            stream_individuals(device)

        if _STREAM_AGGREGATED_DATA:
            print("Streaming aggregated data.")
            stream_aggregates(device)

        if _SET_PROPERTIES:
            print("Setting properties data.")
            set_properties(device)

        if _UNSET_PROPERTIES:
            print("Unsetting properties data.")
            unset_properties(device)

        print(f"Waiting {_WAIT_FOR_INCOMING_S} seconds for server data.")
        time.sleep(_WAIT_FOR_INCOMING_S)

        print("Disconnecting the device.")
        device.disconnect()


# If called as a script
if __name__ == "__main__":

    # [Optional] Preparing a different asyncio loop for the callbacks to prevent deadlocks
    # Replace with loop = None to run the Astarte event callback in the main thread
    print("Generating async loop.")
    (loop, thread) = _generate_async_loop()
    main(loop)
    loop.call_soon_threadsafe(loop.stop)
    print("Requested async loop stop.")
    thread.join()
    print("Async loop stopped.")
