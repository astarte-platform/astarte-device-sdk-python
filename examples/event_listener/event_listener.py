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

""" Device event listener example

This module shows an example usage of the Astarte device SDK.
Here we show how to simply connect your device to Astarte and start listening on events on a
server-owned interface.

"""
import asyncio
import signal
import tempfile
from pathlib import Path
from threading import Thread
from time import sleep
from typing import Optional, Tuple

from astarte.device import DeviceMqtt

_ROOT_DIR = Path(__file__).parent.absolute()
_INTERFACES_DIR = _ROOT_DIR.joinpath("interfaces")
_INTERFACE_FILE = _INTERFACES_DIR.joinpath("org.astarte-platform.genericsensors.SamplingRate.json")
_DEVICE_ID = "DEVICE_ID_HERE"
_REALM = "REALM_HERE"
_CREDENTIAL_SECRET = "CREDENTIAL_SECRET_HERE"
_PAIRING_URL = "https://api.astarte.EXAMPLE.COM/pairing"
_PERSISTENCY_DIR = tempfile.gettempdir()


class ProgramKilled(Exception):
    pass


def _signal_handler(signum, frame):
    print("Shutting Down...")
    raise ProgramKilled


def _start_background_loop(_loop: asyncio.AbstractEventLoop) -> None:
    asyncio.set_event_loop(_loop)
    _loop.run_forever()


def _generate_async_loop() -> Tuple[asyncio.AbstractEventLoop, Thread]:
    _loop = asyncio.new_event_loop()
    other_thread = Thread(target=_start_background_loop, args=(_loop,), daemon=True)
    other_thread.start()
    return _loop, other_thread


def callback(device: DeviceMqtt, interface_name: str, path: str, payload: object) -> None:
    """
    A function where we are going to handle the Astarte events triggered by server-owned interface
    updates.
    N.B. Depending if how the loop variable has been instanced this function could be run on another
    thread than the main.

    Parameters
    ----------
    device: Device
        The Astarte device whose event is registered to
    interface_name: str
        The name of the server-owned interface where the event was triggered
    path: Str
        Path to the property/datastream  that triggered the event
    payload:
        New Value of the property/datastream

    """
    print(
        f"[device_id: {device.get_device_id()}] Received message for {interface_name}{path}:"
        f" {payload}"
    )


def main(cb_loop: Optional[asyncio.AbstractEventLoop] = None):
    """
    Main function
    """

    # Instance the device
    device = DeviceMqtt(
        device_id=_DEVICE_ID,
        realm=_REALM,
        credentials_secret=_CREDENTIAL_SECRET,
        pairing_base_url=_PAIRING_URL,
        persistency_dir=_PERSISTENCY_DIR,
    )
    # Load all the interfaces
    device.add_interface_from_file(_INTERFACE_FILE)

    # Connect the device
    device.connect()
    # Attach the callback
    device.set_events_callbacks(on_data_received=callback, loop=cb_loop)

    print("Initialization completed, waiting for messages")
    while True:
        # We have to keep alive the main loop
        sleep(5)


if __name__ == "__main__":
    # [Optional] Preparing a different asyncio loop for the callbacks to prevent deadlocks
    # Replace with loop = None to run the Astarte event callback in the main thread
    (loop, thread) = _generate_async_loop()
    try:
        signal.signal(signal.SIGTERM, _signal_handler)
        signal.signal(signal.SIGINT, _signal_handler)
        main(loop)
    except ProgramKilled:
        if loop:
            loop.call_soon_threadsafe(loop.stop)
            print("Requested async loop stop")
            thread.join()
        print("Stopped")
