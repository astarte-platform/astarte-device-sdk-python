""" Device Publication Example

This module shows an example usage of the Astarte device SDK.
Here we show how to simply connect your device to Astarte to start publishing on various
interfaces.
All the interfaces we are going to use are located in the `interface` directory and are used as
follows:
1. AvailableSensors: to publish single properties
2. Values: to publish single datastreams
3. Geolocation: to publish an object aggregated datastream

"""
import json
import os
import signal
import tempfile
from datetime import datetime, timezone
from os import listdir
from os.path import join, isfile
from pathlib import Path
from random import random
from time import sleep

from astarte.device import Device

_ROOT_DIR = Path(__file__).parent.absolute()
_INTERFACES_DIR = os.path.join(_ROOT_DIR, 'interfaces')
_DEVICE_ID = 'DEVICE_ID_HERE'
_REALM = 'REALM_HERE'
_CREDENTIAL_SECRET = 'CREDENTIAL_SECRET_HERE'
_PAIRING_URL = 'https://api.astarte.EXAMPLE.COM/pairing'
_PERSISTENCY_DIR = tempfile.gettempdir()


class ProgramKilled(Exception):
    pass


def signal_handler(signum, frame):
    print("Shutting Down...")
    raise ProgramKilled


def _load_interfaces() -> [dict]:
    files = [join(_INTERFACES_DIR, f) for f in listdir(_INTERFACES_DIR)
             if isfile(join(_INTERFACES_DIR, f))]
    interfaces = []
    for file in files:
        with open(file, 'r') as interface_file:
            interfaces.append(json.load(interface_file))
    return interfaces


def main():
    """
    Main function
    """

    # Instance the device
    device = Device(device_id=_DEVICE_ID, realm=_REALM, credentials_secret=_CREDENTIAL_SECRET,
                    pairing_base_url=_PAIRING_URL, persistency_dir=_PERSISTENCY_DIR)
    # Load all the interfaces
    for interface in _load_interfaces():
        device.add_interface(interface)
    # Connect the device
    device.connect()

    # Set properties
    sensor_id = "b2c5a6ed-ebe4-4c5c-9d8a-6d2f114fc6e5"
    device.send("org.astarte-platform.genericsensors.AvailableSensors", f"/{sensor_id}/interface_name",
                "randomThermometer")
    device.send("org.astarte-platform.genericsensors.AvailableSensors", f"/{sensor_id}/unit", "°C")

    # Unset property
    device.send("org.astarte-platform.genericsensors.AvailableSensors", "/wrongId/interface_name",
                "randomThermometer")
    device.unset_property("org.astarte-platform.genericsensors.AvailableSensors",
                          "/wrongId/interface_name")

    max_temp = 30
    while True:
        # Send single datastream
        temp = round(random() * max_temp, 2)
        device.send("org.astarte-platform.genericsensors.Values", f"/{sensor_id}/value", temp,
                    datetime.now(tz=timezone.utc))

        # Send object aggregated datastream
        geo_data = {
            "accuracy": 1.0,
            "altitude": 331.81,
            "altitudeAccuracy": 1.0,
            "heading": 0,
            "latitude": 43.32215,
            "longitude": 11.3259,
            "speed": 0,
        }
        device.send_aggregate("org.astarte-platform.genericsensors.Geolocation", "/gps", geo_data)

        sleep(5)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    main()
