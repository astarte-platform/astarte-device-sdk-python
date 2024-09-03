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

from __future__ import annotations

import asyncio
import collections.abc
import json
import logging
from abc import ABC, abstractmethod
from collections.abc import Callable
from datetime import datetime
from enum import Enum
from pathlib import Path

from astarte.device.exceptions import (
    InterfaceFileDecodeError,
    InterfaceFileNotFoundError,
    InterfaceNotFoundError,
    ValidationError,
)
from astarte.device.interface import Interface
from astarte.device.introspection import Introspection


class ConnectionState(Enum):
    """
    Possible connection states for a device.
    """

    CONNECTING = 1
    CONNECTED = 2
    DISCONNECTED = 3


class Device(ABC):
    """
    Abstract class defining the minumum APIs for an Astarte device implementation.

    This class is agnostic of a transport layer. It should be used to implement transport specific
    children classes.
    """

    @abstractmethod
    def __init__(self):
        self._introspection = Introspection()
        self._on_connected: Callable[[Device], None] | None = None
        self._on_data_received: Callable[[Device, str, str, object], None] | None = None
        self._on_disconnected: Callable[[Device, int], None] | None = None
        self._loop = None

    @abstractmethod
    def add_interface_from_json(self, interface_json: dict):
        """
        Adds an interface to the device.

        Parameters
        ----------
        interface_json : dict
            Description of the interface obtained through `json.loads()` or similar methods.
        """

    def add_interface_from_file(self, interface_file: Path):
        """
        Adds an interface to the device, from a json file.

        It has to be called before :py:func:`connect`, as it will be used for building the device
        introspection.

        Parameters
        ----------
        interface_file : Path
            An absolute path to an Astarte interface json file.

        Raises
        ------
        InterfaceFileNotFoundError
            If specified file does not exists.
        InterfaceFileDecodeError
            If specified file is not a valid json file.
        """
        if not interface_file.is_file():
            raise InterfaceFileNotFoundError(f'"{interface_file}" does not exist or is not a file')
        with open(interface_file, "r", encoding="utf-8") as interface_fp:
            try:
                self.add_interface_from_json(json.load(interface_fp))
            except json.JSONDecodeError as exc:
                raise InterfaceFileDecodeError(
                    f'"{interface_file}" is not a parsable json file'
                ) from exc

    def add_interfaces_from_dir(self, interfaces_dir: Path):
        """
        Adds a series of interfaces to the device, from a directory containing json files.

        It has to be called before :py:func:`connect`, as it will be used for building the device
        introspection.

        Parameters
        ----------
        interfaces_dir : Path
            An absolute path to an a folder containing some Astarte interface json files.

        Raises
        ------
        InterfaceFileNotFoundError
            If specified directory does not exists.
        """
        if not interfaces_dir.exists():
            raise InterfaceFileNotFoundError(f'"{interfaces_dir}" does not exist')
        if not interfaces_dir.is_dir():
            raise InterfaceFileNotFoundError(f'"{interfaces_dir}" is not a directory')
        for interface_file in [i for i in interfaces_dir.iterdir() if i.suffix == ".json"]:
            self.add_interface_from_file(interface_file)

    @abstractmethod
    def remove_interface(self, interface_name: str) -> None:
        """
        Removes an Interface from the device.

        Parameters
        ----------
        interface_name : str
            The name of an Interface previously added with one of the `add_interface(s)_from_*`
            functions.
        """

    def set_events_callbacks(
        self,
        on_connected: Callable[[Device], None] | None = None,
        on_data_received: Callable[[Device, str, str, object], None] | None = None,
        on_disconnected: Callable[[Device, int], None] | None = None,
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> None:
        """
        Can be used to set various callbacks to user provided functions.

        note:: All parameters default to None. Meaning that all unspeficied callbacks will be
        disabled. Same for the event loop.

        Parameters
        ----------
        on_connected : Callable[[Device], None] | None
            A function that will be invoked everytime the device is connected.
        on_data_received : Callable[[Device, string, string, object], None] | None
            A function that will be invoked everytime data is received from Astarte. Parameters are
            the device itself, the Interface name, the Interface path, and the payload. The payload
            will reflect the type defined in the Interface.
        on_disconnected : Callable[[Device], None] | None
            A function that will be invoked everytime the device experiences a disconnection event.
            The int parameter bears the disconnect reason. With 0 being a graceful disconnection.
        loop : asyncio.AbstractEventLoop | None
            An optional loop which will be used for invoking the callbacks. When this is not None,
            the device will call any specified callback through loop.call_soon_threadsafe, ensuring
            that the callbacks will be run in thread the loop belongs to. Usually, you want
            to set this to get_running_loop(). When not sent, callbacks will be invoked as a
            standard function - keep in mind this means your callbacks might create deadlocks.
        """
        self._on_connected = on_connected
        self._on_data_received = on_data_received
        self._on_disconnected = on_disconnected
        self._loop = loop

    @abstractmethod
    def connect(self) -> None:
        """
        Connects the device to Astarte.
        """

    @abstractmethod
    def disconnect(self) -> None:
        """
        Disconnects the device from Astarte.
        """

    @abstractmethod
    def is_connected(self) -> bool:
        """
        Returns whether the device is currently connected.

        Returns
        -------
        bool
            The device connection status.
        """

    def send(
        self,
        interface_name: str,
        interface_path: str,
        payload: object,
        timestamp: datetime | None = None,
    ) -> None:
        """
        Sends an individual message to an interface.

        Parameters
        ----------
        interface_name : str
            The name of an the Interface to send data to.
        interface_path : str
            The path on the Interface to send data to.
        payload : object
            The value to be sent. The type should be compatible to the one specified in the
            interface path.
        timestamp : datetime, optional
            If sending a Datastream with explicit_timestamp, you can specify a datetime object
            which will be registered as the timestamp for the value.

        Raises
        ------
        InterfaceNotFoundError
            If the specified interface is not declared in the introspection.
        ValidationError
            If the interface or payload validation was unsuccessful.
        """
        interface = self._introspection.get_interface(interface_name)
        if not interface:
            raise InterfaceNotFoundError(
                f"Interface {interface_name} not declared in introspection"
            )
        if interface.is_server_owned():
            raise ValidationError(f"The interface {interface.name} is not owned by the device.")
        if interface.is_aggregation_object():
            raise ValidationError(
                f"Interface {interface_name} is an aggregate interface. You should use "
                f"send_aggregate."
            )

        if payload is None:
            raise ValidationError("Payload should be different from None")
        if isinstance(payload, collections.abc.Mapping):
            raise ValidationError("Payload for individual interfaces should not be a dictionary")
        interface.validate_payload_and_timestamp(interface_path, payload, timestamp)

        self._send_generic(
            interface,
            interface_path,
            payload,
            timestamp,
        )

    def send_aggregate(
        self,
        interface_name: str,
        interface_path: str,
        payload: collections.abc.Mapping,
        timestamp: datetime | None = None,
    ) -> None:
        """
        Sends an aggregate message to an interface.

        Parameters
        ----------
        interface_name : str
            The name of the Interface to send data to.
        interface_path: str
            The endpoint to send the data to
        payload : dict
            A dictionary containing the path:value map for the aggregate.
        timestamp : datetime, optional
            If the Datastream has explicit_timestamp, you can specify a datetime object which
            will be registered as the timestamp for the value.

        Raises
        ------
        InterfaceNotFoundError
            If the specified interface is not declared in the introspection.
        ValidationError
            If the interface or payload validation was unsuccessful.
        """
        interface = self._introspection.get_interface(interface_name)
        if not interface:
            raise InterfaceNotFoundError(
                f"Interface {interface_name} not declared in introspection"
            )
        if interface.is_server_owned():
            raise ValidationError(f"The interface {interface.name} is not owned by the device.")
        if not interface.is_aggregation_object():
            raise ValidationError(
                f"Interface {interface_name} is not an aggregate interface. You should use send."
            )

        if payload is None:
            raise ValidationError("Payload should be different from None")
        if not isinstance(payload, collections.abc.Mapping):
            raise ValidationError("Payload for aggregate interfaces should be a dictionary")
        interface.validate_payload_and_timestamp(interface_path, payload, timestamp)

        self._send_generic(
            interface,
            interface_path,
            payload,
            timestamp,
        )

    def unset_property(self, interface_name: str, interface_path: str) -> None:
        """
        Unset the specified property on an interface.

        Parameters
        ----------
        interface_name : str
            The name of the Interface where the property to unset is located.
        interface_path : str
            The path on the Interface to unset.

        Raises
        ------
        InterfaceNotFoundError
            If the specified interface is not declared in the introspection.
        ValidationError
            If the interface validation was unsuccessful.
        """

        interface = self._introspection.get_interface(interface_name)
        if not interface:
            raise InterfaceNotFoundError(
                f"Interface {interface_name} not declared in introspection"
            )
        if interface.is_server_owned():
            raise ValidationError(f"The interface {interface.name} is not owned by the device.")
        if not interface.is_type_properties():
            raise ValidationError(
                f"Interface {interface_name} is a datastream interface. You can only unset a "
                f"property."
            )

        self._send_generic(
            interface,
            interface_path,
            None,
            None,
        )

    @abstractmethod
    def _send_generic(
        self,
        interface: Interface,
        path: str,
        payload: object | collections.abc.Mapping | None,
        timestamp: datetime | None,
    ) -> None:
        """
        Utility function used to publish a generic payload to an Astarte interface.

        Parameters
        ----------
        interface : Interface
            The Interface to send data to.
        path: str
            The endpoint to send the data to
        payload : object, collections.abc.Mapping, optional
            The payload to send if present.
        timestamp : datetime, optional
            If the Datastream has explicit_timestamp, you can specify a datetime object which
            will be registered as the timestamp for the value.
        """

    def _on_message_generic(self, interface_name, path, payload):
        """
        Called each time a message has been received by the transport layer.

        Parameters
        ----------
        interface_name: str
            Interface name for the payload.
        path: str
            Path on which the payload has been received.
        payload: object | collections.abc.Mapping | None
            Payload to process.
        """

        # Check if interface name is correct
        interface = self._introspection.get_interface(interface_name)
        if not interface:
            logging.warning(
                "Received unexpected message for unregistered interface %s: %s, %s",
                interface_name,
                path,
                payload,
            )
            return

        # Check over ownership of the interface
        if not interface.is_server_owned():
            logging.warning(
                "Received unexpected message for device owned interface %s: %s, %s",
                interface_name,
                path,
                payload,
            )
            return

        # Ensure that an empty payload is only for resettable properties
        if (payload is None) and (not interface.is_property_endpoint_resettable(path)):
            logging.warning(
                "Received empty payload for non property interface %s or non resettable %s endpoint",
                interface_name,
                path,
            )
            return

        # Check the received path corresponds to the one in the interface
        try:
            interface.validate_path(path, payload)
        except ValidationError as val_err:
            logging.warning("Validation error: %s", val_err)
            logging.warning(
                "Received message on incorrect endpoint for interface %s, path %s, payload %s.",
                interface_name,
                path,
                payload,
            )
            return

        # Check the payload matches with the interface
        if payload:
            try:
                interface.validate_payload(path, payload)
            except ValidationError as val_err:
                logging.warning("Validation error: %s", val_err)
                logging.warning(
                    "Received incompatible payload for interface %s, path %s, payload %s.",
                    interface_name,
                    path,
                    payload,
                )
                return

        self._store_property(interface, path, payload)

        if self._loop:
            # Use threadsafe, as we're in a different thread here
            self._loop.call_soon_threadsafe(
                self._on_data_received,
                self,
                interface_name,
                path,
                payload,
            )
        else:
            self._on_data_received(self, interface_name, path, payload)

    @abstractmethod
    def _store_property(
        self,
        interface: Interface,
        path: str,
        payload: object | collections.abc.Mapping | None,
    ) -> None:
        """
        Store the property in the properties database.

        Parameters
        ----------
        interface: Interface
            Interface to use for property store.
        path: str
            Path to use for property store.
        payload: object | collections.abc.Mapping | None
            Payload to store.
        """
