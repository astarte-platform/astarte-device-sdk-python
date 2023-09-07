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

from abc import ABC, abstractmethod
import collections.abc
import json
from pathlib import Path
from datetime import datetime

from astarte.device.interface import Interface
from astarte.device.introspection import Introspection
from astarte.device.exceptions import (
    ValidationError,
    InterfaceNotFoundError,
    InterfaceFileNotFoundError,
)
from astarte.device.exceptions import (
    InterfaceFileDecodeError,
)


class Device(ABC):
    """
    Generic device abstract class.
    Can be used as a template to implement devices with different transpot protocols.
    """

    @abstractmethod
    def __init__(self, *args):
        """
        Parameters
        ----------
        args :
            TODO.
        """
        self._introspection = Introspection()

    @abstractmethod
    def _add_interface_from_json(self, interface_json: json):
        """
        Adds an interface to the device

        Parameters
        ----------
        interface_json : json
            json file containing the interface description.
        """

    def add_interface_from_file(self, interface_file: Path):
        """
        Adds an interface to the device

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
                self._add_interface_from_json(json.load(interface_fp))
            except json.JSONDecodeError as exc:
                raise InterfaceFileDecodeError(
                    f'"{interface_file}" is not a parsable json file'
                ) from exc

    def add_interfaces_from_dir(self, interfaces_dir: Path):
        """
        Adds a series of interfaces to the device

        This will add all the interfaces contained in the provided folder to the device.
        It has to be called before :py:func:`connect`, as it will be used for building the device
        introspection.

        Parameters
        ----------
        interfaces_dir : Path
            An absolute path to an a folder containing some Astarte interface .json files.
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
        Removes an Interface from the device

        Removes an Interface definition from the device. It has to be called before
        :py:func:`connect`, as it will be used for building the device introspection.

        Parameters
        ----------
        interface_name : str
            The name of an Interface previously added with :py:func:`add_interface`.
        """

    @abstractmethod
    def connect(self) -> None:
        """
        Connects the device.
        """

    @abstractmethod
    def disconnect(self) -> None:
        """
        Disconnects the device.
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
            If the interface is not declared in the introspection
        ValidationError
            If the interface or the payload are not compatible.
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
        validation_result = interface.validate(interface_path, payload, timestamp)
        if validation_result:
            raise validation_result

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
        Sends an aggregate message to an interface

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
            If the interface is not declared in the introspection
        ValidationError
            If the interface or the payload are not compatible.
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
        validation_result = interface.validate(interface_path, payload, timestamp)
        if validation_result:
            raise validation_result

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
            If the interface is not declared in the introspection
        ValidationError
            If the interface is of type datastream.
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
