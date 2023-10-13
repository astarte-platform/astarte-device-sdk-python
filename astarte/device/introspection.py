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

from astarte.device.interface import Interface


class Introspection:
    """
    Class that represent the introspection of a device.

    The introspection is the list od interfaces that the device declares to the server it is
    compatible with.

    In any given time a device can have a single interface with a given name, multiple interfaces
    with the same name but different major/minor are not supported.
    """

    def __init__(self):
        self.__interfaces_list = {}

    def add_interface(self, interface: Interface) -> None:
        """
        Adds an interface to the introspection

        This will add an interface to the device.

        Parameters
        ----------
        interface : Interface
            An Astarte interface object. Usually obtained by using json.loads() on an interface
            file and then using the loaded json to initialize the interface object.
        """
        self.__interfaces_list[interface.name] = interface

    def remove_interface(self, interface_name: str) -> None:
        """
        Removes an Interface from the Introspection

        Removes an Interface definition from the device. It has to be called before
        :py:func:`connect`, as it will be used for building the device Introspection.

        Parameters
        ----------
        interface_name : str
            The name of an Interface previously added with :py:func:`add_interface`.
        """
        if interface_name in self.__interfaces_list:
            del self.__interfaces_list[interface_name]

    def get_interface(self, interface_name: str) -> Interface | None:
        """
        Retrieve an Interface definition from the Introspection

        Parameters
        ----------
        interface_name : str
            The name of an Interface previously added with :py:func:`add_interface`.

        Returns
        -------
        Interface or None
            the Interface definition if found in the Introspection, None otherwise
        """
        if interface_name in self.__interfaces_list:
            return self.__interfaces_list[interface_name]

        return None

    def get_all_interfaces(self) -> list[Interface]:
        """
        Retrieve all the list of all Interfaces in device's Introspection

        Returns
        -------
        list
            The list of all Interfaces in the Introspection
        """
        return self.__interfaces_list.values()

    def get_all_server_owned_interfaces(self) -> list[Interface]:
        """
        Retrieve all the list of all Interfaces in device's Introspection with server ownership

        Returns
        -------
        list
            The list of all Interfaces in the Introspection that have ownership "server"
        """
        return list(
            filter(
                lambda interface: interface.is_server_owned(),
                self.__interfaces_list.values(),
            )
        )
