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
API for an SQLite database to be used for Astarte properties persistency.
"""

from __future__ import annotations

import pickle
import sqlite3
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from grpc import logging

from astarte.device.interface import InterfaceOwnership
from astarte.device.types import TypeAstarteData


def ownership_sqlite_set() -> str:
    """
    Construct a string representing the values stored in the InterfaceOwnership enum as an sqlite set.

    Returns
    -------
    str
        Representation of all the possible values of this enum as a sqlite set string.
    """
    return "(" + ",".join(["'" + e.value + "'" for e in InterfaceOwnership]) + ")"


record_ownership_value_set: str = ownership_sqlite_set()


class StoredProperty:
    """
    Allows access to data of a stored property, returned by the PropertyAccess class
    """

    interface: str
    path: str
    major: int
    ownership: InterfaceOwnership
    value: TypeAstarteData

    def __init__(
        self,
        interface: str,
        path: str,
        major: int,
        ownership: InterfaceOwnership,
        value: TypeAstarteData,
    ):
        self.interface = interface
        self.path = path
        self.major = major
        self.ownership = ownership
        self.value = value


class AstarteDatabase(ABC):
    """
    Abstract class for a database that can be used to provide persistency to the properties.
    """

    @abstractmethod
    def store_prop(
        self,
        interface: str,
        major: int,
        path: str,
        ownership: InterfaceOwnership,
        value: TypeAstarteData | None,
    ):
        """
        Store a property value in the database. It will overwrite the previous value where present.

        Parameters
        ----------
        interface : str
            The interface name.
        major : int
            The path to the property endpoint.
        path : str
            The path to the property endpoint.
        ownership : InterfaceOwnership
            The ownership of the interface.
        value : object
            The new value for the property.
        """

    @abstractmethod
    def load_prop(self, interface: str, major: int, path: str) -> StoredProperty | None:
        """
        Load a property from the database. If a property is found but the major version does not
        match, the property in the database will be deleted and None will be returned.

        Parameters
        ----------
        interface : str
            The interface name.
        major : int
            The major version for the interface.
        path : str
            The path to the property endpoint.

        Returns
        -------
        StoredProperty | None
            The property value if the property is present and the provided interface major
            version matches the interface version stored in the database. None otherwise.
        """

    @abstractmethod
    def delete_prop(self, interface: str, path: str):
        """
        Delete a property from the database.

        Parameters
        ----------
        interface : str
            The interface name.
        path : str
            The path to the property endpoint.
        """

    @abstractmethod
    def delete_props_from_interface(self, interface: str):
        """
        Delete all the properties from the database belonging to an interface.

        Parameters
        ----------
        interface : str
            The interface name.
        """

    @abstractmethod
    def clear(self) -> None:
        """
        Fully clear the database of all the properties.
        """

    @abstractmethod
    def load_interface_props(self, interface: str) -> list[StoredProperty]:
        """
        Load all the properties of an interface stored in the database.

        Parameters
        ----------
        interface : str
            The interface name.

        Returns
        -------
        list[StoredProperty]
            A list containing the propeties of the specified interface stored in the database.
        """

    @abstractmethod
    def load_device_props(self) -> list[StoredProperty]:
        """
        Load all the device properties stored in the database.

        Returns
        -------
        list[StoredProperty]
            A list containing the device propeties stored in the database.
        """

    @abstractmethod
    def load_server_props(self) -> list[StoredProperty]:
        """
        Load all the server properties stored in the database.

        Returns
        -------
        list[StoredProperty]
            A list containing the server propeties stored in the database.
        """

    @abstractmethod
    def load_all_props(self) -> list[StoredProperty]:
        """
        Load all the properties stored in the database.

        Returns
        -------
        list[StoredProperty]
            A list containing all the propeties stored in the database.
        """


class AstarteDatabaseSQLite(AstarteDatabase):
    """
    An implementation for the abstract AstarteDatabase class. This implementation uses the standard
    SQLite library of Python to implement property persistency.
    """

    def __init__(self, database_path: Path) -> None:
        """
        Parameters
        ----------
        database_path : Path
            The path to the file to use to instantiate the database.
        """
        self.__database_path = database_path
        cursor = sqlite3.connect(self.__database_path).cursor()
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS properties ("
            "interface TEXT NOT NULL,"
            "major INTEGER NOT NULL,"
            "path TEXT NOT NULL,"
            "ownership CHARACTER(6) CHECK(ownership IN "
            + record_ownership_value_set
            + ") NOT NULL,"
            "value BLOB NOT NULL,"
            "PRIMARY KEY (interface, path))"
        )

    def store_prop(
        self,
        interface: str,
        major: int,
        path: str,
        ownership: InterfaceOwnership,
        value: TypeAstarteData | None,
    ):
        """
        Store a property value in the database. It will overwrite the previous value where present.

        Parameters
        ----------
        interface : str
            See documentation in AstarteDatabase.
        major : int
            See documentation in AstarteDatabase.
        path : str
            See documentation in AstarteDatabase.
        ownership : InterfaceOwnership
            See documentation in AstarteDatabase.
        value : object
            See documentation in AstarteDatabase.
        """
        if not value:
            self.delete_prop(interface, path)
        else:
            connection = sqlite3.connect(self.__database_path)
            connection.cursor().execute(
                "INSERT OR REPLACE INTO properties (interface, major, path, ownership, value) VALUES "
                "(?, ?, ?, ?, ?)",
                (
                    interface,
                    major,
                    path,
                    ownership.value,
                    pickle.dumps(value),
                ),
            )
            connection.commit()

    def load_prop(self, interface: str, major: int, path: str) -> StoredProperty | None:
        """
        Load a property from the database. If a property is found but the major version does not
        match, the property in the database will be deleted and None will be returned.

        Parameters
        ----------
        interface : str
            See documentation in AstarteDatabase.
        major : int
            See documentation in AstarteDatabase.
        path : str
            See documentation in AstarteDatabase.

        Returns
        -------
        TypeAstarteData | None
            See documentation in AstarteDatabase.
        """
        query_result = (
            sqlite3.connect(self.__database_path)
            .cursor()
            .execute(
                "SELECT ownership, value, major FROM properties WHERE interface=? AND path=?",
                (interface, path),
            )
            .fetchone()
        )

        if query_result is None:
            return None

        ownership, value, stored_major = query_result

        if value is None:
            return None

        # if version mismatch, delete the old value
        if stored_major != major:
            self.delete_prop(interface, path)
            return None

        return StoredProperty(
            interface, path, major, InterfaceOwnership(ownership), pickle.loads(value)
        )

    def delete_prop(self, interface: str, path: str):
        """
        Delete a property from the database.

        Parameters
        ----------
        interface : str
            See documentation in AstarteDatabase.
        path : str
            See documentation in AstarteDatabase.
        """
        connection = sqlite3.connect(self.__database_path)
        connection.cursor().execute(
            "DELETE FROM properties WHERE interface=? AND path=?",
            (interface, path),
        )
        connection.commit()

    def delete_props_from_interface(self, interface: str):
        """
        Delete all the properties from the database belonging to an interface.

        Parameters
        ----------
        interface : str
            See documentation in AstarteDatabase.
        """
        connection = sqlite3.connect(self.__database_path)
        connection.cursor().execute(
            "DELETE FROM properties WHERE interface=?",
            (interface,),
        )
        connection.commit()

    def clear(self):
        """
        Fully clear the database of all the properties.
        """
        connection = sqlite3.connect(self.__database_path)
        connection.cursor().execute("DELETE * FROM properties")
        connection.commit()

    def load_interface_props(self, interface: str) -> list[StoredProperty]:
        """
        Parameters
        ----------
        interface : str
            See documentation in AstarteDatabase.

        Returns
        -------
        list[StoredProperty]
            See documentation in AstarteDatabase.
        """
        properties = (
            sqlite3.connect(self.__database_path)
            .cursor()
            .execute(
                "SELECT interface, major, path, ownership, value FROM properties WHERE interface = ?",
                (interface,),
            )
            .fetchall()
        )
        return _to_property_data_list(properties)

    def load_device_props(self) -> list[StoredProperty]:
        """
        Returns
        -------
        list[StoredProperty]
            See documentation in AstarteDatabase.
        """
        properties = (
            sqlite3.connect(self.__database_path)
            .cursor()
            .execute(
                "SELECT interface, major, path, ownership, value FROM properties WHERE ownership = ?",
                (InterfaceOwnership.DEVICE.value,),
            )
            .fetchall()
        )
        return _to_property_data_list(properties)

    def load_server_props(self) -> list[StoredProperty]:
        """
        Returns
        -------
        list[StoredProperty]
            See documentation in AstarteDatabase.
        """
        properties = (
            sqlite3.connect(self.__database_path)
            .cursor()
            .execute(
                "SELECT interface, major, path, ownership, value FROM properties WHERE ownership = ?",
                (InterfaceOwnership.SERVER.value,),
            )
            .fetchall()
        )
        return _to_property_data_list(properties)

    def load_all_props(self) -> list[StoredProperty]:
        """
        Load all the properties stored in the database.

        Returns
        -------
        list[StoredProperty]
            See documentation in AstarteDatabase.
        """
        properties = (
            sqlite3.connect(self.__database_path)
            .cursor()
            .execute("SELECT interface, major, path, ownership, value FROM properties")
            .fetchall()
        )
        return _to_property_data_list(properties)


def _to_property_data_list(properties: list[Any]) -> list[StoredProperty]:
    """
    Maps the list of properties returned by the sqlite metho to a list of
    StoredProperty.

    Parameters
    ----------
    properties : list[Any]
        List of properties as returned from the sqlite database

    Returns
    -------
    list[StoredProperty]
        Mapped list of properties.

    Raises
    ------
    ValueError
        The ownership stored in the record is not valid. A possible corrupted database state.
    """
    parsed_properties = []
    for interface, major, path, ownership, value in properties:
        try:
            interface_ownership = InterfaceOwnership(ownership)
        except ValueError as e:
            logging.error(f"Incorrect value stored in the database: {ownership}")
            raise e

        parsed_properties += [
            StoredProperty(
                interface,
                path,
                major,
                interface_ownership,
                pickle.loads(value),
            )
        ]
    return parsed_properties
