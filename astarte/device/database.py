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


class AstarteDatabase(ABC):
    """
    Abstract class for a database that can be used to provide persistency to the properties.
    """

    @abstractmethod
    def store_prop(self, interface: str, major: int, path: str, value: object) -> None:
        """
        Store a property value in the database. It will overwrite the previous value where present.

        Parameters
        ----------
        interface : str
            The interface name.
        major : int
            The major version for the interface.
        path : str
            The path to the property endpoint.
        value : object
            The new value for the property.
        """

    @abstractmethod
    def load_prop(self, interface: str, major: int, path: str) -> object | None:
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
        object | None
            The property value if the property is present and the provided interface major
            version matches the interface version stored in the database. None otherwise.
        """

    @abstractmethod
    def delete_prop(self, interface: str, path: str) -> None:
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
    def delete_props_from_interface(self, interface: str) -> None:
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
    def load_all_props(self) -> list[tuple[str, int, str, object]]:
        """
        Load all the properties stored in the database.

        Returns
        -------
        list[tuple[str, int, str, object]]
            A list containing all the propeties stored in the database.
            Each element of the list is a tuple in the format:
            (interface, interface major version, path, value)
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
            "CREATE TABLE IF NOT EXISTS properties "
            "(interface TEXT NOT NULL, major INTEGER NOT NULL, "
            "path TEXT NOT NULL, value BLOB NOT NULL, PRIMARY KEY (interface, path))"
        )

    def store_prop(self, interface: str, major: int, path: str, value: object | None) -> None:
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
        value : object
            See documentation in AstarteDatabase.
        """
        if not value:
            self.delete_prop(interface, path)
        else:
            connection = sqlite3.connect(self.__database_path)
            connection.cursor().execute(
                "INSERT OR REPLACE INTO properties (interface, major, path, value) VALUES "
                "(?, ?, ?, ?)",
                (interface, major, path, pickle.dumps(value)),
            )
            connection.commit()

    def load_prop(self, interface: str, major: int, path: str) -> object | None:
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
        object | None
            See documentation in AstarteDatabase.
        """
        value, stored_major = (
            sqlite3.connect(self.__database_path)
            .cursor()
            .execute(
                "SELECT value, major FROM properties WHERE interface=? AND path=?",
                (interface, path),
            )
            .fetchone()
        )

        # if version mismatch, delete the old value
        if stored_major != major:
            self.delete_prop(interface, path)
            return None

        return pickle.loads(value)

    def delete_prop(self, interface: str, path: str) -> None:
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

    def delete_props_from_interface(self, interface: str) -> None:
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

    def clear(self) -> None:
        """
        Fully clear the database of all the properties.
        """
        connection = sqlite3.connect(self.__database_path)
        connection.cursor().execute("DELETE * FROM properties")
        connection.commit()

    def load_all_props(self) -> list[tuple[str, int, str, object]]:
        """
        Load all the properties stored in the database.

        Returns
        -------
        list[tuple[str, int, str, object]]
            See documentation in AstarteDatabase.
        """
        properties = (
            sqlite3.connect(self.__database_path)
            .cursor()
            .execute("SELECT * FROM properties")
            .fetchall()
        )
        parsed_properties = []
        for interface, major, path, value in properties:
            parsed_properties += [(interface, major, path, pickle.loads(value))]
        return parsed_properties
