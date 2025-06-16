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

# pylint: disable=useless-suppression,missing-function-docstring,missing-class-docstring
# pylint: disable=too-many-statements,too-many-instance-attributes,missing-return-doc
# pylint: disable=missing-return-type-doc,no-value-for-parameter,protected-access,
# pylint: disable=too-many-public-methods,no-self-use

import pickle
import typing
import unittest
from pathlib import Path
from unittest import mock

from astarte.device import database
from astarte.device.interface import InterfaceOwnership

TEST_DATABASE = Path("./test_database_DELETE_ME.db")


class UnitTests(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    @mock.patch("astarte.device.database.sqlite3.connect")
    def test_initialize(self, mock_sqlite3_connect):
        path_to_database = mock.MagicMock()

        database.AstarteDatabaseSQLite(path_to_database)

        mock_sqlite3_connect.assert_called_once_with(path_to_database)
        mock_sqlite3_connect.return_value.cursor.assert_called_once_with()
        execute_expected_arg = (
            "CREATE TABLE IF NOT EXISTS properties ("
            "interface TEXT NOT NULL,"
            "major INTEGER NOT NULL,"
            "path TEXT NOT NULL,"
            "ownership CHARACTER(6) CHECK(ownership IN "
            + database.ownership_sqlite_set()
            + ") NOT NULL,"
            "value BLOB NOT NULL,"
            "PRIMARY KEY (interface, path))"
        )
        mock_sqlite3_connect.return_value.cursor.return_value.execute.assert_called_once_with(
            execute_expected_arg
        )

    # Mocking delete_prop as it gets tested as a stand alone function
    @mock.patch.object(database.AstarteDatabaseSQLite, "delete_prop")
    @mock.patch("astarte.device.database.pickle.dumps")
    @mock.patch("astarte.device.database.sqlite3.connect")
    def test_store_property_with_payload(
        self, mock_sqlite3_connect, mock_pickle_dumps, mock_delete_prop
    ):
        mock_database_name = mock.MagicMock()

        mock_connection = mock_sqlite3_connect.return_value
        mock_cursor = mock_sqlite3_connect.return_value.cursor.return_value

        db = database.AstarteDatabaseSQLite(mock_database_name)
        mock_sqlite3_connect.reset_mock()

        mock_interface = mock.MagicMock()
        mock_version = mock.MagicMock()
        mock_path = mock.MagicMock()
        mock_ownership = InterfaceOwnership.DEVICE
        mock_value = mock.MagicMock()
        db.store_prop(mock_interface, mock_version, mock_path, mock_ownership, mock_value)

        mock_delete_prop.assert_not_called()
        mock_sqlite3_connect.assert_called_once_with(mock_database_name)
        mock_sqlite3_connect.return_value.cursor.assert_called_once_with()
        mock_pickle_dumps.assert_called_once_with(mock_value)
        execute_expected_arg = "INSERT OR REPLACE INTO properties (interface, major, path, ownership, value) VALUES (?, ?, ?, ?, ?)"
        mock_cursor.execute.assert_called_once_with(
            execute_expected_arg,
            (
                mock_interface,
                mock_version,
                mock_path,
                mock_ownership.value,
                mock_pickle_dumps.return_value,
            ),
        )
        mock_connection.commit.assert_called_once_with()

    # Mocking delete_prop as it gets tested as a stand alone function
    @mock.patch.object(database.AstarteDatabaseSQLite, "delete_prop")
    @mock.patch("astarte.device.database.pickle.dumps")
    @mock.patch("astarte.device.database.sqlite3.connect")
    def test_store_property_without_payload(
        self, mock_sqlite3_connect, mock_pickle_dumps, mock_delete_prop
    ):
        mock_database_name = mock.MagicMock()

        mock_connection = mock_sqlite3_connect.return_value
        mock_cursor = mock_sqlite3_connect.return_value.cursor.return_value

        db = database.AstarteDatabaseSQLite(mock_database_name)
        mock_sqlite3_connect.reset_mock()

        mock_interface = mock.MagicMock()
        mock_version = mock.MagicMock()
        mock_path = mock.MagicMock()
        mock_ownership = mock.MagicMock()
        db.store_prop(mock_interface, mock_version, mock_path, mock_ownership, None)

        mock_delete_prop.assert_called_once_with(mock_interface, mock_path)
        mock_sqlite3_connect.assert_not_called()
        mock_sqlite3_connect.return_value.cursor.assert_not_called()
        mock_pickle_dumps.assert_not_called()
        mock_cursor.execute.assert_not_called()
        mock_connection.commit.assert_not_called()

    @mock.patch("astarte.device.database.pickle.loads")
    @mock.patch("astarte.device.database.sqlite3.connect")
    def test_load_property(self, mock_sqlite3_connect, mock_pickle_loads):
        mock_database_name = mock.MagicMock()

        mock_cursor = mock_sqlite3_connect.return_value.cursor.return_value
        mock_db_ownership = "device"
        mock_db_version = mock.MagicMock()
        mock_db_value = mock.MagicMock()
        mock_cursor.execute.return_value.fetchone.return_value = (
            mock_db_ownership,
            mock_db_value,
            mock_db_version,
        )

        db = database.AstarteDatabaseSQLite(mock_database_name)
        mock_sqlite3_connect.reset_mock()

        mock_interface = mock.MagicMock()
        mock_version = mock_db_version
        mock_path = mock.MagicMock()
        result = db.load_prop(mock_interface, mock_version, mock_path)

        mock_sqlite3_connect.assert_called_once_with(mock_database_name)
        mock_sqlite3_connect.return_value.cursor.assert_called_once_with()
        execute_expected_arg = (
            "SELECT ownership, value, major FROM properties WHERE interface=? AND path=?"
        )
        mock_cursor.execute.assert_called_once_with(
            execute_expected_arg, (mock_interface, mock_path)
        )
        mock_cursor.execute.return_value.fetchone.assert_called_once_with()
        mock_pickle_loads.assert_called_once_with(mock_db_value)
        self.assertEqual(
            _property_to_tuple(typing.cast(database.StoredProperty, result)),
            (
                mock_interface,
                mock_path,
                mock_db_version,
                InterfaceOwnership(mock_db_ownership),
                mock_pickle_loads.return_value,
            ),
        )

    # Mocking delete_prop as it gets tested as a stand alone function
    @mock.patch.object(database.AstarteDatabaseSQLite, "delete_prop")
    @mock.patch("astarte.device.database.pickle.loads")
    @mock.patch("astarte.device.database.sqlite3.connect")
    def test_load_property_version_mismatch(
        self, mock_sqlite3_connect, mock_pickle_loads, mock_delete_prop
    ):
        mock_database_name = mock.MagicMock()

        mock_cursor = mock_sqlite3_connect.return_value.cursor.return_value
        mock_db_ownership = mock.MagicMock()
        mock_db_version = mock.MagicMock()
        mock_db_value = mock.MagicMock()
        mock_cursor.execute.return_value.fetchone.return_value = (
            mock_db_ownership,
            mock_db_value,
            mock_db_version,
        )

        db = database.AstarteDatabaseSQLite(mock_database_name)
        mock_sqlite3_connect.reset_mock()

        mock_interface = mock.MagicMock()
        mock_version = mock.MagicMock()
        mock_path = mock.MagicMock()
        result = db.load_prop(mock_interface, mock_version, mock_path)

        mock_sqlite3_connect.assert_called_once_with(mock_database_name)
        mock_sqlite3_connect.return_value.cursor.assert_called_once_with()
        execute_expected_arg = (
            "SELECT ownership, value, major FROM properties WHERE interface=? AND path=?"
        )
        mock_cursor.execute.assert_called_once_with(
            execute_expected_arg, (mock_interface, mock_path)
        )
        mock_cursor.execute.return_value.fetchone.assert_called_once_with()
        mock_pickle_loads.assert_not_called()
        mock_delete_prop.assert_called_once_with(mock_interface, mock_path)
        self.assertEqual(result, None)

    @mock.patch("astarte.device.database.sqlite3.connect")
    def test_delete_property(self, mock_sqlite3_connect):
        mock_database_name = mock.MagicMock()

        mock_connection = mock_sqlite3_connect.return_value
        mock_cursor = mock_sqlite3_connect.return_value.cursor.return_value

        db = database.AstarteDatabaseSQLite(mock_database_name)
        mock_sqlite3_connect.reset_mock()

        mock_interface = mock.MagicMock()
        mock_path = mock.MagicMock()
        db.delete_prop(mock_interface, mock_path)

        mock_sqlite3_connect.assert_called_once_with(mock_database_name)
        mock_sqlite3_connect.return_value.cursor.assert_called_once_with()
        execute_expected_arg = "DELETE FROM properties WHERE interface=? AND path=?"
        mock_cursor.execute.assert_called_once_with(
            execute_expected_arg, (mock_interface, mock_path)
        )
        mock_connection.commit.assert_called_once_with()

    @mock.patch("astarte.device.database.sqlite3.connect")
    def test_delete_props_from_interface(self, mock_sqlite3_connect):
        mock_database_name = mock.MagicMock()

        mock_connection = mock_sqlite3_connect.return_value
        mock_cursor = mock_sqlite3_connect.return_value.cursor.return_value

        db = database.AstarteDatabaseSQLite(mock_database_name)
        mock_sqlite3_connect.reset_mock()

        mock_interface = mock.MagicMock()
        db.delete_props_from_interface(mock_interface)

        mock_sqlite3_connect.assert_called_once_with(mock_database_name)
        mock_sqlite3_connect.return_value.cursor.assert_called_once_with()
        execute_expected_arg = "DELETE FROM properties WHERE interface=?"
        mock_cursor.execute.assert_called_once_with(execute_expected_arg, (mock_interface,))
        mock_connection.commit.assert_called_once_with()

    @mock.patch("astarte.device.database.sqlite3.connect")
    def test_clear(self, mock_sqlite3_connect):
        mock_database_name = mock.MagicMock()

        mock_connection = mock_sqlite3_connect.return_value
        mock_cursor = mock_sqlite3_connect.return_value.cursor.return_value

        db = database.AstarteDatabaseSQLite(mock_database_name)
        mock_sqlite3_connect.reset_mock()

        db.clear()

        mock_sqlite3_connect.assert_called_once_with(mock_database_name)
        mock_sqlite3_connect.return_value.cursor.assert_called_once_with()
        execute_expected_arg = "DELETE * FROM properties"
        mock_cursor.execute.assert_called_once_with(execute_expected_arg)
        mock_connection.commit.assert_called_once_with()

    @mock.patch("astarte.device.database.pickle.loads")
    @mock.patch("astarte.device.database.sqlite3.connect")
    def test_load_all_properties(self, mock_sqlite3_connect, mock_pickle_loads):
        mock_database_name = mock.MagicMock()

        mock_cursor = mock_sqlite3_connect.return_value.cursor.return_value
        mock_db_query_result = [
            (
                mock.MagicMock(),
                mock.MagicMock(),
                mock.MagicMock(),
                "server",
                mock.MagicMock(),
            ),
            (
                mock.MagicMock(),
                mock.MagicMock(),
                mock.MagicMock(),
                "server",
                mock.MagicMock(),
            ),
            (
                mock.MagicMock(),
                mock.MagicMock(),
                mock.MagicMock(),
                "server",
                mock.MagicMock(),
            ),
        ]
        mock_cursor.execute.return_value.fetchall.return_value = mock_db_query_result
        deserialization_result = [mock.MagicMock(), mock.MagicMock(), mock.MagicMock()]
        mock_pickle_loads.side_effect = deserialization_result

        db = database.AstarteDatabaseSQLite(mock_database_name)
        mock_sqlite3_connect.reset_mock()

        result = db.load_all_props()

        mock_sqlite3_connect.assert_called_once_with(mock_database_name)
        mock_sqlite3_connect.return_value.cursor.assert_called_once_with()
        execute_expected_arg = "SELECT interface, major, path, ownership, value FROM properties"
        mock_cursor.execute.assert_called_once_with(execute_expected_arg)
        mock_cursor.execute.return_value.fetchall.assert_called_once_with()
        calls = [
            mock.call(mock_db_query_result[0][4]),
            mock.call(mock_db_query_result[1][4]),
            mock.call(mock_db_query_result[2][4]),
        ]
        mock_pickle_loads.assert_has_calls(calls)
        self.assertEqual(mock_pickle_loads.call_count, 3)
        expected_result = [
            database.StoredProperty(
                mock_db_query_result[0][0],
                mock_db_query_result[0][2],
                mock_db_query_result[0][1],
                InterfaceOwnership(mock_db_query_result[0][3]),
                deserialization_result[0],
            ),
            database.StoredProperty(
                mock_db_query_result[1][0],
                mock_db_query_result[1][2],
                mock_db_query_result[1][1],
                InterfaceOwnership(mock_db_query_result[1][3]),
                deserialization_result[1],
            ),
            database.StoredProperty(
                mock_db_query_result[2][0],
                mock_db_query_result[2][2],
                mock_db_query_result[2][1],
                InterfaceOwnership(mock_db_query_result[2][3]),
                deserialization_result[2],
            ),
        ]

        result_array = [_property_to_tuple(r) for r in result]
        expected_result_array = [_property_to_tuple(r) for r in expected_result]

        self.assertEqual(result_array, expected_result_array)

    @mock.patch("astarte.device.database.sqlite3.connect")
    def test_load_server_properties(self, mock_sqlite3_connect):
        mock_database_name = mock.MagicMock()

        mock_cursor = mock_sqlite3_connect.return_value.cursor.return_value
        mock_db_query_result = [
            (
                mock.MagicMock(),
                mock.MagicMock(),
                mock.MagicMock(),
                "server",
                pickle.dumps("test"),
            ),
            (
                mock.MagicMock(),
                mock.MagicMock(),
                mock.MagicMock(),
                "server",
                pickle.dumps(101),
            ),
        ]
        mock_cursor.execute.return_value.fetchall.return_value = mock_db_query_result

        db = database.AstarteDatabaseSQLite(mock_database_name)
        mock_sqlite3_connect.reset_mock()

        result = db.load_server_props()

        mock_sqlite3_connect.assert_called_once_with(mock_database_name)
        mock_sqlite3_connect.return_value.cursor.assert_called_once_with()
        mock_cursor.execute.assert_called_once_with(
            "SELECT interface, major, path, ownership, value FROM properties WHERE ownership = ?",
            (InterfaceOwnership.SERVER.value,),
        )
        mock_cursor.execute.return_value.fetchall.assert_called_once_with()
        expected_result = [
            database.StoredProperty(
                mock_db_query_result[0][0],
                mock_db_query_result[0][2],
                mock_db_query_result[0][1],
                InterfaceOwnership(mock_db_query_result[0][3]),
                pickle.loads(mock_db_query_result[0][4]),
            ),
            database.StoredProperty(
                mock_db_query_result[1][0],
                mock_db_query_result[1][2],
                mock_db_query_result[1][1],
                InterfaceOwnership(mock_db_query_result[1][3]),
                pickle.loads(mock_db_query_result[1][4]),
            ),
        ]

        result_array = [_property_to_tuple(r) for r in result]
        expected_result_array = [_property_to_tuple(r) for r in expected_result]

        self.assertEqual(result_array, expected_result_array)

    @mock.patch("astarte.device.database.sqlite3.connect")
    def test_load_device_properties(self, mock_sqlite3_connect):
        mock_database_name = mock.MagicMock()

        mock_cursor = mock_sqlite3_connect.return_value.cursor.return_value
        mock_db_query_result = [
            (
                mock.MagicMock(),
                mock.MagicMock(),
                mock.MagicMock(),
                "device",
                pickle.dumps(1.3),
            ),
            (
                mock.MagicMock(),
                mock.MagicMock(),
                mock.MagicMock(),
                "device",
                pickle.dumps(True),
            ),
        ]
        mock_cursor.execute.return_value.fetchall.return_value = mock_db_query_result

        db = database.AstarteDatabaseSQLite(mock_database_name)
        mock_sqlite3_connect.reset_mock()

        result = db.load_device_props()

        mock_sqlite3_connect.assert_called_once_with(mock_database_name)
        mock_sqlite3_connect.return_value.cursor.assert_called_once_with()
        mock_cursor.execute.assert_called_once_with(
            "SELECT interface, major, path, ownership, value FROM properties WHERE ownership = ?",
            (InterfaceOwnership.DEVICE.value,),
        )
        mock_cursor.execute.return_value.fetchall.assert_called_once_with()
        expected_result = [
            database.StoredProperty(
                mock_db_query_result[0][0],
                mock_db_query_result[0][2],
                mock_db_query_result[0][1],
                InterfaceOwnership(mock_db_query_result[0][3]),
                pickle.loads(mock_db_query_result[0][4]),
            ),
            database.StoredProperty(
                mock_db_query_result[1][0],
                mock_db_query_result[1][2],
                mock_db_query_result[1][1],
                InterfaceOwnership(mock_db_query_result[1][3]),
                pickle.loads(mock_db_query_result[1][4]),
            ),
        ]

        result_array = [_property_to_tuple(r) for r in result]
        expected_result_array = [_property_to_tuple(r) for r in expected_result]

        self.assertEqual(result_array, expected_result_array)


def _property_to_tuple(property: database.StoredProperty) -> tuple:
    return (
        property.interface,
        property.path,
        property.major,
        property.ownership,
        property.value,
    )
