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

# pylint: disable=missing-function-docstring,missing-class-docstring
# pylint: disable=no-value-for-parameter,protected-access,
# pylint: disable=too-many-public-methods,no-self-use, abstract-class-instantiated

import unittest
from datetime import datetime
from json import JSONDecodeError
from pathlib import Path
from unittest import mock

from astarte.device.device import Device
from astarte.device.exceptions import (
    InterfaceFileDecodeError,
    InterfaceFileNotFoundError,
    InterfaceNotFoundError,
    ValidationError,
)
from astarte.device.introspection import Introspection


class TestMyAbstract(unittest.TestCase):
    def test_device_cannot_instantiate(self):
        """abstract class is not instantiable"""
        with self.assertRaises(TypeError):
            Device()

    @mock.patch.multiple(
        Device,
        __abstractmethods__=set(),
        add_interface_from_json=mock.DEFAULT,
        remove_interface=mock.DEFAULT,
        connect=mock.DEFAULT,
        disconnect=mock.DEFAULT,
        is_connected=mock.DEFAULT,
        _send_generic=mock.DEFAULT,
        _store_property=mock.DEFAULT,
    )
    def test_device_full_mock(
        self,
        add_interface_from_json,
        remove_interface,
        connect,
        disconnect,
        is_connected,
        _send_generic,
        _store_property,
    ):
        add_interface_from_json.return_value = 3
        remove_interface.return_value = 3
        connect.return_value = 3
        disconnect.return_value = 3
        is_connected.return_value = 3
        _send_generic.return_value = 3
        _store_property.return_value = 3

        Device()

    @mock.patch.multiple(Device, __abstractmethods__=set(), add_interface_from_json=mock.DEFAULT)
    @mock.patch("astarte.device.device.open", new_callable=mock.mock_open)
    @mock.patch("astarte.device.device.json.load", return_value="Fake json content")
    @mock.patch.object(Path, "is_file", return_value=True)
    def test_device_add_interface_from_file(
        self, mock_isfile, mock_json_load, mock_open, add_interface_from_json
    ):
        device = Device()

        device.add_interface_from_file(Path.cwd())

        mock_isfile.assert_called_once()
        mock_open.assert_called_once_with(Path.cwd(), "r", encoding="utf-8")
        mock_json_load.assert_called_once()
        add_interface_from_json.assert_called_once_with("Fake json content")

    @mock.patch.multiple(Device, __abstractmethods__=set())
    @mock.patch.object(Path, "is_file", return_value=False)
    def test_device_add_interface_from_file_missing_file_raises(self, mock_isfile):
        device = Device()

        self.assertRaises(
            InterfaceFileNotFoundError, lambda: device.add_interface_from_file(Path.cwd())
        )
        mock_isfile.assert_called_once()

    @mock.patch.multiple(Device, __abstractmethods__=set(), add_interface_from_json=mock.DEFAULT)
    @mock.patch.object(JSONDecodeError, "__init__", return_value=None)
    @mock.patch("astarte.device.device.open", new_callable=mock.mock_open)
    @mock.patch("astarte.device.device.json.load")
    @mock.patch.object(Path, "is_file", return_value=True)
    def test_device_add_interface_from_file_incorrect_json_raises(
        self, mock_isfile, mock_json_load, mock_open, mock_json_err, add_interface_from_json
    ):
        device = Device()

        mock_json_load.side_effect = JSONDecodeError()
        self.assertRaises(
            InterfaceFileDecodeError, lambda: device.add_interface_from_file(Path.cwd())
        )

        mock_isfile.assert_called_once()
        mock_json_err.assert_called_once()
        mock_open.assert_called_once_with(Path.cwd(), "r", encoding="utf-8")
        mock_json_load.assert_called_once()
        add_interface_from_json.assert_not_called()

    @mock.patch.multiple(Device, __abstractmethods__=set(), add_interface_from_file=mock.DEFAULT)
    @mock.patch.object(
        Path, "iterdir", return_value=[Path("f1.json"), Path("f.exe"), Path("f2.json")]
    )
    @mock.patch.object(Path, "is_dir", return_value=True)
    @mock.patch.object(Path, "exists", return_value=True)
    def test_device_add_interface_from_dir(
        self, mock_exists, mock_is_dir, mock_iterdir, add_interface_from_file
    ):
        device = Device()

        device.add_interfaces_from_dir(Path.cwd())

        mock_exists.assert_called_once()
        mock_is_dir.assert_called_once()
        mock_iterdir.assert_called_once()
        calls = [mock.call(Path("f1.json")), mock.call(Path("f2.json"))]
        add_interface_from_file.assert_has_calls(calls)
        self.assertEqual(add_interface_from_file.call_count, 2)

    @mock.patch.multiple(Device, __abstractmethods__=set())
    @mock.patch.object(Path, "exists", return_value=False)
    def test_device_add_interface_from_dir_non_existing_dir_raises(self, mock_exists):
        device = Device()

        self.assertRaises(
            InterfaceFileNotFoundError, lambda: device.add_interfaces_from_dir(Path.cwd())
        )
        mock_exists.assert_called_once()

    @mock.patch.multiple(Device, __abstractmethods__=set())
    @mock.patch.object(Path, "is_dir", return_value=False)
    @mock.patch.object(Path, "exists", return_value=True)
    def test_device_add_interface_from_dir_not_a_dir_raises(self, mock_exists, mock_is_dir):
        device = Device()

        self.assertRaises(
            InterfaceFileNotFoundError, lambda: device.add_interfaces_from_dir(Path.cwd())
        )
        mock_exists.assert_called_once()
        mock_is_dir.assert_called_once()

    @mock.patch.multiple(Device, __abstractmethods__=set(), _send_generic=mock.DEFAULT)
    @mock.patch.object(Introspection, "get_interface")
    def test_device_send(self, mock_get_interface, _send_generic):
        device = Device()

        mock_interface = mock.MagicMock()
        mock_interface.name = "interface name"
        mock_interface.is_aggregation_object.return_value = False
        mock_interface.is_server_owned.return_value = False
        mock_interface.is_type_properties.return_value = False
        mock_get_interface.return_value = mock_interface

        interface_name = "interface name"
        interface_path = "interface path"
        payload = 12
        timestamp = datetime.now()
        device.send(interface_name, interface_path, payload, timestamp)

        mock_get_interface.assert_called_once_with(interface_name)
        mock_interface.is_server_owned.assert_called_once()
        mock_interface.is_aggregation_object.assert_called_once()
        mock_interface.validate_payload_and_timestamp.assert_called_once_with(
            interface_path, payload, timestamp
        )
        _send_generic.assert_called_once_with(mock_interface, interface_path, payload, timestamp)

    @mock.patch.multiple(Device, __abstractmethods__=set(), _send_generic=mock.DEFAULT)
    @mock.patch.object(Introspection, "get_interface")
    def test_device_send_non_existing_interface_raises_interface_not_found(
        self, mock_get_interface, _send_generic
    ):
        device = Device()

        mock_get_interface.return_value = None

        interface_name = "interface name"
        interface_path = "interface path"
        payload = 12
        timestamp = datetime.now()
        self.assertRaises(
            InterfaceNotFoundError,
            lambda: device.send(interface_name, interface_path, payload, timestamp),
        )

        mock_get_interface.assert_called_once_with("interface name")
        _send_generic.assert_not_called()

    @mock.patch.multiple(Device, __abstractmethods__=set(), _send_generic=mock.DEFAULT)
    @mock.patch.object(Introspection, "get_interface")
    def test_device_send_to_a_server_owned_interface_raises(
        self, mock_get_interface, _send_generic
    ):
        device = Device()

        mock_interface = mock.MagicMock()
        mock_interface.is_server_owned.return_value = True
        mock_get_interface.return_value = mock_interface

        interface_name = "interface name"
        interface_path = "interface path"
        payload = 12
        timestamp = datetime.now()
        self.assertRaises(
            ValidationError, lambda: device.send(interface_name, interface_path, payload, timestamp)
        )

        mock_get_interface.assert_called_once_with("interface name")
        mock_get_interface.return_value.is_server_owned.assert_called_once()
        mock_get_interface.return_value.is_aggregation_object.assert_not_called()
        _send_generic.assert_not_called()

    @mock.patch.multiple(Device, __abstractmethods__=set(), _send_generic=mock.DEFAULT)
    @mock.patch.object(Introspection, "get_interface")
    def test_device_send_an_aggregate_raises_validation_err(
        self, mock_get_interface, _send_generic
    ):
        device = Device()

        mock_interface = mock.MagicMock()
        mock_interface.is_server_owned.return_value = False
        mock_interface.is_aggregation_object.return_value = True
        mock_get_interface.return_value = mock_interface

        interface_name = "interface name"
        interface_path = "interface path"
        payload = 12
        timestamp = datetime.now()
        self.assertRaises(
            ValidationError, lambda: device.send(interface_name, interface_path, payload, timestamp)
        )

        mock_get_interface.assert_called_once_with("interface name")
        mock_interface.is_server_owned.assert_called_once()
        mock_get_interface.return_value.is_aggregation_object.assert_called_once_with()
        _send_generic.assert_not_called()

    @mock.patch.multiple(Device, __abstractmethods__=set(), _send_generic=mock.DEFAULT)
    @mock.patch.object(Introspection, "get_interface")
    def test_device_send_none_payload_type_raises_validation_err(
        self, mock_get_interface, _send_generic
    ):
        device = Device()

        mock_interface = mock.MagicMock()
        mock_interface.is_server_owned.return_value = False
        mock_interface.is_aggregation_object.return_value = False
        mock_get_interface.return_value = mock_interface

        interface_name = "interface name"
        interface_path = "interface path"
        payload = None
        timestamp = datetime.now()
        self.assertRaises(
            ValidationError, lambda: device.send(interface_name, interface_path, payload, timestamp)
        )

        mock_get_interface.assert_called_once_with("interface name")
        mock_interface.is_server_owned.assert_called_once()
        mock_get_interface.return_value.is_aggregation_object.assert_called_once()
        _send_generic.assert_not_called()

    @mock.patch.multiple(Device, __abstractmethods__=set(), _send_generic=mock.DEFAULT)
    @mock.patch.object(Introspection, "get_interface")
    def test_device_send_wrong_payload_type_raises_validation_err(
        self, mock_get_interface, _send_generic
    ):
        device = Device()

        mock_interface = mock.MagicMock()
        mock_interface.is_server_owned.return_value = False
        mock_interface.is_aggregation_object.return_value = False
        mock_get_interface.return_value = mock_interface

        interface_name = "interface name"
        interface_path = "interface path"
        payload = {"something": 12}
        timestamp = datetime.now()
        self.assertRaises(
            ValidationError, lambda: device.send(interface_name, interface_path, payload, timestamp)
        )

        mock_get_interface.assert_called_once_with("interface name")
        mock_interface.is_server_owned.assert_called_once()
        mock_get_interface.return_value.is_aggregation_object.assert_called_once()
        _send_generic.assert_not_called()

    @mock.patch.multiple(Device, __abstractmethods__=set(), _send_generic=mock.DEFAULT)
    @mock.patch.object(Introspection, "get_interface")
    def test_device_send_interface_validate_raises_validation_err(
        self, mock_get_interface, _send_generic
    ):
        device = Device()

        mock_interface = mock.MagicMock()
        mock_interface.name = "interface name"
        mock_interface.is_server_owned.return_value = False
        mock_interface.is_aggregation_object.return_value = False
        mock_interface.validate_payload_and_timestamp.side_effect = ValidationError("Error msg")
        mock_get_interface.return_value = mock_interface

        interface_name = "interface name"
        interface_path = "interface path"
        payload = 12
        timestamp = datetime.now()
        self.assertRaises(
            ValidationError, lambda: device.send(interface_name, interface_path, payload, timestamp)
        )

        mock_get_interface.assert_called_once_with(interface_name)
        mock_interface.is_server_owned.assert_called_once()
        mock_interface.is_aggregation_object.assert_called_once()
        mock_interface.validate_payload_and_timestamp.assert_called_once_with(
            interface_path, payload, timestamp
        )
        _send_generic.assert_not_called()

    @mock.patch.multiple(Device, __abstractmethods__=set(), _send_generic=mock.DEFAULT)
    @mock.patch.object(Introspection, "get_interface")
    def test_device_send_aggregate(self, mock_get_interface, _send_generic):
        device = Device()

        mock_interface = mock.MagicMock()
        mock_interface.name = "interface name"
        mock_interface.is_server_owned.return_value = False
        mock_interface.is_aggregation_object.return_value = True
        mock_interface.is_type_properties.return_value = False
        mock_get_interface.return_value = mock_interface

        interface_name = "interface name"
        interface_path = "interface path"
        payload = {"something": 12}
        timestamp = datetime.now()
        device.send_aggregate(interface_name, interface_path, payload, timestamp)

        mock_get_interface.assert_called_once_with(interface_name)
        mock_interface.is_server_owned.assert_called_once()
        mock_interface.is_aggregation_object.assert_called_once()
        mock_interface.validate_payload_and_timestamp.assert_called_once_with(
            interface_path, payload, timestamp
        )
        _send_generic.assert_called_once_with(mock_interface, interface_path, payload, timestamp)

    @mock.patch.multiple(Device, __abstractmethods__=set(), _send_generic=mock.DEFAULT)
    @mock.patch.object(Introspection, "get_interface")
    def test_device_send_aggregate_non_existing_interface_raises(
        self, mock_get_interface, _send_generic
    ):
        device = Device()

        mock_get_interface.return_value = None

        interface_name = "interface name"
        interface_path = "interface path"
        payload = {"something": 12}
        timestamp = datetime.now()
        self.assertRaises(
            InterfaceNotFoundError,
            lambda: device.send_aggregate(interface_name, interface_path, payload, timestamp),
        )

        mock_get_interface.assert_called_once_with("interface name")
        _send_generic.assert_not_called()

    @mock.patch.multiple(Device, __abstractmethods__=set(), _send_generic=mock.DEFAULT)
    @mock.patch.object(Introspection, "get_interface")
    def test_device_send_aggregate_server_owned_interface_raises(
        self, mock_get_interface, _send_generic
    ):
        device = Device()

        mock_interface = mock.MagicMock()
        mock_interface.name = "interface name"
        mock_interface.is_server_owned.return_value = True
        mock_get_interface.return_value = mock_interface

        interface_name = "interface name"
        interface_path = "interface path"
        payload = {"something": 12}
        timestamp = datetime.now()
        self.assertRaises(
            ValidationError,
            lambda: device.send_aggregate(interface_name, interface_path, payload, timestamp),
        )

        mock_get_interface.assert_called_once_with("interface name")
        mock_interface.is_server_owned.assert_called_once()
        _send_generic.assert_not_called()

    @mock.patch.multiple(Device, __abstractmethods__=set(), _send_generic=mock.DEFAULT)
    @mock.patch.object(Introspection, "get_interface")
    def test_device_send_aggregate_is_not_an_aggregate_raises(
        self, mock_get_interface, _send_generic
    ):
        device = Device()

        mock_interface = mock.MagicMock()
        mock_interface.is_server_owned.return_value = False
        mock_interface.is_aggregation_object.return_value = False
        mock_interface.is_type_properties.return_value = False
        mock_get_interface.return_value = mock_interface

        interface_name = "interface name"
        interface_path = "interface path"
        payload = {"something": 12}
        timestamp = datetime.now()
        self.assertRaises(
            ValidationError,
            lambda: device.send_aggregate(interface_name, interface_path, payload, timestamp),
        )

        mock_get_interface.assert_called_once_with(interface_name)
        mock_interface.is_server_owned.assert_called_once()
        mock_interface.is_aggregation_object.assert_called_once()
        mock_interface.validate_payload_and_timestamp.assert_not_called()
        _send_generic.assert_not_called()

    @mock.patch.multiple(Device, __abstractmethods__=set(), _send_generic=mock.DEFAULT)
    @mock.patch.object(Introspection, "get_interface")
    def test_device_send_aggregate_none_payload_type_raises(
        self, mock_get_interface, _send_generic
    ):
        device = Device()

        mock_interface = mock.MagicMock()
        mock_interface.is_server_owned.return_value = False
        mock_interface.is_aggregation_object.return_value = True
        mock_interface.is_type_properties.return_value = False
        mock_get_interface.return_value = mock_interface

        interface_name = "interface name"
        interface_path = "interface path"
        payload = None
        timestamp = datetime.now()
        self.assertRaises(
            ValidationError,
            lambda: device.send_aggregate(interface_name, interface_path, payload, timestamp),
        )

        mock_get_interface.assert_called_once_with(interface_name)
        mock_interface.is_server_owned.assert_called_once()
        mock_interface.is_aggregation_object.assert_called_once()
        mock_interface.validate_payload_and_timestamp.assert_not_called()
        _send_generic.assert_not_called()

    @mock.patch.multiple(Device, __abstractmethods__=set(), _send_generic=mock.DEFAULT)
    @mock.patch.object(Introspection, "get_interface")
    def test_device_send_aggregate_wrong_payload_type_raises(
        self, mock_get_interface, _send_generic
    ):
        device = Device()

        mock_interface = mock.MagicMock()
        mock_interface.is_server_owned.return_value = False
        mock_interface.is_aggregation_object.return_value = True
        mock_interface.is_type_properties.return_value = False
        mock_get_interface.return_value = mock_interface

        interface_name = "interface name"
        interface_path = "interface path"
        payload = 12
        timestamp = datetime.now()
        self.assertRaises(
            ValidationError,
            lambda: device.send_aggregate(interface_name, interface_path, payload, timestamp),
        )

        mock_get_interface.assert_called_once_with(interface_name)
        mock_interface.is_server_owned.assert_called_once()
        mock_interface.is_aggregation_object.assert_called_once()
        mock_interface.validate_payload_and_timestamp.assert_not_called()
        _send_generic.assert_not_called()

    @mock.patch.multiple(Device, __abstractmethods__=set(), _send_generic=mock.DEFAULT)
    @mock.patch.object(Introspection, "get_interface")
    def test_device_send_aggregate_interface_validate_raises(
        self, mock_get_interface, _send_generic
    ):
        device = Device()

        mock_interface = mock.MagicMock()
        mock_interface.name = "interface name"
        mock_interface.is_server_owned.return_value = False
        mock_interface.is_aggregation_object.return_value = True
        mock_interface.is_type_properties.return_value = False
        mock_interface.validate_payload_and_timestamp.side_effect = ValidationError("Error msg")
        mock_get_interface.return_value = mock_interface

        interface_name = "interface name"
        interface_path = "interface path"
        payload = {"something": 12}
        timestamp = datetime.now()
        self.assertRaises(
            ValidationError,
            lambda: device.send_aggregate(interface_name, interface_path, payload, timestamp),
        )

        mock_get_interface.assert_called_once_with(interface_name)
        mock_interface.is_server_owned.assert_called_once()
        mock_interface.is_aggregation_object.assert_called_once()
        mock_interface.validate_payload_and_timestamp.assert_called_once_with(
            interface_path, payload, timestamp
        )
        _send_generic.assert_not_called()

    @mock.patch.multiple(Device, __abstractmethods__=set(), _send_generic=mock.DEFAULT)
    @mock.patch.object(Introspection, "get_interface")
    def test_device_unset_property(self, mock_get_interface, _send_generic):
        device = Device()

        mock_interface = mock.MagicMock()
        mock_interface.name = "interface name"
        mock_interface.is_server_owned.return_value = False
        mock_interface.is_type_properties.return_value = True
        mock_get_interface.return_value = mock_interface

        interface_name = "interface name"
        interface_path = "interface path"
        device.unset_property(interface_name, interface_path)

        mock_get_interface.assert_called_once_with(interface_name)
        mock_interface.is_server_owned.assert_called_once()
        mock_interface.is_type_properties.assert_called_once()
        _send_generic.assert_called_once_with(mock_interface, interface_path, None, None)

    @mock.patch.multiple(Device, __abstractmethods__=set(), _send_generic=mock.DEFAULT)
    @mock.patch.object(Introspection, "get_interface")
    def test_device_unset_property_non_existing_interface_raises(
        self, mock_get_interface, _send_generic
    ):
        device = Device()

        mock_get_interface.return_value = None

        interface_name = "interface name"
        interface_path = "interface path"
        self.assertRaises(
            InterfaceNotFoundError,
            lambda: device.unset_property(interface_name, interface_path),
        )

        mock_get_interface.assert_called_once_with("interface name")
        _send_generic.assert_not_called()

    @mock.patch.multiple(Device, __abstractmethods__=set(), _send_generic=mock.DEFAULT)
    @mock.patch.object(Introspection, "get_interface")
    def test_device_unset_property_server_owned_interface_raises(
        self, mock_get_interface, _send_generic
    ):
        device = Device()

        mock_interface = mock.MagicMock()
        mock_interface.name = "interface name"
        mock_interface.is_server_owned.return_value = True
        mock_get_interface.return_value = mock_interface

        interface_name = "interface name"
        interface_path = "interface path"
        self.assertRaises(
            ValidationError, lambda: device.unset_property(interface_name, interface_path)
        )

        mock_get_interface.assert_called_once_with(interface_name)
        mock_interface.is_server_owned.assert_called_once()
        mock_interface.is_type_properties.assert_not_called()
        _send_generic.assert_not_called()

    @mock.patch.multiple(Device, __abstractmethods__=set(), _send_generic=mock.DEFAULT)
    @mock.patch.object(Introspection, "get_interface")
    def test_device_unset_property_interface_not_a_property_raises(
        self, mock_get_interface, _send_generic
    ):
        device = Device()

        mock_interface = mock.MagicMock()
        mock_interface.name = "interface name"
        mock_interface.is_server_owned.return_value = False
        mock_interface.is_type_properties.return_value = False
        mock_get_interface.return_value = mock_interface

        interface_name = "interface name"
        interface_path = "interface path"
        self.assertRaises(
            ValidationError, lambda: device.unset_property(interface_name, interface_path)
        )

        mock_get_interface.assert_called_once_with(interface_name)
        mock_interface.is_server_owned.assert_called_once()
        mock_interface.is_type_properties.assert_called_once()
        _send_generic.assert_not_called()

    @mock.patch.multiple(Device, __abstractmethods__=set(), _store_property=mock.DEFAULT)
    @mock.patch.object(Introspection, "get_interface")
    def test_device_on_message_generic(self, mock_get_interface, _store_property):
        device = Device()

        on_data_received_mock = mock.MagicMock()
        device.set_events_callbacks(on_data_received=on_data_received_mock)
        interface_name = "interface name"
        interface_path = "interface path"
        mock_message = mock.MagicMock()
        device._on_message_generic(interface_name, interface_path, mock_message)

        mock_get_interface.assert_called_once_with(interface_name)
        mock_get_interface.return_value.is_server_owned.assert_called_once()
        mock_get_interface.return_value.is_property_endpoint_resettable.assert_not_called()
        mock_get_interface.return_value.validate_path.assert_called_once_with(
            interface_path, mock_message
        )
        mock_get_interface.return_value.validate_payload.assert_called_once_with(
            interface_path, mock_message
        )
        _store_property.assert_called_once_with(
            mock_get_interface.return_value, interface_path, mock_message
        )
        on_data_received_mock.assert_called_once_with(
            device, interface_name, interface_path, mock_message
        )

    @mock.patch.multiple(Device, __abstractmethods__=set(), _store_property=mock.DEFAULT)
    @mock.patch.object(Introspection, "get_interface")
    def test_device_on_message_generic_with_threading(self, mock_get_interface, _store_property):
        device = Device()

        mock_loop = mock.MagicMock()
        on_data_received_mock = mock.MagicMock()
        device.set_events_callbacks(on_data_received=on_data_received_mock, loop=mock_loop)
        interface_name = "interface name"
        interface_path = "interface path"
        mock_message = mock.MagicMock()
        device._on_message_generic(interface_name, interface_path, mock_message)

        mock_get_interface.assert_called_once_with(interface_name)
        mock_get_interface.return_value.is_server_owned.assert_called_once()
        mock_get_interface.return_value.is_property_endpoint_resettable.assert_not_called()
        mock_get_interface.return_value.validate_path.assert_called_once_with(
            interface_path, mock_message
        )
        mock_get_interface.return_value.validate_payload.assert_called_once_with(
            interface_path, mock_message
        )
        _store_property.assert_called_once_with(
            mock_get_interface.return_value, interface_path, mock_message
        )
        mock_loop.call_soon_threadsafe.assert_called_once_with(
            on_data_received_mock, device, interface_name, interface_path, mock_message
        )
        on_data_received_mock.assert_not_called()

    @mock.patch.multiple(Device, __abstractmethods__=set(), _store_property=mock.DEFAULT)
    @mock.patch.object(Introspection, "get_interface")
    def test_device_on_message_generic_unregistered_interface_name(
        self, mock_get_interface, _store_property
    ):
        device = Device()

        mock_get_interface.return_value = None

        on_data_received_mock = mock.MagicMock()
        device.set_events_callbacks(on_data_received=on_data_received_mock)
        interface_name = "interface name"
        interface_path = "interface path"
        mock_message = mock.MagicMock()
        device._on_message_generic(interface_name, interface_path, mock_message)

        mock_get_interface.assert_called_once_with(interface_name)
        on_data_received_mock.assert_not_called()

    @mock.patch.multiple(Device, __abstractmethods__=set(), _store_property=mock.DEFAULT)
    @mock.patch.object(Introspection, "get_interface")
    def test_device_on_message_generic_device_owned_interface(
        self, mock_get_interface, _store_property
    ):
        device = Device()

        mock_interface = mock.MagicMock()
        mock_interface.is_server_owned.return_value = False
        mock_get_interface.return_value = mock_interface

        on_data_received_mock = mock.MagicMock()
        device.set_events_callbacks(on_data_received=on_data_received_mock)
        interface_name = "interface name"
        interface_path = "interface path"
        mock_message = mock.MagicMock()
        device._on_message_generic(interface_name, interface_path, mock_message)

        mock_get_interface.assert_called_once_with(interface_name)
        mock_interface.is_server_owned.assert_called_once()
        mock_interface.is_property_endpoint_resettable.assert_not_called()
        mock_interface.validate_path.assert_not_called()
        mock_interface.validate_payload.assert_not_called()
        _store_property.assert_not_called()
        on_data_received_mock.assert_not_called()

    @mock.patch.multiple(Device, __abstractmethods__=set(), _store_property=mock.DEFAULT)
    @mock.patch.object(Introspection, "get_interface")
    def test_device_on_message_generic_empty_payload_but_not_a_property(
        self, mock_get_interface, _store_property
    ):
        device = Device()

        mock_interface = mock.MagicMock()
        mock_interface.is_server_owned.return_value = True
        mock_interface.is_property_endpoint_resettable.return_value = False
        mock_get_interface.return_value = mock_interface

        on_data_received_mock = mock.MagicMock()
        device.set_events_callbacks(on_data_received=on_data_received_mock)
        interface_name = "interface name"
        interface_path = "interface path"
        device._on_message_generic(interface_name, interface_path, None)

        mock_get_interface.assert_called_once_with(interface_name)
        mock_interface.is_server_owned.assert_called_once()
        mock_interface.is_property_endpoint_resettable.assert_called_once_with(interface_path)
        mock_interface.validate_path.assert_not_called()
        mock_interface.validate_payload.assert_not_called()
        _store_property.assert_not_called()
        on_data_received_mock.assert_not_called()

    @mock.patch.multiple(Device, __abstractmethods__=set(), _store_property=mock.DEFAULT)
    @mock.patch.object(Introspection, "get_interface")
    def test_device_on_message_generic_incorrect_path(self, mock_get_interface, _store_property):
        device = Device()

        mock_interface = mock.MagicMock()
        mock_interface.is_server_owned.return_value = True
        mock_interface.validate_path.side_effect = ValidationError("")
        mock_get_interface.return_value = mock_interface

        on_data_received_mock = mock.MagicMock()
        device.set_events_callbacks(on_data_received=on_data_received_mock)
        interface_name = "interface name"
        interface_path = "interface path"
        mock_message = mock.MagicMock()
        device._on_message_generic(interface_name, interface_path, mock_message)

        mock_get_interface.assert_called_once_with(interface_name)
        mock_get_interface.return_value.is_server_owned.assert_called_once()
        mock_get_interface.return_value.is_property_endpoint_resettable.assert_not_called()
        mock_get_interface.return_value.validate_path.assert_called_once_with(
            interface_path, mock_message
        )
        mock_get_interface.return_value.validate_payload.assert_not_called()
        _store_property.assert_not_called()
        on_data_received_mock.assert_not_called()

    @mock.patch.multiple(Device, __abstractmethods__=set(), _store_property=mock.DEFAULT)
    @mock.patch.object(Introspection, "get_interface")
    def test_device_on_message_generic_payload_validation_failure(
        self, mock_get_interface, _store_property
    ):
        device = Device()

        mock_interface = mock.MagicMock()
        mock_interface.is_server_owned.return_value = True
        mock_interface.validate_payload.side_effect = ValidationError("")
        mock_get_interface.return_value = mock_interface

        on_data_received_mock = mock.MagicMock()
        device.set_events_callbacks(on_data_received=on_data_received_mock)
        interface_name = "interface name"
        interface_path = "interface path"
        mock_message = mock.MagicMock()
        device._on_message_generic(interface_name, interface_path, mock_message)

        mock_get_interface.assert_called_once_with(interface_name)
        mock_get_interface.return_value.is_server_owned.assert_called_once()
        mock_get_interface.return_value.is_property_endpoint_resettable.assert_not_called()
        mock_get_interface.return_value.validate_path.assert_called_once_with(
            interface_path, mock_message
        )
        mock_get_interface.return_value.validate_payload.assert_called_once_with(
            interface_path, mock_message
        )
        _store_property.assert_not_called()
        on_data_received_mock.assert_not_called()
