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

import unittest
from unittest import mock

from astarte.device.introspection import Introspection


class UnitTests(unittest.TestCase):
    def setUp(self):
        pass

    def test_introspection_initialization(self):
        introspection = Introspection()
        self.assertEqual(introspection._Introspection__interfaces_list, {})

    def test_introspection_add_interface(self):
        introspection = Introspection()
        self.assertEqual(introspection._Introspection__interfaces_list, {})

        interface_instance_1 = mock.MagicMock()
        interface_instance_1.name = "first interface"
        interface_instance_2 = mock.MagicMock()
        interface_instance_2.name = "second interface"

        introspection.add_interface(interface_instance_1)
        self.assertEqual(
            introspection._Introspection__interfaces_list,
            {interface_instance_1.name: interface_instance_1},
        )

        introspection.add_interface(interface_instance_2)
        self.assertEqual(
            introspection._Introspection__interfaces_list,
            {
                interface_instance_1.name: interface_instance_1,
                interface_instance_2.name: interface_instance_2,
            },
        )

    def test_introspection_add_interface_duplicated(self):
        introspection = Introspection()
        self.assertEqual(introspection._Introspection__interfaces_list, {})

        interface_instance_1 = mock.MagicMock()
        interface_instance_1.name = "first interface"
        interface_instance_2 = mock.MagicMock()
        interface_instance_2.name = "first interface"

        introspection.add_interface(interface_instance_1)
        self.assertEqual(
            introspection._Introspection__interfaces_list,
            {interface_instance_1.name: interface_instance_1},
        )

        introspection.add_interface(interface_instance_2)
        self.assertEqual(
            introspection._Introspection__interfaces_list,
            {interface_instance_2.name: interface_instance_2},
        )

    def test_introspection_remove_interface(self):
        introspection = Introspection()

        mock_interface_1 = mock.MagicMock()
        mock_interface_2 = mock.MagicMock()
        mock_interface_3 = mock.MagicMock()
        introspection._Introspection__interfaces_list = {
            "interface_1": mock_interface_1,
            "interface_2": mock_interface_2,
            "interface_3": mock_interface_3,
        }

        introspection.remove_interface("interface_2")
        self.assertEqual(
            introspection._Introspection__interfaces_list,
            {
                "interface_1": mock_interface_1,
                "interface_3": mock_interface_3,
            },
        )

        introspection.remove_interface("interface_4")
        self.assertEqual(
            introspection._Introspection__interfaces_list,
            {
                "interface_1": mock_interface_1,
                "interface_3": mock_interface_3,
            },
        )

        introspection.remove_interface("interface_1")
        self.assertEqual(
            introspection._Introspection__interfaces_list,
            {
                "interface_3": mock_interface_3,
            },
        )

    def test_introspection_get_interface(self):
        introspection = Introspection()

        mock_interface_1 = mock.MagicMock()
        mock_interface_2 = mock.MagicMock()
        mock_interface_3 = mock.MagicMock()
        introspection._Introspection__interfaces_list = {
            "interface_1": mock_interface_1,
            "interface_2": mock_interface_2,
            "interface_3": mock_interface_3,
        }

        interface = introspection.get_interface("interface_2")
        self.assertEqual(interface, mock_interface_2)

        interface = introspection.get_interface("interface_3")
        self.assertEqual(interface, mock_interface_3)

        interface = introspection.get_interface("interface_3")
        self.assertEqual(interface, mock_interface_3)

        interface = introspection.get_interface("interface_4")
        self.assertEqual(interface, None)

    def test_introspection_get_all_interfaces(self):
        introspection = Introspection()

        interfaces = introspection.get_all_interfaces()
        self.assertEqual(list(interfaces), [])

        mock_interface_1 = mock.MagicMock()
        mock_interface_2 = mock.MagicMock()
        mock_interface_3 = mock.MagicMock()
        introspection._Introspection__interfaces_list = {
            "interface_1": mock_interface_1,
            "interface_2": mock_interface_2,
            "interface_3": mock_interface_3,
        }

        interfaces = introspection.get_all_interfaces()
        self.assertEqual(list(interfaces), [mock_interface_1, mock_interface_2, mock_interface_3])

    def test_introspection_get_all_server_owned_interfaces(self):
        introspection = Introspection()

        interfaces = introspection.get_all_server_owned_interfaces()
        self.assertEqual(interfaces, [])

        mock_interface_1 = mock.MagicMock()
        mock_interface_1.is_server_owned.return_value = True
        mock_interface_2 = mock.MagicMock()
        mock_interface_2.is_server_owned.return_value = False
        mock_interface_3 = mock.MagicMock()
        mock_interface_3.is_server_owned.return_value = True
        introspection._Introspection__interfaces_list = {
            "interface_1": mock_interface_1,
            "interface_2": mock_interface_2,
            "interface_3": mock_interface_3,
        }

        interfaces = introspection.get_all_server_owned_interfaces()
        mock_interface_1.is_server_owned.assert_called_once()
        mock_interface_2.is_server_owned.assert_called_once()
        mock_interface_3.is_server_owned.assert_called_once()
        self.assertEqual(interfaces, [mock_interface_1, mock_interface_3])

        mock_interface_1 = mock.MagicMock()
        mock_interface_1.is_server_owned.return_value = False
        mock_interface_2 = mock.MagicMock()
        mock_interface_2.is_server_owned.return_value = False
        mock_interface_3 = mock.MagicMock()
        mock_interface_3.is_server_owned.return_value = False
        introspection._Introspection__interfaces_list = {
            "interface_1": mock_interface_1,
            "interface_2": mock_interface_2,
            "interface_3": mock_interface_3,
        }

        interfaces = introspection.get_all_server_owned_interfaces()
        mock_interface_1.is_server_owned.assert_called_once()
        mock_interface_2.is_server_owned.assert_called_once()
        mock_interface_3.is_server_owned.assert_called_once()
        self.assertEqual(interfaces, [])
