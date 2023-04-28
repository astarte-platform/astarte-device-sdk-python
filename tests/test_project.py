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

import unittest
import os
from unittest.mock import MagicMock

from astarte.device import Device


class UnitTests(unittest.TestCase):
    def setUp(self):
        self.device = Device('device_id', 'realm', 'credentials_secret', 'pairing_base_url',
                                                   os.path.curdir)
        self.datastream_interface_definition = {
            "interface_name": "Test",
            "version_major": 0,
            "version_minor": 1,
            "type": "datastream",
            "ownership": "device",
            "mappings": [
                {
                    "endpoint": "/test/uno",
                    "type": "integer",
                    "database_retention_policy": "use_ttl",
                }
            ]
        }

        self.property_interface_json = {
            "interface_name": "com.astarte.PropertyTest",
            "version_major": 0,
            "version_minor": 1,
            "type": "properties",
            "ownership": "device",
            "mappings": [
                {
                    "endpoint": "/test/int",
                    "type": "integer",
                    "database_retention_policy": "use_ttl",
                    "database_retention_ttl": 31536000,
                }
            ]
        }

    def test_device_get_qos0(self):
        interface_definition_unreliable = self.datastream_interface_definition.copy()
        interface_definition_unreliable["mappings"][0]["reliability"] = "unreliable"
        self.device.add_interface(interface_definition_unreliable)
        self.assertIs(self.device._get_qos(interface_definition_unreliable["interface_name"]), 0,
                      msg="qos should be '0' for 'unreliable' reliability")

    def test_device_get_qos1(self):
        interface_definition_guaranteed = self.datastream_interface_definition.copy()
        interface_definition_guaranteed["mappings"][0]["reliability"] = "guaranteed"
        self.device.add_interface(interface_definition_guaranteed)
        self.assertIs(self.device._get_qos(interface_definition_guaranteed["interface_name"]), 1,
                      msg="qos should be '1' for 'guaranteed' reliability")

    def test_device_get_qos2(self):
        interface_definition_unique = self.datastream_interface_definition.copy()
        interface_definition_unique["mappings"][0]["reliability"] = "unique"
        self.device.add_interface(interface_definition_unique)
        self.assertIs(self.device._get_qos(interface_definition_unique["interface_name"]), 2,
                      msg="qos should be '2' for 'unique' reliability")

    def test_unset_property(self):
        self.device.add_interface(self.property_interface_json)
        self.device._send_generic = MagicMock(return_value=None)
        self.device.unset_property("com.astarte.PropertyTest", "/test/int")
