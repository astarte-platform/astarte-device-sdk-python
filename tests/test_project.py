#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
import os
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
