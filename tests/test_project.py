#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
import astarte.device
import os


class UnitTests(unittest.TestCase):
    def __init__(self, methodName=None):
        # Initialize a dummy device
        super().__init__(methodName)
        self.device = astarte.device.Device('device_id', 'realm', 'credentials_secret', 'pairing_base_url',
                                            os.path.curdir)

    def test_import(self):
        self.assertIsNotNone(astarte.device)

    def test_device_get_qos(self):
        interface_definition_unreliable = {
            "interface_name": "com.test.qos.Unreliable",
            "reliability": "unreliable",
        }
        self.device.add_interface(interface_definition_unreliable)

        interface_definition_guaranteed = {
            "interface_name": "com.test.qos.Guaranteed",
            "reliability": "guaranteed",
        }
        self.device.add_interface(interface_definition_guaranteed)

        interface_definition_unique = {
            "interface_name": "com.test.qos.Unique",
            "reliability": "unique",
        }
        self.device.add_interface(interface_definition_unique)

        self.assertIs(self.device._Device__get_qos(interface_definition_unreliable["interface_name"]), 0,
                      msg="qos should be '0' for 'unreliable' reliability")
        self.assertIs(self.device._Device__get_qos(interface_definition_guaranteed["interface_name"]), 1,
                      msg="qos should be '1' for 'guaranteed' reliability")
        self.assertIs(self.device._Device__get_qos(interface_definition_unique["interface_name"]), 2,
                      msg="qos should be '2' for 'unique' reliability")

    def test_project(self):
        self.assertTrue(False, "write more tests here")
