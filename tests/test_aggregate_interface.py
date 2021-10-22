#!/usr/bin/env python
# -*- coding: utf-8 -*-
from datetime import datetime
import unittest
from astarte.device import Interface


class UnitTests(unittest.TestCase):
    def setUp(self):
        interface_json = {
            "interface_name": "com.astarte.Test",
            "version_major": 0,
            "version_minor": 1,
            "type": "datastream",
            "ownership": "device",
            "aggregation": "object",
            "mappings": [
                {
                    "endpoint": "/test/one",
                    "type": "integer",
                    "database_retention_policy": "use_ttl",
                    "database_retention_ttl": 31536000,
                    "explicit_timestamp": True
                },
                {
                    "endpoint": "/test/two",
                    "type": "integer",
                    "database_retention_policy": "use_ttl",
                    "database_retention_ttl": 31536000,
                    "explicit_timestamp": True
                }
            ]
        }
        self.interface = Interface(interface_json)

    def test_validate_property(self):
        payload = {
            "one": 1,
            "two": 2
        }
        result, msg = self.interface.validate("/test", payload, datetime.now())
        self.assertTrue(result)
        self.assertIs(msg, "")

    def test_validate_less_properties(self):
        payload = {
            "one": 1
        }
        result, msg = self.interface.validate("/test", payload, datetime.now())
        self.assertFalse(result)

    def test_validate_too_much_properties(self):
        payload = {
            "one": 1,
            "two": 2,
            "three": 3
        }
        result, msg = self.interface.validate("/test", payload, datetime.now())
        self.assertFalse(result)
