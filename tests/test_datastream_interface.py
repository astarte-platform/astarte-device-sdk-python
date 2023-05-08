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
from datetime import datetime
from astarte.device import Interface


class UnitTests(unittest.TestCase):
    def setUp(self):
        interface_json = {
            "interface_name": "com.astarte.Test",
            "version_major": 0,
            "version_minor": 1,
            "type": "datastream",
            "ownership": "device",
            "mappings": [
                {
                    "endpoint": "/test/int",
                    "type": "integer",
                    "database_retention_policy": "use_ttl",
                    "database_retention_ttl": 31536000,
                },
                {
                    "endpoint": "/test/longinteger",
                    "type": "longinteger",
                    "database_retention_policy": "use_ttl",
                    "database_retention_ttl": 31536000,
                },
                {
                    "endpoint": "/test/double",
                    "type": "double",
                    "database_retention_policy": "use_ttl",
                    "database_retention_ttl": 31536000,
                },
                {
                    "endpoint": "/test/string",
                    "type": "string",
                    "database_retention_policy": "use_ttl",
                    "database_retention_ttl": 31536000,
                },
                {
                    "endpoint": "/test/boolean",
                    "type": "boolean",
                    "database_retention_policy": "use_ttl",
                    "database_retention_ttl": 31536000,
                },
                {
                    "endpoint": "/test/intArray",
                    "type": "integerarray",
                    "database_retention_policy": "use_ttl",
                    "database_retention_ttl": 31536000,
                },
                {
                    "endpoint": "/test/longintegerArray",
                    "type": "longintegerarray",
                    "database_retention_policy": "use_ttl",
                    "database_retention_ttl": 31536000,
                },
                {
                    "endpoint": "/test/doubleArray",
                    "type": "doublearray",
                    "database_retention_policy": "use_ttl",
                    "database_retention_ttl": 31536000,
                },
                {
                    "endpoint": "/test/stringArray",
                    "type": "stringarray",
                    "database_retention_policy": "use_ttl",
                    "database_retention_ttl": 31536000,
                },
                {
                    "endpoint": "/test/booleanArray",
                    "type": "booleanarray",
                    "database_retention_policy": "use_ttl",
                    "database_retention_ttl": 31536000,
                },
                {
                    "endpoint": "/test/datetime",
                    "type": "datetime",
                    "database_retention_policy": "use_ttl",
                    "database_retention_ttl": 31536000,
                },
                {
                    "endpoint": "/test/datetimeArray",
                    "type": "datetimearray",
                    "database_retention_policy": "use_ttl",
                    "database_retention_ttl": 31536000,
                },
            ],
        }
        self.interface = Interface(interface_json)

    def test_validate_property(self):
        payload = 1
        result, msg = self.interface.validate("/test/int", payload, None)
        self.assertTrue(result)
        self.assertIs(msg, "")

    def test_validate_property_w_date(self):
        payload = 1
        result, msg = self.interface.validate("/test/int", payload, datetime.now())
        self.assertFalse(result)
        self.assertIsNot(msg, "")

    def test_validate_property_wrong_int(self):
        payload = 1.0
        result, msg = self.interface.validate("/test/int", payload, None)
        self.assertFalse(result)
        self.assertIsNot(msg, "")

    def test_validate_property_long_int(self):
        payload = 1
        result, msg = self.interface.validate("/test/longinteger", payload, None)
        self.assertTrue(result)
        self.assertIs(msg, "")

    def test_validate_property_32b_int(self):
        payload = 2147483647
        result, msg = self.interface.validate("/test/int", payload, None)
        self.assertTrue(result)
        self.assertIs(msg, "")

    def test_validate_property_64b_int(self):
        payload = 2147483648
        result, msg = self.interface.validate("/test/int", payload, None)
        self.assertFalse(result)
        self.assertIsNot(msg, "")

    def test_validate_property_64b_longint(self):
        payload = 2147483648
        result, msg = self.interface.validate("/test/longinteger", payload, None)
        self.assertTrue(result)
        self.assertIs(msg, "")

    def test_validate_property_double(self):
        payload = 1.0
        result, msg = self.interface.validate("/test/double", payload, None)
        self.assertTrue(result)
        self.assertIs(msg, "")

    def test_validate_property_double_nan(self):
        payload = float("Nan")
        result, msg = self.interface.validate("/test/double", payload, None)
        self.assertFalse(result)
        self.assertIsNot(msg, "")

    def test_validate_property_double_infinity(self):
        payload = float("inf")
        result, msg = self.interface.validate("/test/double", payload, None)
        self.assertFalse(result)
        self.assertIsNot(msg, "")

    def test_validate_property_wrong_double(self):
        payload = "two"
        result, msg = self.interface.validate("/test/double", payload, None)
        self.assertFalse(result)
        self.assertIsNot(msg, "")

    def test_validate_property_string(self):
        payload = "foo"
        result, msg = self.interface.validate("/test/string", payload, None)
        self.assertTrue(result)
        self.assertIs(msg, "")

    def test_validate_property_wrong_string(self):
        payload = 1
        result, msg = self.interface.validate("/test/string", payload, None)
        self.assertFalse(result)
        self.assertIsNot(msg, "")

    def test_validate_property_boolean(self):
        payload = False
        result, msg = self.interface.validate("/test/boolean", payload, None)
        self.assertTrue(result)
        self.assertIs(msg, "")

    def test_validate_property_wrong_boolean(self):
        payload = 3
        result, msg = self.interface.validate("/test/boolean", payload, None)
        self.assertFalse(result)
        self.assertIsNot(msg, "")

    def test_validate_property_intarray(self):
        payload = [1, 2]
        result, msg = self.interface.validate("/test/intArray", payload, None)
        self.assertTrue(result)
        self.assertIs(msg, "")

    def test_validate_property_wrong_intarray(self):
        payload = [1, 1.0]
        result, msg = self.interface.validate("/test/intArray", payload, None)
        self.assertFalse(result)
        self.assertIsNot(msg, "")

    def test_validate_property_longintarray(self):
        payload = [1, -1]
        result, msg = self.interface.validate("/test/longintegerArray", payload, None)
        self.assertTrue(result)
        self.assertIs(msg, "")

    def test_validate_property_32b_intarray(self):
        payload = [-2147483648, 2147483647]
        result, msg = self.interface.validate("/test/intArray", payload, None)
        self.assertTrue(result)
        self.assertIs(msg, "")

    def test_validate_property_64b_intarray(self):
        payload = [-1, 2147483648]
        result, msg = self.interface.validate("/test/intArray", payload, None)
        self.assertFalse(result)
        self.assertIsNot(msg, "")

    def test_validate_property_64b_longintarray(self):
        payload = [-1, 2147483648]
        result, msg = self.interface.validate("/test/longintegerArray", payload, None)
        self.assertTrue(result)
        self.assertIs(msg, "")

    def test_validate_property_doublearray(self):
        payload = [1.0, -1.0]
        result, msg = self.interface.validate("/test/doubleArray", payload, None)
        self.assertTrue(result)
        self.assertIs(msg, "")

    def test_validate_property_doublearray_nan(self):
        payload = [0.0, float("Nan")]
        result, msg = self.interface.validate("/test/doubleArray", payload, None)
        self.assertFalse(result)
        self.assertIsNot(msg, "")

    def test_validate_property_doublearray_infinity(self):
        payload = [0.0, float("inf")]
        result, msg = self.interface.validate("/test/doubleArray", payload, None)
        self.assertFalse(result)
        self.assertIsNot(msg, "")

    def test_validate_property_wrong_doublearray(self):
        payload = [1.0, 0]
        result, msg = self.interface.validate("/test/doubleArray", payload, None)
        self.assertFalse(result)
        self.assertIsNot(msg, "")

    def test_validate_property_stringarray(self):
        payload = ["foo", "bar"]
        result, msg = self.interface.validate("/test/stringArray", payload, None)
        self.assertTrue(result)
        self.assertIs(msg, "")

    def test_validate_property_wrong_stringarray(self):
        payload = ["one", 2]
        result, msg = self.interface.validate("/test/stringArray", payload, None)
        self.assertFalse(result)
        self.assertIsNot(msg, "")

    def test_validate_property_booleanarray(self):
        payload = [False, True]
        result, msg = self.interface.validate("/test/booleanArray", payload, None)
        self.assertTrue(result)
        self.assertIs(msg, "")

    def test_validate_property_wrong_booleanarray(self):
        payload = [False, "True"]
        result, msg = self.interface.validate("/test/booleanArray", payload, None)
        self.assertFalse(result)
        self.assertIsNot(msg, "")

    def test_validate_property_datetime(self):
        payload = datetime.now()
        result, msg = self.interface.validate("/test/datetime", payload, None)
        self.assertTrue(result)
        self.assertIs(msg, "")

    def test_validate_property_wrong_datetime(self):
        payload = "2021-09-14"
        result, msg = self.interface.validate("/test/datetime", payload, None)
        self.assertFalse(result)
        self.assertIsNot(msg, "")

    def test_validate_property_datetimeanarray(self):
        payload = [datetime.now(), datetime.now()]
        result, msg = self.interface.validate("/test/datetimeArray", payload, None)
        self.assertTrue(result)
        self.assertIs(msg, "")

    def test_validate_property_wrong_datetimeanarray(self):
        payload = [datetime.now(), "2021-09-14"]
        result, msg = self.interface.validate("/test/datetimeArray", payload, None)
        self.assertFalse(result)
        self.assertIsNot(msg, "")
