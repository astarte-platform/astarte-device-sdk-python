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
# pylint: disable=too-many-statements,too-many-instance-attributes, no-self-use
# pylint: disable=too-many-public-methods

import unittest
from datetime import datetime
from math import nan

from astarte.device import InterfaceFileDecodeError, Mapping, ValidationError


class UnitTests(unittest.TestCase):
    def setUp(self):
        self.mapping_integer_dict = {
            "endpoint": "/test/one",
            "type": "integer",
        }
        self.mapping_longinteger_dict = {
            "endpoint": "/test/one",
            "type": "longinteger",
        }
        self.mapping_double_dict = {
            "endpoint": "/test/one",
            "type": "double",
        }
        self.mapping_string_dict = {
            "endpoint": "/test/one",
            "type": "string",
        }
        self.mapping_binaryblob_dict = {
            "endpoint": "/test/one",
            "type": "binaryblob",
        }
        self.mapping_boolean_dict = {
            "endpoint": "/test/one",
            "type": "boolean",
        }
        self.mapping_datetime_dict = {
            "endpoint": "/test/one",
            "type": "datetime",
        }
        self.mapping_integerarray_dict = {
            "endpoint": "/test/one",
            "type": "integerarray",
        }
        self.mapping_longintegerarray_dict = {
            "endpoint": "/test/one",
            "type": "longintegerarray",
        }
        self.mapping_doublearray_dict = {
            "endpoint": "/test/one",
            "type": "doublearray",
        }
        self.mapping_stringarray_dict = {
            "endpoint": "/test/one",
            "type": "stringarray",
        }
        self.mapping_binaryblobarray_dict = {
            "endpoint": "/test/one",
            "type": "binaryblobarray",
        }
        self.mapping_booleanarray_dict = {
            "endpoint": "/test/one",
            "type": "booleanarray",
        }
        self.mapping_datetimearray_dict = {
            "endpoint": "/test/one",
            "type": "datetimearray",
        }

    def test_mapping_init(self):
        basic_mapping = {
            "endpoint": "/test/one",
            "type": "boolean",
        }
        mapping_basic = Mapping(basic_mapping, True)
        self.assertEqual(mapping_basic.endpoint, "/test/one")
        self.assertEqual(mapping_basic.type, "boolean")
        self.assertEqual(mapping_basic.explicit_timestamp, False)
        self.assertEqual(mapping_basic.reliability, 0)

        explicit_timestamp_mapping = {
            "endpoint": "/test/two",
            "type": "integer",
            "explicit_timestamp": True,
        }
        mapping_basic = Mapping(explicit_timestamp_mapping, True)
        self.assertEqual(mapping_basic.endpoint, "/test/two")
        self.assertEqual(mapping_basic.type, "integer")
        self.assertEqual(mapping_basic.explicit_timestamp, True)
        self.assertEqual(mapping_basic.reliability, 0)

        unreliable_mapping = {
            "endpoint": "/test/one",
            "type": "boolean",
            "reliability": "unreliable",
        }
        mapping_basic = Mapping(unreliable_mapping, True)
        self.assertEqual(mapping_basic.endpoint, "/test/one")
        self.assertEqual(mapping_basic.type, "boolean")
        self.assertEqual(mapping_basic.explicit_timestamp, False)
        self.assertEqual(mapping_basic.reliability, 0)

        guaranteed_mapping = {
            "endpoint": "/test/one",
            "type": "boolean",
            "reliability": "guaranteed",
        }
        mapping_basic = Mapping(guaranteed_mapping, True)
        self.assertEqual(mapping_basic.endpoint, "/test/one")
        self.assertEqual(mapping_basic.type, "boolean")
        self.assertEqual(mapping_basic.explicit_timestamp, False)
        self.assertEqual(mapping_basic.reliability, 1)

        unique_mapping = {
            "endpoint": "/test/one",
            "type": "boolean",
            "reliability": "unique",
        }
        mapping_basic = Mapping(unique_mapping, True)
        self.assertEqual(mapping_basic.endpoint, "/test/one")
        self.assertEqual(mapping_basic.type, "boolean")
        self.assertEqual(mapping_basic.explicit_timestamp, False)
        self.assertEqual(mapping_basic.reliability, 2)

        basic_mapping = {
            "endpoint": "/test/one",
            "type": "boolean",
        }
        mapping_basic = Mapping(basic_mapping, False)
        self.assertEqual(mapping_basic.endpoint, "/test/one")
        self.assertEqual(mapping_basic.type, "boolean")
        self.assertEqual(mapping_basic.explicit_timestamp, False)
        self.assertEqual(mapping_basic.reliability, 2)
        self.assertEqual(mapping_basic.allow_unset, False)

        unsettable_mapping = {
            "endpoint": "/test/one",
            "type": "boolean",
            "allow_unset": True,
        }
        mapping_basic = Mapping(unsettable_mapping, False)
        self.assertEqual(mapping_basic.endpoint, "/test/one")
        self.assertEqual(mapping_basic.type, "boolean")
        self.assertEqual(mapping_basic.explicit_timestamp, False)
        self.assertEqual(mapping_basic.reliability, 2)
        self.assertEqual(mapping_basic.allow_unset, True)

    def test_mapping_initialize_missing_endpoint_raises(self):
        basic_mapping = {
            "type": "boolean",
        }
        self.assertRaises(InterfaceFileDecodeError, lambda: Mapping(basic_mapping, True))

    def test_mapping_initialize_missing_type_raises(self):
        basic_mapping = {
            "endpoint": "/test/one",
        }
        self.assertRaises(InterfaceFileDecodeError, lambda: Mapping(basic_mapping, True))

    def test_mapping_initialize_incorrect_endpoint_raises(self):
        basic_mapping = {
            "endpoint": 42,
            "type": "boolean",
        }
        self.assertRaises(InterfaceFileDecodeError, lambda: Mapping(basic_mapping, True))

        basic_mapping["endpoint"] = r""
        self.assertRaises(InterfaceFileDecodeError, lambda: Mapping(basic_mapping, True))

        basic_mapping["endpoint"] = r"foo"
        self.assertRaises(InterfaceFileDecodeError, lambda: Mapping(basic_mapping, True))

        basic_mapping["endpoint"] = r"/"
        self.assertRaises(InterfaceFileDecodeError, lambda: Mapping(basic_mapping, True))

        basic_mapping["endpoint"] = r"/foo/"
        self.assertRaises(InterfaceFileDecodeError, lambda: Mapping(basic_mapping, True))

        basic_mapping["endpoint"] = r"/1foo"
        self.assertRaises(InterfaceFileDecodeError, lambda: Mapping(basic_mapping, True))

        basic_mapping["endpoint"] = r"//foo"
        self.assertRaises(InterfaceFileDecodeError, lambda: Mapping(basic_mapping, True))

        basic_mapping["endpoint"] = r"/foo/fo+o"
        self.assertRaises(InterfaceFileDecodeError, lambda: Mapping(basic_mapping, True))

        basic_mapping["endpoint"] = r"/foo/fo#o"
        self.assertRaises(InterfaceFileDecodeError, lambda: Mapping(basic_mapping, True))

        basic_mapping["endpoint"] = r"/foo/fo%o"
        self.assertRaises(InterfaceFileDecodeError, lambda: Mapping(basic_mapping, True))

        basic_mapping["endpoint"] = r"/%%{foo}/foo"
        self.assertRaises(InterfaceFileDecodeError, lambda: Mapping(basic_mapping, True))

        basic_mapping["endpoint"] = r"/%foo}/foo"
        self.assertRaises(InterfaceFileDecodeError, lambda: Mapping(basic_mapping, True))

        basic_mapping["endpoint"] = r"/%{foo/foo"
        self.assertRaises(InterfaceFileDecodeError, lambda: Mapping(basic_mapping, True))

        basic_mapping["endpoint"] = r"/%{fo+o}/foo"
        self.assertRaises(InterfaceFileDecodeError, lambda: Mapping(basic_mapping, True))

        basic_mapping["endpoint"] = r"/%{fo#o}/foo"
        self.assertRaises(InterfaceFileDecodeError, lambda: Mapping(basic_mapping, True))

        basic_mapping["endpoint"] = r"/%{fo%o}/foo"
        self.assertRaises(InterfaceFileDecodeError, lambda: Mapping(basic_mapping, True))

        basic_mapping["endpoint"] = r"/foo.foo"
        self.assertRaises(InterfaceFileDecodeError, lambda: Mapping(basic_mapping, True))

        basic_mapping["endpoint"] = r"/{foo}/foo"
        self.assertRaises(InterfaceFileDecodeError, lambda: Mapping(basic_mapping, True))

        # Some correct endpoints for reference
        basic_mapping["endpoint"] = r"/foo"
        Mapping(basic_mapping, True)
        basic_mapping["endpoint"] = r"/f00/f00"
        Mapping(basic_mapping, True)
        basic_mapping["endpoint"] = r"/%{param1}/%{param2}"
        Mapping(basic_mapping, True)
        basic_mapping["endpoint"] = r"/foo/%{param2}"
        Mapping(basic_mapping, True)

    def test_mapping_initialize_incorrect_type_raises(self):
        basic_mapping = {
            "endpoint": "/test/one",
            "type": 42,
        }
        self.assertRaises(InterfaceFileDecodeError, lambda: Mapping(basic_mapping, True))

    def test_mapping_initialize_non_existing_type_raises(self):
        basic_mapping = {
            "endpoint": "/test/one",
            "type": "foo",
        }
        self.assertRaises(InterfaceFileDecodeError, lambda: Mapping(basic_mapping, True))

    def test_mapping_initialize_explicit_timestamp_incorrect_type_raises(self):
        basic_mapping = {
            "endpoint": "/test/one",
            "type": "integer",
            "explicit_timestamp": " ",
        }
        self.assertRaises(InterfaceFileDecodeError, lambda: Mapping(basic_mapping, True))

    def test_mapping_initialize_reliability_incorrect_value_raises(self):
        basic_mapping = {
            "endpoint": "/test/one",
            "type": "integer",
            "reliability": "something else",
        }
        self.assertRaises(InterfaceFileDecodeError, lambda: Mapping(basic_mapping, True))

    def test_mapping_initialize_allow_unset_incorrect_type_raises(self):
        basic_mapping = {
            "endpoint": "/test/one",
            "type": "integer",
            "allow_unset": 42,
        }
        self.assertRaises(InterfaceFileDecodeError, lambda: Mapping(basic_mapping, False))

    def test_mapping_initialize_explicit_timestamp_in_property_raises(self):
        basic_mapping = {
            "endpoint": "/test/one",
            "type": "integer",
            "explicit_timestamp": True,
        }
        self.assertRaises(InterfaceFileDecodeError, lambda: Mapping(basic_mapping, False))

    def test_mapping_initialize_reliability_in_property_raises(self):
        basic_mapping = {
            "endpoint": "/test/one",
            "type": "integer",
            "reliability": "unreliable",
        }
        self.assertRaises(InterfaceFileDecodeError, lambda: Mapping(basic_mapping, False))

    def test_mapping_initialize_unsettability_in_datastreams_raises(self):
        basic_mapping = {
            "endpoint": "/test/one",
            "type": "integer",
            "allow_unset": True,
        }
        self.assertRaises(InterfaceFileDecodeError, lambda: Mapping(basic_mapping, True))

    def test_mapping_validate_path(self):
        basic_mapping = {
            "endpoint": "/test/one",
            "type": "boolean",
        }
        mapping_basic = Mapping(basic_mapping, True)
        mapping_basic.validate_path("/test/one")
        self.assertRaises(ValidationError, lambda: mapping_basic.validate_path("test/one"))
        self.assertRaises(ValidationError, lambda: mapping_basic.validate_path("/tests/one"))
        self.assertRaises(ValidationError, lambda: mapping_basic.validate_path("/test/one/more"))
        self.assertRaises(ValidationError, lambda: mapping_basic.validate_path("/test/one/"))
        self.assertRaises(ValidationError, lambda: mapping_basic.validate_path("more/test/one"))

        param_mapping = {
            "endpoint": r"/%{param}/one",
            "type": "boolean",
        }
        mapping_param = Mapping(param_mapping, True)
        mapping_param.validate_path("/a21smt/one")
        self.assertRaises(ValidationError, lambda: mapping_param.validate_path("/more/21/one"))
        self.assertRaises(ValidationError, lambda: mapping_param.validate_path("/a21/more/one"))
        self.assertRaises(ValidationError, lambda: mapping_param.validate_path("/a21+/one"))
        self.assertRaises(ValidationError, lambda: mapping_param.validate_path("/a21#/one"))

        two_param_mapping = {
            "endpoint": r"/%{param1}/%{param2}/one",
            "type": "boolean",
        }
        mapping_two_param = Mapping(two_param_mapping, True)
        mapping_two_param.validate_path("/aa/bb/one")
        self.assertRaises(ValidationError, lambda: mapping_two_param.validate_path("/a21/one"))
        self.assertRaises(
            ValidationError, lambda: mapping_two_param.validate_path("/aa/bb/more/one")
        )
        self.assertRaises(
            ValidationError, lambda: mapping_two_param.validate_path("more/aa/bb/one")
        )

    def test_mapping_validate_timestamp(self):
        basic_mapping = {
            "endpoint": "/test/one",
            "type": "integer",
        }
        mapping_integer = Mapping(basic_mapping, True)
        mapping_integer.validate_timestamp(None)

        basic_mapping["explicit_timestamp"] = True
        mapping_integer = Mapping(basic_mapping, True)
        mapping_integer.validate_timestamp(datetime.now())

    def test_mapping_validate_timestamp_missing_timestamp_err(self):
        basic_mapping = {"endpoint": "/test/one", "type": "integer", "explicit_timestamp": True}
        mapping_integer = Mapping(basic_mapping, True)
        self.assertRaises(ValidationError, lambda: mapping_integer.validate_timestamp(None))

    def test_mapping_validate_timestamp_extra_timestamp_err(self):
        basic_mapping = {
            "endpoint": "/test/one",
            "type": "integer",
        }
        mapping_integer = Mapping(basic_mapping, True)
        self.assertRaises(
            ValidationError, lambda: mapping_integer.validate_timestamp(datetime.now())
        )

    def test_mapping_validate_payload(self):
        mapping_integer = Mapping(self.mapping_integer_dict, True)
        mapping_integer.validate_payload(42)

        mapping_longinteger = Mapping(self.mapping_longinteger_dict, True)
        mapping_longinteger.validate_payload(34359738368)

        mapping_double = Mapping(self.mapping_double_dict, True)
        mapping_double.validate_payload(42.0)

        mapping_string = Mapping(self.mapping_string_dict, True)
        mapping_string.validate_payload("my string")

        mapping_binaryblob = Mapping(self.mapping_binaryblob_dict, True)
        mapping_binaryblob.validate_payload(b"hello")

        mapping_boolean = Mapping(self.mapping_boolean_dict, True)
        mapping_boolean.validate_payload(True)

        mapping_datetime = Mapping(self.mapping_datetime_dict, True)
        mapping_datetime.validate_payload(datetime.now())

        mapping_integerarray = Mapping(self.mapping_integerarray_dict, True)
        mapping_integerarray.validate_payload([42])

        mapping_longintegerarray = Mapping(self.mapping_longintegerarray_dict, True)
        mapping_longintegerarray.validate_payload([42])

        mapping_doublearray = Mapping(self.mapping_doublearray_dict, True)
        mapping_doublearray.validate_payload([42.1])

        mapping_stringarray = Mapping(self.mapping_stringarray_dict, True)
        mapping_stringarray.validate_payload(["my string"])

        mapping_binaryblobarray = Mapping(self.mapping_binaryblobarray_dict, True)
        mapping_binaryblobarray.validate_payload([b"hello", b"world"])

        mapping_booleanarray = Mapping(self.mapping_booleanarray_dict, True)
        mapping_booleanarray.validate_payload([True, False])

        mapping_datetimearray = Mapping(self.mapping_datetimearray_dict, True)
        mapping_datetimearray.validate_payload([datetime.now()])

    def test_mapping_validate_payload_empty_arrays(self):
        mapping_integerarray = Mapping(self.mapping_integerarray_dict, True)
        mapping_integerarray.validate_payload([])

        mapping_longintegerarray = Mapping(self.mapping_longintegerarray_dict, True)
        mapping_longintegerarray.validate_payload([])

        mapping_doublearray = Mapping(self.mapping_doublearray_dict, True)
        mapping_doublearray.validate_payload([])

        mapping_stringarray = Mapping(self.mapping_stringarray_dict, True)
        mapping_stringarray.validate_payload([])

        mapping_binaryblobarray = Mapping(self.mapping_binaryblobarray_dict, True)
        mapping_binaryblobarray.validate_payload([])

        mapping_booleanarray = Mapping(self.mapping_booleanarray_dict, True)
        mapping_booleanarray.validate_payload([])

        mapping_datetimearray = Mapping(self.mapping_datetimearray_dict, True)
        mapping_datetimearray.validate_payload([])

    def test_mapping_validate_payload_empty_payload_err(self):
        mapping_basic = Mapping(self.mapping_double_dict, True)

        self.assertRaises(ValidationError, lambda: mapping_basic.validate_payload(None))
        mapping_basic.validate_payload(0.0)

    def test_mapping_validate_payload_integer_mapping_incorrect_type_err(self):
        mapping_integer = Mapping(self.mapping_integer_dict, True)
        self.assertRaises(ValidationError, lambda: mapping_integer.validate_payload(None))
        self.assertRaises(ValidationError, lambda: mapping_integer.validate_payload(0.0))
        self.assertRaises(ValidationError, lambda: mapping_integer.validate_payload(25.4))
        self.assertRaises(ValidationError, lambda: mapping_integer.validate_payload("hello"))
        self.assertRaises(ValidationError, lambda: mapping_integer.validate_payload(b"hello"))
        self.assertRaises(ValidationError, lambda: mapping_integer.validate_payload(True))
        self.assertRaises(ValidationError, lambda: mapping_integer.validate_payload(datetime.now()))
        self.assertRaises(ValidationError, lambda: mapping_integer.validate_payload([25]))
        self.assertRaises(ValidationError, lambda: mapping_integer.validate_payload([25.4]))
        self.assertRaises(ValidationError, lambda: mapping_integer.validate_payload(["hello"]))
        self.assertRaises(ValidationError, lambda: mapping_integer.validate_payload([b"hello"]))
        self.assertRaises(ValidationError, lambda: mapping_integer.validate_payload([True]))
        self.assertRaises(
            ValidationError, lambda: mapping_integer.validate_payload([datetime.now()])
        )

    def test_mapping_validate_payload_longinteger_mapping_incorrect_type_err(self):
        mapping_longinteger = Mapping(self.mapping_longinteger_dict, True)
        self.assertRaises(ValidationError, lambda: mapping_longinteger.validate_payload(None))
        self.assertRaises(ValidationError, lambda: mapping_longinteger.validate_payload(0.0))
        self.assertRaises(ValidationError, lambda: mapping_longinteger.validate_payload(25.4))
        self.assertRaises(ValidationError, lambda: mapping_longinteger.validate_payload("hello"))
        self.assertRaises(ValidationError, lambda: mapping_longinteger.validate_payload(b"hello"))
        self.assertRaises(ValidationError, lambda: mapping_longinteger.validate_payload(True))
        self.assertRaises(
            ValidationError, lambda: mapping_longinteger.validate_payload(datetime.now())
        )
        self.assertRaises(ValidationError, lambda: mapping_longinteger.validate_payload([25]))
        self.assertRaises(ValidationError, lambda: mapping_longinteger.validate_payload([25.4]))
        self.assertRaises(ValidationError, lambda: mapping_longinteger.validate_payload(["hello"]))
        self.assertRaises(ValidationError, lambda: mapping_longinteger.validate_payload([b"hello"]))
        self.assertRaises(ValidationError, lambda: mapping_longinteger.validate_payload([True]))
        self.assertRaises(
            ValidationError, lambda: mapping_longinteger.validate_payload([datetime.now()])
        )

    def test_mapping_validate_payload_double_mapping_incorrect_type_err(self):
        mapping_double = Mapping(self.mapping_double_dict, True)
        self.assertRaises(ValidationError, lambda: mapping_double.validate_payload(None))
        self.assertRaises(ValidationError, lambda: mapping_double.validate_payload(0))
        self.assertRaises(ValidationError, lambda: mapping_double.validate_payload(25))
        self.assertRaises(ValidationError, lambda: mapping_double.validate_payload("hello"))
        self.assertRaises(ValidationError, lambda: mapping_double.validate_payload(b"hello"))
        self.assertRaises(ValidationError, lambda: mapping_double.validate_payload(True))
        self.assertRaises(ValidationError, lambda: mapping_double.validate_payload(datetime.now()))
        self.assertRaises(ValidationError, lambda: mapping_double.validate_payload([25]))
        self.assertRaises(ValidationError, lambda: mapping_double.validate_payload([12.4]))
        self.assertRaises(ValidationError, lambda: mapping_double.validate_payload(["hello"]))
        self.assertRaises(ValidationError, lambda: mapping_double.validate_payload([b"hello"]))
        self.assertRaises(ValidationError, lambda: mapping_double.validate_payload([True]))
        self.assertRaises(
            ValidationError, lambda: mapping_double.validate_payload([datetime.now()])
        )

    def test_mapping_validate_payload_string_mapping_incorrect_type_err(self):
        mapping_string = Mapping(self.mapping_string_dict, True)
        self.assertRaises(ValidationError, lambda: mapping_string.validate_payload(None))
        self.assertRaises(ValidationError, lambda: mapping_string.validate_payload(25))
        self.assertRaises(ValidationError, lambda: mapping_string.validate_payload(12.0))
        self.assertRaises(ValidationError, lambda: mapping_string.validate_payload(b"hello"))
        self.assertRaises(ValidationError, lambda: mapping_string.validate_payload(True))
        self.assertRaises(ValidationError, lambda: mapping_string.validate_payload(datetime.now()))
        self.assertRaises(ValidationError, lambda: mapping_string.validate_payload([25]))
        self.assertRaises(ValidationError, lambda: mapping_string.validate_payload([12.4]))
        self.assertRaises(ValidationError, lambda: mapping_string.validate_payload(["hello"]))
        self.assertRaises(ValidationError, lambda: mapping_string.validate_payload([b"hello"]))
        self.assertRaises(ValidationError, lambda: mapping_string.validate_payload([True]))
        self.assertRaises(
            ValidationError, lambda: mapping_string.validate_payload([datetime.now()])
        )

    def test_mapping_validate_payload_binaryblob_mapping_incorrect_type_err(self):
        mapping_binaryblob = Mapping(self.mapping_binaryblob_dict, True)
        self.assertRaises(ValidationError, lambda: mapping_binaryblob.validate_payload(None))
        self.assertRaises(ValidationError, lambda: mapping_binaryblob.validate_payload(25))
        self.assertRaises(ValidationError, lambda: mapping_binaryblob.validate_payload(12.0))
        self.assertRaises(ValidationError, lambda: mapping_binaryblob.validate_payload("hello"))
        self.assertRaises(ValidationError, lambda: mapping_binaryblob.validate_payload(True))
        self.assertRaises(
            ValidationError, lambda: mapping_binaryblob.validate_payload(datetime.now())
        )
        self.assertRaises(ValidationError, lambda: mapping_binaryblob.validate_payload([25]))
        self.assertRaises(ValidationError, lambda: mapping_binaryblob.validate_payload([25.4]))
        self.assertRaises(ValidationError, lambda: mapping_binaryblob.validate_payload(["hello"]))
        self.assertRaises(ValidationError, lambda: mapping_binaryblob.validate_payload([b"hello"]))
        self.assertRaises(ValidationError, lambda: mapping_binaryblob.validate_payload([True]))
        self.assertRaises(
            ValidationError, lambda: mapping_binaryblob.validate_payload([datetime.now()])
        )

    def test_mapping_validate_payload_boolean_mapping_incorrect_type_err(self):
        mapping_boolean = Mapping(self.mapping_boolean_dict, True)
        self.assertRaises(ValidationError, lambda: mapping_boolean.validate_payload(None))
        self.assertRaises(ValidationError, lambda: mapping_boolean.validate_payload(25))
        self.assertRaises(ValidationError, lambda: mapping_boolean.validate_payload(12.0))
        self.assertRaises(ValidationError, lambda: mapping_boolean.validate_payload(b"hello"))
        self.assertRaises(ValidationError, lambda: mapping_boolean.validate_payload("hello"))
        self.assertRaises(ValidationError, lambda: mapping_boolean.validate_payload(datetime.now()))
        self.assertRaises(ValidationError, lambda: mapping_boolean.validate_payload([25]))
        self.assertRaises(ValidationError, lambda: mapping_boolean.validate_payload([25.4]))
        self.assertRaises(ValidationError, lambda: mapping_boolean.validate_payload(["hello"]))
        self.assertRaises(ValidationError, lambda: mapping_boolean.validate_payload([b"hello"]))
        self.assertRaises(ValidationError, lambda: mapping_boolean.validate_payload([True]))
        self.assertRaises(
            ValidationError, lambda: mapping_boolean.validate_payload([datetime.now()])
        )

    def test_mapping_validate_payload_datetime_mapping_incorrect_type_err(self):
        mapping_datetime = Mapping(self.mapping_datetime_dict, True)
        self.assertRaises(ValidationError, lambda: mapping_datetime.validate_payload(None))
        self.assertRaises(ValidationError, lambda: mapping_datetime.validate_payload(25))
        self.assertRaises(ValidationError, lambda: mapping_datetime.validate_payload(12.0))
        self.assertRaises(ValidationError, lambda: mapping_datetime.validate_payload(b"hello"))
        self.assertRaises(ValidationError, lambda: mapping_datetime.validate_payload("hello"))
        self.assertRaises(ValidationError, lambda: mapping_datetime.validate_payload(True))
        self.assertRaises(ValidationError, lambda: mapping_datetime.validate_payload([25]))
        self.assertRaises(ValidationError, lambda: mapping_datetime.validate_payload([25.4]))
        self.assertRaises(ValidationError, lambda: mapping_datetime.validate_payload(["hello"]))
        self.assertRaises(ValidationError, lambda: mapping_datetime.validate_payload([b"hello"]))
        self.assertRaises(ValidationError, lambda: mapping_datetime.validate_payload([True]))
        self.assertRaises(
            ValidationError, lambda: mapping_datetime.validate_payload([datetime.now()])
        )

    def test_mapping_validate_payload_integerarray_mapping_incorrect_type_err(self):
        mapping_integerarray = Mapping(self.mapping_integerarray_dict, True)
        self.assertRaises(ValidationError, lambda: mapping_integerarray.validate_payload(None))
        self.assertRaises(ValidationError, lambda: mapping_integerarray.validate_payload(25))
        self.assertRaises(ValidationError, lambda: mapping_integerarray.validate_payload(12.0))
        self.assertRaises(ValidationError, lambda: mapping_integerarray.validate_payload(b"hello"))
        self.assertRaises(ValidationError, lambda: mapping_integerarray.validate_payload("hello"))
        self.assertRaises(ValidationError, lambda: mapping_integerarray.validate_payload(True))
        self.assertRaises(
            ValidationError, lambda: mapping_integerarray.validate_payload(datetime.now())
        )
        self.assertRaises(ValidationError, lambda: mapping_integerarray.validate_payload([25.4]))
        self.assertRaises(ValidationError, lambda: mapping_integerarray.validate_payload(["hello"]))
        self.assertRaises(
            ValidationError, lambda: mapping_integerarray.validate_payload([b"hello"])
        )
        self.assertRaises(ValidationError, lambda: mapping_integerarray.validate_payload([True]))
        self.assertRaises(
            ValidationError, lambda: mapping_integerarray.validate_payload([datetime.now()])
        )

    def test_mapping_validate_payload_longintegerarray_mapping_incorrect_type_err(self):
        mapping_longintegerarray = Mapping(self.mapping_longintegerarray_dict, True)
        self.assertRaises(ValidationError, lambda: mapping_longintegerarray.validate_payload(None))
        self.assertRaises(ValidationError, lambda: mapping_longintegerarray.validate_payload(25))
        self.assertRaises(ValidationError, lambda: mapping_longintegerarray.validate_payload(12.0))
        self.assertRaises(
            ValidationError, lambda: mapping_longintegerarray.validate_payload(b"hello")
        )
        self.assertRaises(
            ValidationError, lambda: mapping_longintegerarray.validate_payload("hello")
        )
        self.assertRaises(ValidationError, lambda: mapping_longintegerarray.validate_payload(True))
        self.assertRaises(
            ValidationError, lambda: mapping_longintegerarray.validate_payload(datetime.now())
        )
        self.assertRaises(
            ValidationError, lambda: mapping_longintegerarray.validate_payload([25.4])
        )
        self.assertRaises(
            ValidationError, lambda: mapping_longintegerarray.validate_payload(["hello"])
        )
        self.assertRaises(
            ValidationError, lambda: mapping_longintegerarray.validate_payload([b"hello"])
        )
        self.assertRaises(
            ValidationError, lambda: mapping_longintegerarray.validate_payload([True])
        )
        self.assertRaises(
            ValidationError, lambda: mapping_longintegerarray.validate_payload([datetime.now()])
        )

    def test_mapping_validate_payload_doublearray_mapping_incorrect_type_err(self):
        mapping_doublearray = Mapping(self.mapping_doublearray_dict, True)
        self.assertRaises(ValidationError, lambda: mapping_doublearray.validate_payload(None))
        self.assertRaises(ValidationError, lambda: mapping_doublearray.validate_payload(25))
        self.assertRaises(ValidationError, lambda: mapping_doublearray.validate_payload(12.0))
        self.assertRaises(ValidationError, lambda: mapping_doublearray.validate_payload(b"hello"))
        self.assertRaises(ValidationError, lambda: mapping_doublearray.validate_payload("hello"))
        self.assertRaises(ValidationError, lambda: mapping_doublearray.validate_payload(True))
        self.assertRaises(
            ValidationError, lambda: mapping_doublearray.validate_payload(datetime.now())
        )
        self.assertRaises(ValidationError, lambda: mapping_doublearray.validate_payload([25]))
        self.assertRaises(ValidationError, lambda: mapping_doublearray.validate_payload(["hello"]))
        self.assertRaises(ValidationError, lambda: mapping_doublearray.validate_payload([b"hello"]))
        self.assertRaises(ValidationError, lambda: mapping_doublearray.validate_payload([True]))
        self.assertRaises(
            ValidationError, lambda: mapping_doublearray.validate_payload([datetime.now()])
        )

    def test_mapping_validate_payload_stringarray_mapping_incorrect_type_err(self):
        mapping_stringarray = Mapping(self.mapping_stringarray_dict, True)
        self.assertRaises(ValidationError, lambda: mapping_stringarray.validate_payload(None))
        self.assertRaises(ValidationError, lambda: mapping_stringarray.validate_payload(25))
        self.assertRaises(ValidationError, lambda: mapping_stringarray.validate_payload(12.0))
        self.assertRaises(ValidationError, lambda: mapping_stringarray.validate_payload(b"hello"))
        self.assertRaises(ValidationError, lambda: mapping_stringarray.validate_payload("hello"))
        self.assertRaises(ValidationError, lambda: mapping_stringarray.validate_payload(True))
        self.assertRaises(
            ValidationError, lambda: mapping_stringarray.validate_payload(datetime.now())
        )
        self.assertRaises(ValidationError, lambda: mapping_stringarray.validate_payload([25]))
        self.assertRaises(ValidationError, lambda: mapping_stringarray.validate_payload([25.4]))
        self.assertRaises(ValidationError, lambda: mapping_stringarray.validate_payload([b"hello"]))
        self.assertRaises(ValidationError, lambda: mapping_stringarray.validate_payload([True]))
        self.assertRaises(
            ValidationError, lambda: mapping_stringarray.validate_payload([datetime.now()])
        )

    def test_mapping_validate_payload_binaryblobarray_mapping_incorrect_type_err(self):
        mapping_binaryblobarray = Mapping(self.mapping_binaryblobarray_dict, True)
        self.assertRaises(ValidationError, lambda: mapping_binaryblobarray.validate_payload(None))
        self.assertRaises(ValidationError, lambda: mapping_binaryblobarray.validate_payload(25))
        self.assertRaises(ValidationError, lambda: mapping_binaryblobarray.validate_payload(12.0))
        self.assertRaises(
            ValidationError, lambda: mapping_binaryblobarray.validate_payload(b"hello")
        )
        self.assertRaises(
            ValidationError, lambda: mapping_binaryblobarray.validate_payload("hello")
        )
        self.assertRaises(ValidationError, lambda: mapping_binaryblobarray.validate_payload(True))
        self.assertRaises(
            ValidationError, lambda: mapping_binaryblobarray.validate_payload(datetime.now())
        )
        self.assertRaises(ValidationError, lambda: mapping_binaryblobarray.validate_payload([25]))
        self.assertRaises(ValidationError, lambda: mapping_binaryblobarray.validate_payload([25.4]))
        self.assertRaises(
            ValidationError, lambda: mapping_binaryblobarray.validate_payload(["hello"])
        )
        self.assertRaises(ValidationError, lambda: mapping_binaryblobarray.validate_payload([True]))
        self.assertRaises(
            ValidationError, lambda: mapping_binaryblobarray.validate_payload([datetime.now()])
        )

    def test_mapping_validate_payload_booleanarray_mapping_incorrect_type_err(self):
        mapping_booleanarray = Mapping(self.mapping_booleanarray_dict, True)
        self.assertRaises(ValidationError, lambda: mapping_booleanarray.validate_payload(None))
        self.assertRaises(ValidationError, lambda: mapping_booleanarray.validate_payload(25))
        self.assertRaises(ValidationError, lambda: mapping_booleanarray.validate_payload(12.0))
        self.assertRaises(ValidationError, lambda: mapping_booleanarray.validate_payload(b"hello"))
        self.assertRaises(ValidationError, lambda: mapping_booleanarray.validate_payload("hello"))
        self.assertRaises(ValidationError, lambda: mapping_booleanarray.validate_payload(True))
        self.assertRaises(
            ValidationError, lambda: mapping_booleanarray.validate_payload(datetime.now())
        )
        self.assertRaises(ValidationError, lambda: mapping_booleanarray.validate_payload([25]))
        self.assertRaises(ValidationError, lambda: mapping_booleanarray.validate_payload([25.4]))
        self.assertRaises(ValidationError, lambda: mapping_booleanarray.validate_payload(["hello"]))
        self.assertRaises(
            ValidationError, lambda: mapping_booleanarray.validate_payload([b"hello"])
        )
        self.assertRaises(
            ValidationError, lambda: mapping_booleanarray.validate_payload([datetime.now()])
        )

    def test_mapping_validate_payload_datetimearray_mapping_incorrect_type_err(self):
        mapping_datetimearray = Mapping(self.mapping_datetimearray_dict, True)
        self.assertRaises(ValidationError, lambda: mapping_datetimearray.validate_payload(None))
        self.assertRaises(ValidationError, lambda: mapping_datetimearray.validate_payload(25))
        self.assertRaises(ValidationError, lambda: mapping_datetimearray.validate_payload(12.0))
        self.assertRaises(ValidationError, lambda: mapping_datetimearray.validate_payload(b"hello"))
        self.assertRaises(ValidationError, lambda: mapping_datetimearray.validate_payload("hello"))
        self.assertRaises(ValidationError, lambda: mapping_datetimearray.validate_payload(True))
        self.assertRaises(
            ValidationError, lambda: mapping_datetimearray.validate_payload(datetime.now())
        )
        self.assertRaises(ValidationError, lambda: mapping_datetimearray.validate_payload([25]))
        self.assertRaises(ValidationError, lambda: mapping_datetimearray.validate_payload([25.4]))
        self.assertRaises(
            ValidationError, lambda: mapping_datetimearray.validate_payload(["hello"])
        )
        self.assertRaises(
            ValidationError, lambda: mapping_datetimearray.validate_payload([b"hello"])
        )
        self.assertRaises(ValidationError, lambda: mapping_datetimearray.validate_payload([True]))

    def test_mapping_validate_payload_incoherent_type_in_list_err(self):
        mapping_integerarray = Mapping(self.mapping_integerarray_dict, True)
        self.assertRaises(
            ValidationError, lambda: mapping_integerarray.validate_payload([12, True])
        )

        mapping_longintegerarray = Mapping(self.mapping_longintegerarray_dict, True)
        self.assertRaises(
            ValidationError, lambda: mapping_longintegerarray.validate_payload([12, ""])
        )

        mapping_doublearray = Mapping(self.mapping_doublearray_dict, True)
        self.assertRaises(ValidationError, lambda: mapping_doublearray.validate_payload([12.3, 12]))

        mapping_stringarray = Mapping(self.mapping_stringarray_dict, True)
        self.assertRaises(
            ValidationError, lambda: mapping_stringarray.validate_payload(["23", b""])
        )

        mapping_binaryblobarray = Mapping(self.mapping_binaryblobarray_dict, True)
        self.assertRaises(
            ValidationError, lambda: mapping_binaryblobarray.validate_payload([b"22", ""])
        )

        mapping_booleanarray = Mapping(self.mapping_booleanarray_dict, True)
        self.assertRaises(
            ValidationError, lambda: mapping_booleanarray.validate_payload([True, 11])
        )

        mapping_datetimearray = Mapping(self.mapping_datetimearray_dict, True)
        self.assertRaises(
            ValidationError, lambda: mapping_datetimearray.validate_payload([datetime.now(), 11])
        )

    def test_mapping_validate_payload_out_of_range_integer_err(self):
        mapping_integer = Mapping(self.mapping_integer_dict, True)
        self.assertRaises(ValidationError, lambda: mapping_integer.validate_payload(-2147483649))

        mapping_integer = Mapping(self.mapping_integer_dict, True)
        self.assertRaises(ValidationError, lambda: mapping_integer.validate_payload(2147483648))

        mapping_integerarray = Mapping(self.mapping_integerarray_dict, True)
        self.assertRaises(
            ValidationError, lambda: mapping_integerarray.validate_payload([-2147483649])
        )

        mapping_integerarray = Mapping(self.mapping_integerarray_dict, True)
        self.assertRaises(
            ValidationError, lambda: mapping_integerarray.validate_payload([2147483648])
        )

    def test_mapping_validate_payload_nan_double_err(self):
        mapping_double = Mapping(self.mapping_double_dict, True)
        self.assertRaises(ValidationError, lambda: mapping_double.validate_payload(nan))

        mapping_doublearray = Mapping(self.mapping_doublearray_dict, True)
        self.assertRaises(ValidationError, lambda: mapping_doublearray.validate_payload([nan]))
