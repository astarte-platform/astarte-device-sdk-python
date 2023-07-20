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
# pylint: disable=too-many-statements,too-many-instance-attributes

import unittest
from datetime import datetime
from math import nan
from astarte.device import Mapping, ValidationError, InterfaceFileDecodeError


class UnitTests(unittest.TestCase):
    def setUp(self):
        self.mapping_integer_dict = {
            "endpoint": "/test/one",
            "type": "integer",
            "explicit_timestamp": True,
        }
        self.mapping_longinteger_dict = {
            "endpoint": "/test/one",
            "type": "longinteger",
            "explicit_timestamp": True,
        }
        self.mapping_double_dict = {
            "endpoint": "/test/one",
            "type": "double",
            "explicit_timestamp": True,
        }
        self.mapping_string_dict = {
            "endpoint": "/test/one",
            "type": "string",
            "explicit_timestamp": True,
        }
        self.mapping_binaryblob_dict = {
            "endpoint": "/test/one",
            "type": "binaryblob",
            "explicit_timestamp": True,
        }
        self.mapping_boolean_dict = {
            "endpoint": "/test/one",
            "type": "boolean",
            "explicit_timestamp": True,
        }
        self.mapping_datetime_dict = {
            "endpoint": "/test/one",
            "type": "datetime",
            "explicit_timestamp": True,
        }
        self.mapping_integerarray_dict = {
            "endpoint": "/test/one",
            "type": "integerarray",
            "explicit_timestamp": True,
        }
        self.mapping_longintegerarray_dict = {
            "endpoint": "/test/one",
            "type": "longintegerarray",
            "explicit_timestamp": True,
        }
        self.mapping_doublearray_dict = {
            "endpoint": "/test/one",
            "type": "doublearray",
            "explicit_timestamp": True,
        }
        self.mapping_stringarray_dict = {
            "endpoint": "/test/one",
            "type": "stringarray",
            "explicit_timestamp": True,
        }
        self.mapping_binaryblobarray_dict = {
            "endpoint": "/test/one",
            "type": "binaryblobarray",
            "explicit_timestamp": True,
        }
        self.mapping_booleanarray_dict = {
            "endpoint": "/test/one",
            "type": "booleanarray",
            "explicit_timestamp": True,
        }
        self.mapping_datetimearray_dict = {
            "endpoint": "/test/one",
            "type": "datetimearray",
            "explicit_timestamp": True,
        }

    def test_initialize(self):
        basic_mapping = {
            "endpoint": "/test/one",
            "type": "boolean",
        }
        mapping_basic = Mapping(basic_mapping, "datastream")
        self.assertEqual(mapping_basic.endpoint, "/test/one")
        self.assertEqual(mapping_basic.type, "boolean")
        self.assertEqual(mapping_basic.explicit_timestamp, False)
        self.assertEqual(mapping_basic.reliability, 0)

        explicit_timestamp_mapping = {
            "endpoint": "/test/two",
            "type": "integer",
            "explicit_timestamp": True,
        }
        mapping_basic = Mapping(explicit_timestamp_mapping, "datastream")
        self.assertEqual(mapping_basic.endpoint, "/test/two")
        self.assertEqual(mapping_basic.type, "integer")
        self.assertEqual(mapping_basic.explicit_timestamp, True)
        self.assertEqual(mapping_basic.reliability, 0)

        unreliable_mapping = {
            "endpoint": "/test/one",
            "type": "boolean",
            "reliability": "unreliable",
        }
        mapping_basic = Mapping(unreliable_mapping, "datastream")
        self.assertEqual(mapping_basic.endpoint, "/test/one")
        self.assertEqual(mapping_basic.type, "boolean")
        self.assertEqual(mapping_basic.explicit_timestamp, False)
        self.assertEqual(mapping_basic.reliability, 0)

        guaranteed_mapping = {
            "endpoint": "/test/one",
            "type": "boolean",
            "reliability": "guaranteed",
        }
        mapping_basic = Mapping(guaranteed_mapping, "datastream")
        self.assertEqual(mapping_basic.endpoint, "/test/one")
        self.assertEqual(mapping_basic.type, "boolean")
        self.assertEqual(mapping_basic.explicit_timestamp, False)
        self.assertEqual(mapping_basic.reliability, 1)

        unique_mapping = {
            "endpoint": "/test/one",
            "type": "boolean",
            "reliability": "unique",
        }
        mapping_basic = Mapping(unique_mapping, "datastream")
        self.assertEqual(mapping_basic.endpoint, "/test/one")
        self.assertEqual(mapping_basic.type, "boolean")
        self.assertEqual(mapping_basic.explicit_timestamp, False)
        self.assertEqual(mapping_basic.reliability, 2)

        basic_mapping = {
            "endpoint": "/test/one",
            "type": "boolean",
        }
        mapping_basic = Mapping(basic_mapping, "properties")
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
        mapping_basic = Mapping(unsettable_mapping, "properties")
        self.assertEqual(mapping_basic.endpoint, "/test/one")
        self.assertEqual(mapping_basic.type, "boolean")
        self.assertEqual(mapping_basic.explicit_timestamp, False)
        self.assertEqual(mapping_basic.reliability, 2)
        self.assertEqual(mapping_basic.allow_unset, True)

    def test_initialize_missing_endpoint_raises(self):
        basic_mapping = {
            "type": "boolean",
        }
        self.assertRaises(InterfaceFileDecodeError, lambda: Mapping(basic_mapping, "datastream"))

    def test_initialize_missing_type_raises(self):
        basic_mapping = {
            "endpoint": "/test/one",
        }
        self.assertRaises(InterfaceFileDecodeError, lambda: Mapping(basic_mapping, "datastream"))

    def test_initialize_non_existing_type_raises(self):
        basic_mapping = {
            "endpoint": "/test/one",
            "type": "foo",
        }
        self.assertRaises(InterfaceFileDecodeError, lambda: Mapping(basic_mapping, "datastream"))

    def test_initialize_explicit_timestamp_in_property_raises(self):
        basic_mapping = {
            "endpoint": "/test/one",
            "type": "integer",
            "explicit_timestamp": True,
        }
        self.assertRaises(InterfaceFileDecodeError, lambda: Mapping(basic_mapping, "properties"))

    def test_initialize_reliability_in_property_raises(self):
        basic_mapping = {
            "endpoint": "/test/one",
            "type": "integer",
            "reliability": "unreliable",
        }
        self.assertRaises(InterfaceFileDecodeError, lambda: Mapping(basic_mapping, "properties"))

    def test_initialize_unsettability_in_datastreams_raises(self):
        basic_mapping = {
            "endpoint": "/test/one",
            "type": "integer",
            "allow_unset": True,
        }
        self.assertRaises(InterfaceFileDecodeError, lambda: Mapping(basic_mapping, "datastream"))

    def test_validate_path(self):
        basic_mapping = {
            "endpoint": "/test/one",
            "type": "boolean",
        }
        mapping_basic = Mapping(basic_mapping, "datastream")
        self.assertIsNone(mapping_basic.validate_path("/test/one"))
        self.assertIsInstance(mapping_basic.validate_path("test/one"), ValidationError)
        self.assertIsInstance(mapping_basic.validate_path("/tests/one"), ValidationError)
        self.assertIsInstance(mapping_basic.validate_path("/test/one/more"), ValidationError)
        self.assertIsInstance(mapping_basic.validate_path("/test/one/"), ValidationError)
        self.assertIsInstance(mapping_basic.validate_path("more/test/one"), ValidationError)

        param_mapping = {
            "endpoint": r"/%{param}/one",
            "type": "boolean",
        }
        mapping_param = Mapping(param_mapping, "datastream")
        self.assertIsNone(mapping_param.validate_path("/21smt/one"))
        self.assertIsInstance(mapping_param.validate_path("/more/21/one"), ValidationError)
        self.assertIsInstance(mapping_param.validate_path("/21/more/one"), ValidationError)
        self.assertIsInstance(mapping_param.validate_path("/21+/one"), ValidationError)
        self.assertIsInstance(mapping_param.validate_path("/21#/one"), ValidationError)

        two_param_mapping = {
            "endpoint": r"/%{param1}/%{param2}/one",
            "type": "boolean",
        }
        mapping_two_param = Mapping(two_param_mapping, "datastream")
        self.assertIsNone(mapping_two_param.validate_path("/aa/bb/one"))
        self.assertIsInstance(mapping_two_param.validate_path("/21/one"), ValidationError)
        self.assertIsInstance(mapping_two_param.validate_path("/aa/bb/more/one"), ValidationError)
        self.assertIsInstance(mapping_two_param.validate_path("more/aa/bb/one"), ValidationError)

    def test_validate_timestamp(self):
        basic_mapping = {
            "endpoint": "/test/one",
            "type": "integer",
        }
        mapping_integer = Mapping(basic_mapping, "datastream")
        self.assertIsNone(mapping_integer.validate_timestamp(None))

        basic_mapping["explicit_timestamp"] = True
        mapping_integer = Mapping(basic_mapping, "datastream")
        self.assertIsNone(mapping_integer.validate_timestamp(datetime.now()))

    def test_validate_timestamp_missing_timestamp_err(self):
        basic_mapping = {"endpoint": "/test/one", "type": "integer", "explicit_timestamp": True}
        mapping_integer = Mapping(basic_mapping, "datastream")
        validate_res = mapping_integer.validate_timestamp(None)
        assert isinstance(validate_res, ValidationError)
        assert validate_res.msg == "Timestamp required for /test/one"

    def test_validate_timestamp_extra_timestamp_err(self):
        basic_mapping = {
            "endpoint": "/test/one",
            "type": "integer",
        }
        mapping_integer = Mapping(basic_mapping, "datastream")
        validate_res = mapping_integer.validate_timestamp(datetime.now())
        assert isinstance(validate_res, ValidationError)
        assert validate_res.msg == "It's not possible to set the timestamp for /test/one"

    def test_validate_payload(self):
        mapping_integer = Mapping(self.mapping_integer_dict, "datastream")
        self.assertIsNone(mapping_integer.validate_payload(42))

        mapping_longinteger = Mapping(self.mapping_longinteger_dict, "datastream")
        self.assertIsNone(mapping_longinteger.validate_payload(42))

        mapping_double = Mapping(self.mapping_double_dict, "datastream")
        self.assertIsNone(mapping_double.validate_payload(42.0))

        mapping_string = Mapping(self.mapping_string_dict, "datastream")
        self.assertIsNone(mapping_string.validate_payload("my string"))

        mapping_binaryblob = Mapping(self.mapping_binaryblob_dict, "datastream")
        self.assertIsNone(mapping_binaryblob.validate_payload(b"hello"))

        mapping_boolean = Mapping(self.mapping_boolean_dict, "datastream")
        self.assertIsNone(mapping_boolean.validate_payload(True))

        mapping_datetime = Mapping(self.mapping_datetime_dict, "datastream")
        self.assertIsNone(mapping_datetime.validate_payload(datetime.now()))

        mapping_integerarray = Mapping(self.mapping_integerarray_dict, "datastream")
        self.assertIsNone(mapping_integerarray.validate_payload([42]))

        mapping_longintegerarray = Mapping(self.mapping_longintegerarray_dict, "datastream")
        self.assertIsNone(mapping_longintegerarray.validate_payload([42]))

        mapping_doublearray = Mapping(self.mapping_doublearray_dict, "datastream")
        self.assertIsNone(mapping_doublearray.validate_payload([42.1]))

        mapping_stringarray = Mapping(self.mapping_stringarray_dict, "datastream")
        self.assertIsNone(mapping_stringarray.validate_payload(["my string"]))

        mapping_binaryblobarray = Mapping(self.mapping_binaryblobarray_dict, "datastream")
        self.assertIsNone(mapping_binaryblobarray.validate_payload([b"hello", b"world"]))

        mapping_booleanarray = Mapping(self.mapping_booleanarray_dict, "datastream")
        self.assertIsNone(mapping_booleanarray.validate_payload([True, False]))

        mapping_datetimearray = Mapping(self.mapping_datetimearray_dict, "datastream")
        self.assertIsNone(mapping_datetimearray.validate_payload([datetime.now()]))

    def test_validate_payload_empty_payload_err(self):
        basic_mapping = {
            "endpoint": "/test/one",
            "type": "double",
        }
        mapping_basic = Mapping(basic_mapping, "datastream")
        validate_res = mapping_basic.validate_payload(None)
        assert isinstance(validate_res, ValidationError)
        assert validate_res.msg == "Attempting to validate an empty payload for /test/one"
        validate_res = mapping_basic.validate_payload([])
        assert isinstance(validate_res, ValidationError)
        assert validate_res.msg == "Attempting to validate an empty payload for /test/one"
        self.assertIsNone(mapping_basic.validate_payload(0.0))

    def test_validate_payload_incorrect_type_err(self):
        mapping_integer = Mapping(self.mapping_integer_dict, "datastream")
        validate_res = mapping_integer.validate_payload(True)
        assert isinstance(validate_res, ValidationError)
        assert validate_res.msg == "/test/one is integer but <class 'bool'> was provided"

        mapping_longinteger = Mapping(self.mapping_longinteger_dict, "datastream")
        validate_res = mapping_longinteger.validate_payload(True)
        assert isinstance(validate_res, ValidationError)
        assert validate_res.msg == "/test/one is longinteger but <class 'bool'> was provided"

        mapping_double = Mapping(self.mapping_double_dict, "datastream")
        validate_res = mapping_double.validate_payload(24)
        assert isinstance(validate_res, ValidationError)
        assert validate_res.msg == "/test/one is double but <class 'int'> was provided"

        mapping_string = Mapping(self.mapping_string_dict, "datastream")
        validate_res = mapping_string.validate_payload(b"hello")
        assert isinstance(validate_res, ValidationError)
        assert validate_res.msg == "/test/one is string but <class 'bytes'> was provided"

        mapping_binaryblob = Mapping(self.mapping_binaryblob_dict, "datastream")
        validate_res = mapping_binaryblob.validate_payload(True)
        assert isinstance(validate_res, ValidationError)
        assert validate_res.msg == "/test/one is binaryblob but <class 'bool'> was provided"

        mapping_boolean = Mapping(self.mapping_boolean_dict, "datastream")
        validate_res = mapping_boolean.validate_payload("Hello")
        assert isinstance(validate_res, ValidationError)
        assert validate_res.msg == "/test/one is boolean but <class 'str'> was provided"

        mapping_datetime = Mapping(self.mapping_datetime_dict, "datastream")
        validate_res = mapping_datetime.validate_payload(42)
        assert isinstance(validate_res, ValidationError)
        assert validate_res.msg == "/test/one is datetime but <class 'int'> was provided"

        mapping_integerarray = Mapping(self.mapping_integerarray_dict, "datastream")
        validate_res = mapping_integerarray.validate_payload(12)
        assert isinstance(validate_res, ValidationError)
        assert validate_res.msg == "/test/one is integerarray but <class 'int'> was provided"

        mapping_integerarray = Mapping(self.mapping_integerarray_dict, "datastream")
        validate_res = mapping_integerarray.validate_payload([True])
        assert isinstance(validate_res, ValidationError)
        assert (
            validate_res.msg
            == "/test/one is integerarray but a list of <class 'bool'> was provided"
        )

        mapping_longintegerarray = Mapping(self.mapping_longintegerarray_dict, "datastream")
        validate_res = mapping_longintegerarray.validate_payload([""])
        assert isinstance(validate_res, ValidationError)
        assert (
            validate_res.msg
            == "/test/one is longintegerarray but a list of <class 'str'> was provided"
        )

        mapping_doublearray = Mapping(self.mapping_doublearray_dict, "datastream")
        validate_res = mapping_doublearray.validate_payload(True)
        assert isinstance(validate_res, ValidationError)
        assert validate_res.msg == "/test/one is doublearray but <class 'bool'> was provided"

        mapping_stringarray = Mapping(self.mapping_stringarray_dict, "datastream")
        validate_res = mapping_stringarray.validate_payload([12])
        assert isinstance(validate_res, ValidationError)
        assert (
            validate_res.msg == "/test/one is stringarray but a list of <class 'int'> was provided"
        )

        mapping_binaryblobarray = Mapping(self.mapping_binaryblobarray_dict, "datastream")
        validate_res = mapping_binaryblobarray.validate_payload([True])
        assert isinstance(validate_res, ValidationError)
        assert (
            validate_res.msg
            == "/test/one is binaryblobarray but a list of <class 'bool'> was provided"
        )

        mapping_booleanarray = Mapping(self.mapping_booleanarray_dict, "datastream")
        validate_res = mapping_booleanarray.validate_payload([12])
        assert isinstance(validate_res, ValidationError)
        assert (
            validate_res.msg == "/test/one is booleanarray but a list of <class 'int'> was provided"
        )

        mapping_datetimearray = Mapping(self.mapping_datetimearray_dict, "datastream")
        validate_res = mapping_datetimearray.validate_payload([32])
        assert isinstance(validate_res, ValidationError)
        assert (
            validate_res.msg
            == "/test/one is datetimearray but a list of <class 'int'> was provided"
        )

    def test_validate_payload_incoherent_type_in_list_err(self):
        mapping_integerarray = Mapping(self.mapping_integerarray_dict, "datastream")
        validate_res = mapping_integerarray.validate_payload([12, True])
        assert isinstance(validate_res, ValidationError)
        assert validate_res.msg == "Type incoherence in payload elements"

        mapping_longintegerarray = Mapping(self.mapping_longintegerarray_dict, "datastream")
        validate_res = mapping_longintegerarray.validate_payload([12, ""])
        assert isinstance(validate_res, ValidationError)
        assert validate_res.msg == "Type incoherence in payload elements"

        mapping_doublearray = Mapping(self.mapping_doublearray_dict, "datastream")
        validate_res = mapping_doublearray.validate_payload([12.3, 12])
        assert isinstance(validate_res, ValidationError)
        assert validate_res.msg == "Type incoherence in payload elements"

        mapping_stringarray = Mapping(self.mapping_stringarray_dict, "datastream")
        validate_res = mapping_stringarray.validate_payload(["23", b""])
        assert isinstance(validate_res, ValidationError)
        assert validate_res.msg == "Type incoherence in payload elements"

        mapping_binaryblobarray = Mapping(self.mapping_binaryblobarray_dict, "datastream")
        validate_res = mapping_binaryblobarray.validate_payload([b"22", ""])
        assert isinstance(validate_res, ValidationError)
        assert validate_res.msg == "Type incoherence in payload elements"

        mapping_booleanarray = Mapping(self.mapping_booleanarray_dict, "datastream")
        validate_res = mapping_booleanarray.validate_payload([True, 11])
        assert isinstance(validate_res, ValidationError)
        assert validate_res.msg == "Type incoherence in payload elements"

        mapping_datetimearray = Mapping(self.mapping_datetimearray_dict, "datastream")
        validate_res = mapping_datetimearray.validate_payload([datetime.now(), 11])
        assert isinstance(validate_res, ValidationError)
        assert validate_res.msg == "Type incoherence in payload elements"

    def test_validate_payload_out_of_range_integer_err(self):
        mapping_integer = Mapping(self.mapping_integer_dict, "datastream")
        validate_res = mapping_integer.validate_payload(-2147483649)
        assert isinstance(validate_res, ValidationError)
        assert validate_res.msg == "Value out of int32 range for /test/one"

        mapping_integer = Mapping(self.mapping_integer_dict, "datastream")
        validate_res = mapping_integer.validate_payload(2147483648)
        assert isinstance(validate_res, ValidationError)
        assert validate_res.msg == "Value out of int32 range for /test/one"

        mapping_integerarray = Mapping(self.mapping_integerarray_dict, "datastream")
        validate_res = mapping_integerarray.validate_payload([-2147483649])
        assert isinstance(validate_res, ValidationError)
        assert validate_res.msg == "Value out of int32 range for /test/one"

        mapping_integerarray = Mapping(self.mapping_integerarray_dict, "datastream")
        validate_res = mapping_integerarray.validate_payload([2147483648])
        assert isinstance(validate_res, ValidationError)
        assert validate_res.msg == "Value out of int32 range for /test/one"

    def test_validate_payload_nan_double_err(self):
        mapping_double = Mapping(self.mapping_double_dict, "datastream")
        validate_res = mapping_double.validate_payload(nan)
        assert isinstance(validate_res, ValidationError)
        assert validate_res.msg == "Invalid float value for /test/one"

        mapping_doublearray = Mapping(self.mapping_doublearray_dict, "datastream")
        validate_res = mapping_doublearray.validate_payload([nan])
        assert isinstance(validate_res, ValidationError)
        assert validate_res.msg == "Invalid float value for /test/one"
