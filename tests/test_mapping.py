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
from astarte.device import Mapping, ValidationError


class UnitTests(unittest.TestCase):
    def setUp(self):
        self.mapping_no_timestamp_dict = {
            "endpoint": "/test/two",
            "type": "boolean",
        }
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

    def test_initialize_datastream(self):
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

    def test_initialize_properties(self):
        basic_mapping = {
            "endpoint": "/test/one",
            "type": "boolean",
        }
        mapping_basic = Mapping(basic_mapping, "properties")
        self.assertEqual(mapping_basic.endpoint, "/test/one")
        self.assertEqual(mapping_basic.type, "boolean")
        self.assertEqual(mapping_basic.explicit_timestamp, False)
        self.assertEqual(mapping_basic.reliability, 2)

        explicit_timestamp_mapping = {
            "endpoint": "/test/two",
            "type": "integer",
            "explicit_timestamp": True,
        }
        mapping_basic = Mapping(explicit_timestamp_mapping, "properties")
        self.assertEqual(mapping_basic.endpoint, "/test/two")
        self.assertEqual(mapping_basic.type, "integer")
        self.assertEqual(mapping_basic.explicit_timestamp, True)
        self.assertEqual(mapping_basic.reliability, 2)

        basic_mapping = {
            "endpoint": "/test/one",
            "type": "boolean",
            "reliability": "unreliable",
        }
        mapping_basic = Mapping(basic_mapping, "properties")
        self.assertEqual(mapping_basic.endpoint, "/test/one")
        self.assertEqual(mapping_basic.type, "boolean")
        self.assertEqual(mapping_basic.explicit_timestamp, False)
        self.assertEqual(mapping_basic.reliability, 2)

        basic_mapping = {
            "endpoint": "/test/one",
            "type": "boolean",
            "reliability": "guaranteed",
        }
        mapping_basic = Mapping(basic_mapping, "properties")
        self.assertEqual(mapping_basic.endpoint, "/test/one")
        self.assertEqual(mapping_basic.type, "boolean")
        self.assertEqual(mapping_basic.explicit_timestamp, False)
        self.assertEqual(mapping_basic.reliability, 2)

        basic_mapping = {
            "endpoint": "/test/one",
            "type": "boolean",
            "reliability": "unique",
        }
        mapping_basic = Mapping(basic_mapping, "properties")
        self.assertEqual(mapping_basic.endpoint, "/test/one")
        self.assertEqual(mapping_basic.type, "boolean")
        self.assertEqual(mapping_basic.explicit_timestamp, False)
        self.assertEqual(mapping_basic.reliability, 2)

    def test_mapping_validate_ok(self):
        mapping_no_timestamp = Mapping(self.mapping_no_timestamp_dict, "datastream")
        validate_res = mapping_no_timestamp.validate(True, None)
        assert validate_res is None

        mapping_integer = Mapping(self.mapping_integer_dict, "properties")
        validate_res = mapping_integer.validate(42, datetime.now())
        assert validate_res is None

        mapping_longinteger = Mapping(self.mapping_longinteger_dict, "properties")
        validate_res = mapping_longinteger.validate(42, datetime.now())
        assert validate_res is None

        mapping_double = Mapping(self.mapping_double_dict, "datastream")
        validate_res = mapping_double.validate(42.0, datetime.now())
        assert validate_res is None

        mapping_string = Mapping(self.mapping_string_dict, "datastream")
        validate_res = mapping_string.validate("my string", datetime.now())
        assert validate_res is None

        mapping_binaryblob = Mapping(self.mapping_binaryblob_dict, "datastream")
        validate_res = mapping_binaryblob.validate(b"hello", datetime.now())
        assert validate_res is None

        mapping_boolean = Mapping(self.mapping_boolean_dict, "datastream")
        validate_res = mapping_boolean.validate(True, datetime.now())
        assert validate_res is None

        mapping_datetime = Mapping(self.mapping_datetime_dict, "datastream")
        validate_res = mapping_datetime.validate(datetime.now(), datetime.now())
        assert validate_res is None

        mapping_integerarray = Mapping(self.mapping_integerarray_dict, "datastream")
        validate_res = mapping_integerarray.validate([42], datetime.now())
        assert validate_res is None

        mapping_longintegerarray = Mapping(self.mapping_longintegerarray_dict, "datastream")
        validate_res = mapping_longintegerarray.validate([42], datetime.now())
        assert validate_res is None

        mapping_doublearray = Mapping(self.mapping_doublearray_dict, "datastream")
        validate_res = mapping_doublearray.validate([42.1], datetime.now())
        assert validate_res is None

        mapping_stringarray = Mapping(self.mapping_stringarray_dict, "properties")
        validate_res = mapping_stringarray.validate(["my string"], datetime.now())
        assert validate_res is None

        mapping_binaryblobarray = Mapping(self.mapping_binaryblobarray_dict, "properties")
        validate_res = mapping_binaryblobarray.validate([b"hello", b"world"], datetime.now())
        assert validate_res is None

        mapping_booleanarray = Mapping(self.mapping_booleanarray_dict, "properties")
        validate_res = mapping_booleanarray.validate([True, False], datetime.now())
        assert validate_res is None

        mapping_datetimearray = Mapping(self.mapping_datetimearray_dict, "properties")
        validate_res = mapping_datetimearray.validate([datetime.now()], datetime.now())
        assert validate_res is None

    def test_mapping_validate_missing_timestamp_err(self):
        mapping_integer = Mapping(self.mapping_integer_dict, "datastream")
        validate_res = mapping_integer.validate(42, None)
        assert isinstance(validate_res, ValidationError)
        assert validate_res.msg == "Timestamp required for /test/one"

    def test_mapping_validate_not_required_timestamp_err(self):
        mapping_no_timestamp = Mapping(self.mapping_no_timestamp_dict, "datastream")
        validate_res = mapping_no_timestamp.validate(True, datetime.now())
        assert isinstance(validate_res, ValidationError)
        assert validate_res.msg == "It's not possible to set the timestamp for /test/two"

    def test_mapping_validate_incorrect_type_err(self):
        mapping_integer = Mapping(self.mapping_integer_dict, "datastream")
        validate_res = mapping_integer.validate(True, datetime.now())
        assert isinstance(validate_res, ValidationError)
        assert validate_res.msg == "/test/one is integer but <class 'bool'> was provided"

        mapping_longinteger = Mapping(self.mapping_longinteger_dict, "datastream")
        validate_res = mapping_longinteger.validate(True, datetime.now())
        assert isinstance(validate_res, ValidationError)
        assert validate_res.msg == "/test/one is longinteger but <class 'bool'> was provided"

        mapping_double = Mapping(self.mapping_double_dict, "datastream")
        validate_res = mapping_double.validate(24, datetime.now())
        assert isinstance(validate_res, ValidationError)
        assert validate_res.msg == "/test/one is double but <class 'int'> was provided"

        mapping_string = Mapping(self.mapping_string_dict, "datastream")
        validate_res = mapping_string.validate(b"hello", datetime.now())
        assert isinstance(validate_res, ValidationError)
        assert validate_res.msg == "/test/one is string but <class 'bytes'> was provided"

        mapping_binaryblob = Mapping(self.mapping_binaryblob_dict, "datastream")
        validate_res = mapping_binaryblob.validate(True, datetime.now())
        assert isinstance(validate_res, ValidationError)
        assert validate_res.msg == "/test/one is binaryblob but <class 'bool'> was provided"

        mapping_boolean = Mapping(self.mapping_boolean_dict, "datastream")
        validate_res = mapping_boolean.validate("Hello", datetime.now())
        assert isinstance(validate_res, ValidationError)
        assert validate_res.msg == "/test/one is boolean but <class 'str'> was provided"

        mapping_datetime = Mapping(self.mapping_datetime_dict, "datastream")
        validate_res = mapping_datetime.validate(42, datetime.now())
        assert isinstance(validate_res, ValidationError)
        assert validate_res.msg == "/test/one is datetime but <class 'int'> was provided"

        mapping_integerarray = Mapping(self.mapping_integerarray_dict, "datastream")
        validate_res = mapping_integerarray.validate(12, datetime.now())
        assert isinstance(validate_res, ValidationError)
        assert validate_res.msg == "/test/one is integerarray but <class 'int'> was provided"

        mapping_integerarray = Mapping(self.mapping_integerarray_dict, "datastream")
        validate_res = mapping_integerarray.validate([True], datetime.now())
        assert isinstance(validate_res, ValidationError)
        assert (
            validate_res.msg
            == "/test/one is integerarray but a list of <class 'bool'> was provided"
        )

        mapping_longintegerarray = Mapping(self.mapping_longintegerarray_dict, "datastream")
        validate_res = mapping_longintegerarray.validate([""], datetime.now())
        assert isinstance(validate_res, ValidationError)
        assert (
            validate_res.msg
            == "/test/one is longintegerarray but a list of <class 'str'> was provided"
        )

        mapping_doublearray = Mapping(self.mapping_doublearray_dict, "datastream")
        validate_res = mapping_doublearray.validate(True, datetime.now())
        assert isinstance(validate_res, ValidationError)
        assert validate_res.msg == "/test/one is doublearray but <class 'bool'> was provided"

        mapping_stringarray = Mapping(self.mapping_stringarray_dict, "datastream")
        validate_res = mapping_stringarray.validate([12], datetime.now())
        assert isinstance(validate_res, ValidationError)
        assert (
            validate_res.msg == "/test/one is stringarray but a list of <class 'int'> was provided"
        )

        mapping_binaryblobarray = Mapping(self.mapping_binaryblobarray_dict, "datastream")
        validate_res = mapping_binaryblobarray.validate([True], datetime.now())
        assert isinstance(validate_res, ValidationError)
        assert (
            validate_res.msg
            == "/test/one is binaryblobarray but a list of <class 'bool'> was provided"
        )

        mapping_booleanarray = Mapping(self.mapping_booleanarray_dict, "datastream")
        validate_res = mapping_booleanarray.validate([12], datetime.now())
        assert isinstance(validate_res, ValidationError)
        assert (
            validate_res.msg == "/test/one is booleanarray but a list of <class 'int'> was provided"
        )

        mapping_datetimearray = Mapping(self.mapping_datetimearray_dict, "datastream")
        validate_res = mapping_datetimearray.validate([32], datetime.now())
        assert isinstance(validate_res, ValidationError)
        assert (
            validate_res.msg
            == "/test/one is datetimearray but a list of <class 'int'> was provided"
        )

    def test_mapping_validate_incoherent_type_in_list_err(self):
        mapping_integerarray = Mapping(self.mapping_integerarray_dict, "datastream")
        validate_res = mapping_integerarray.validate([12, True], datetime.now())
        assert isinstance(validate_res, ValidationError)
        assert validate_res.msg == "Type incoherence in payload elements"

        mapping_longintegerarray = Mapping(self.mapping_longintegerarray_dict, "datastream")
        validate_res = mapping_longintegerarray.validate([12, ""], datetime.now())
        assert isinstance(validate_res, ValidationError)
        assert validate_res.msg == "Type incoherence in payload elements"

        mapping_doublearray = Mapping(self.mapping_doublearray_dict, "datastream")
        validate_res = mapping_doublearray.validate([12.3, 12], datetime.now())
        assert isinstance(validate_res, ValidationError)
        assert validate_res.msg == "Type incoherence in payload elements"

        mapping_stringarray = Mapping(self.mapping_stringarray_dict, "datastream")
        validate_res = mapping_stringarray.validate(["23", b""], datetime.now())
        assert isinstance(validate_res, ValidationError)
        assert validate_res.msg == "Type incoherence in payload elements"

        mapping_binaryblobarray = Mapping(self.mapping_binaryblobarray_dict, "datastream")
        validate_res = mapping_binaryblobarray.validate([b"22", ""], datetime.now())
        assert isinstance(validate_res, ValidationError)
        assert validate_res.msg == "Type incoherence in payload elements"

        mapping_booleanarray = Mapping(self.mapping_booleanarray_dict, "datastream")
        validate_res = mapping_booleanarray.validate([True, 11], datetime.now())
        assert isinstance(validate_res, ValidationError)
        assert validate_res.msg == "Type incoherence in payload elements"

        mapping_datetimearray = Mapping(self.mapping_datetimearray_dict, "datastream")
        validate_res = mapping_datetimearray.validate([datetime.now(), 11], datetime.now())
        assert isinstance(validate_res, ValidationError)
        assert validate_res.msg == "Type incoherence in payload elements"

    def test_mapping_validate_out_of_range_integer_err(self):
        mapping_integer = Mapping(self.mapping_integer_dict, "datastream")
        validate_res = mapping_integer.validate(-2147483649, datetime.now())
        assert isinstance(validate_res, ValidationError)
        assert validate_res.msg == "Value out of int32 range for /test/one"

        mapping_integer = Mapping(self.mapping_integer_dict, "datastream")
        validate_res = mapping_integer.validate(2147483648, datetime.now())
        assert isinstance(validate_res, ValidationError)
        assert validate_res.msg == "Value out of int32 range for /test/one"

        mapping_integerarray = Mapping(self.mapping_integerarray_dict, "datastream")
        validate_res = mapping_integerarray.validate([-2147483649], datetime.now())
        assert isinstance(validate_res, ValidationError)
        assert validate_res.msg == "Value out of int32 range for /test/one"

        mapping_integerarray = Mapping(self.mapping_integerarray_dict, "datastream")
        validate_res = mapping_integerarray.validate([2147483648], datetime.now())
        assert isinstance(validate_res, ValidationError)
        assert validate_res.msg == "Value out of int32 range for /test/one"

    def test_mapping_validate_nan_double_err(self):
        mapping_double = Mapping(self.mapping_double_dict, "datastream")
        validate_res = mapping_double.validate(nan, datetime.now())
        assert isinstance(validate_res, ValidationError)
        assert validate_res.msg == "Invalid float value for /test/one"

        mapping_doublearray = Mapping(self.mapping_doublearray_dict, "datastream")
        validate_res = mapping_doublearray.validate([nan], datetime.now())
        assert isinstance(validate_res, ValidationError)
        assert validate_res.msg == "Invalid float value for /test/one"
