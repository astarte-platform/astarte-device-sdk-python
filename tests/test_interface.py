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
from unittest import mock
from astarte.device import Interface, Mapping
from astarte.device.exceptions import (
    ValidationError,
    InterfaceNotFoundError,
    InterfaceFileDecodeError,
)


class UnitTests(unittest.TestCase):
    def setUp(self):
        self.interface_minimal_dict = {
            "interface_name": "com.astarte.Test",
            "version_major": 0,
            "version_minor": 1,
            "type": "datastream",
            "mappings": [
                {
                    "endpoint": "/test/int",
                    "type": "integer",
                }
            ],
        }

    @mock.patch("astarte.device.interface.Mapping")
    def test_initialize(self, mock_mapping):
        mock_instance = mock_mapping.return_value
        mock_instance.endpoint = "/test/int"
        interface_basic = Interface(self.interface_minimal_dict)
        self.assertEqual(interface_basic.name, "com.astarte.Test")
        self.assertEqual(interface_basic.version_major, 0)
        self.assertEqual(interface_basic.version_minor, 1)
        self.assertEqual(interface_basic.type, "datastream")
        self.assertEqual(interface_basic.ownership, "device")
        self.assertEqual(interface_basic.aggregation, "")
        self.assertEqual(interface_basic.mappings, [mock_instance])
        mock_mapping.assert_called_once_with(
            {"endpoint": "/test/int", "type": "integer"}, "datastream"
        )

        mock_mapping.reset_mock()
        self.interface_minimal_dict["type"] = "properties"
        self.interface_minimal_dict["ownership"] = "server"
        interface_basic = Interface(self.interface_minimal_dict)
        self.assertEqual(interface_basic.name, "com.astarte.Test")
        self.assertEqual(interface_basic.version_major, 0)
        self.assertEqual(interface_basic.version_minor, 1)
        self.assertEqual(interface_basic.type, "properties")
        self.assertEqual(interface_basic.ownership, "server")
        self.assertEqual(interface_basic.aggregation, "")
        self.assertEqual(interface_basic.mappings, [mock_instance])
        mock_mapping.assert_called_once_with(
            {"endpoint": "/test/int", "type": "integer"}, "properties"
        )

    def test_initialize_same_minor_major_version_raises(self):
        self.interface_minimal_dict["version_minor"] = 0
        self.assertRaises(ValueError, lambda: Interface(self.interface_minimal_dict))

    def test_initialize_duplicate_mapping_raises(self):
        duplicate_mapping = {
            "endpoint": "/test/int",
            "type": "integer",
        }
        self.interface_minimal_dict["mappings"].append(duplicate_mapping)
        self.assertRaises(InterfaceFileDecodeError, lambda: Interface(self.interface_minimal_dict))

    def test_is_aggregation_object(self):
        # Defaults to individual when it misses the aggregation field
        interface_individual = Interface(self.interface_minimal_dict)
        assert not interface_individual.is_aggregation_object()

        self.interface_minimal_dict["aggregation"] = "object"
        interface_aggregated = Interface(self.interface_minimal_dict)
        assert interface_aggregated.is_aggregation_object()

    def test_is_server_owned(self):
        # Defaults to device owned when it misses the ownership field
        interface_device_owned = Interface(self.interface_minimal_dict)
        assert not interface_device_owned.is_server_owned()

        self.interface_minimal_dict["ownership"] = "device"
        interface_device_owned = Interface(self.interface_minimal_dict)
        assert not interface_device_owned.is_server_owned()

        self.interface_minimal_dict["ownership"] = "server"
        interface_device_owned = Interface(self.interface_minimal_dict)
        assert interface_device_owned.is_server_owned()

    def test_is_type_properties(self):
        interface_datastream = Interface(self.interface_minimal_dict)
        assert not interface_datastream.is_type_properties()

        self.interface_minimal_dict["type"] = "properties"
        interface_property = Interface(self.interface_minimal_dict)
        assert interface_property.is_type_properties()

    @mock.patch.object(Mapping, "validate_path")
    def test_get_mapping(self, mock_validate_path):
        new_mappings = [
            {"endpoint": "/test/one", "type": "integer"},
            {"endpoint": "/test/two", "type": "boolean"},
        ]
        self.interface_minimal_dict["mappings"] = new_mappings
        interface_simple_endpoint = Interface(self.interface_minimal_dict)

        path = "/test/two"
        mock_validate_res = mock.MagicMock()
        mock_validate_path.side_effect = [mock_validate_res, None]

        self.assertIsNotNone(interface_simple_endpoint.get_mapping(path))

        mock_validate_path.assert_has_calls([mock.call("/test/two"), mock.call("/test/two")])
        self.assertEqual(mock_validate_path.call_count, 2)

    @mock.patch.object(Mapping, "validate_path")
    def test_get_mapping_no_mapping(self, mock_validate_path):
        new_mappings = [
            {"endpoint": "/test/one", "type": "integer"},
            {"endpoint": "/test/two", "type": "boolean"},
        ]
        self.interface_minimal_dict["mappings"] = new_mappings
        interface_simple_endpoint = Interface(self.interface_minimal_dict)

        path = "/test/three"
        mock_validate_res = mock.MagicMock()
        mock_validate_path.side_effect = [mock_validate_res, mock_validate_res]

        self.assertIsNone(interface_simple_endpoint.get_mapping(path))

        mock_validate_path.assert_has_calls([mock.call("/test/three"), mock.call("/test/three")])
        self.assertEqual(mock_validate_path.call_count, 2)

    @mock.patch.object(Mapping, "validate_payload")
    def test_validate_individual(self, mock_validate_payload):
        interface_individual = Interface(self.interface_minimal_dict)

        mock_validate_payload.return_value = "Map validate return value"
        result = interface_individual.validate("/test/int", 42, None)
        self.assertEqual("Map validate return value", result)
        mock_validate_payload.assert_called_once_with(42, None)

    @mock.patch.object(Mapping, "validate_payload")
    def test_validate_individual_parametric(self, mock_validate_payload):
        new_mappings = [
            {
                "endpoint": r"/test/%{var}/int",
                "type": "integer",
                "database_retention_policy": "use_ttl",
                "database_retention_ttl": 31536000,
            }
        ]
        self.interface_minimal_dict["mappings"] = new_mappings
        interface_individual = Interface(self.interface_minimal_dict)

        mock_validate_payload.return_value = "Map validate return value"
        result = interface_individual.validate("/test/11/int", 42, None)
        self.assertEqual("Map validate return value", result)
        mock_validate_payload.assert_called_once_with(42, None)

    def test_validate_aggregate(self):
        self.interface_minimal_dict["aggregation"] = "object"
        new_mappings = [
            {
                "endpoint": "/test/one",
                "type": "integer",
                "database_retention_policy": "use_ttl",
                "database_retention_ttl": 31536000,
            },
            {
                "endpoint": "/test/two",
                "type": "boolean",
                "database_retention_policy": "use_ttl",
                "database_retention_ttl": 31536000,
            },
        ]
        self.interface_minimal_dict["mappings"] = new_mappings

        interface_aggregate = Interface(self.interface_minimal_dict)
        payload = {
            "one": 42,
            "two": True,
        }
        result = interface_aggregate.validate("/test", payload, None)
        self.assertIsNone(result)

    def test_validate_aggregate_parametric(self):
        self.interface_minimal_dict["aggregation"] = "object"
        new_mappings = [
            {
                "endpoint": r"/test/%{var}/one",
                "type": "integer",
                "database_retention_policy": "use_ttl",
                "database_retention_ttl": 31536000,
            },
            {
                "endpoint": r"/test/%{var}/two",
                "type": "boolean",
                "database_retention_policy": "use_ttl",
                "database_retention_ttl": 31536000,
            },
        ]
        self.interface_minimal_dict["mappings"] = new_mappings

        interface_aggregate = Interface(self.interface_minimal_dict)
        payload = {
            "one": 42,
            "two": True,
        }
        result = interface_aggregate.validate("/test/43", payload, None)
        self.assertIsNone(result)

    def test_validate_individual_endpoint_not_existing_err(self):
        interface_server = Interface(self.interface_minimal_dict)
        result = interface_server.validate("/test/something", 42, None)
        assert isinstance(result, ValidationError)
        assert result.msg == "Path /test/something not in the com.astarte.Test interface."

    def test_validate_aggregate_payload_not_dict_err(self):
        self.interface_minimal_dict["aggregation"] = "object"
        new_mappings = [
            {
                "endpoint": "/test/one",
                "type": "integer",
                "database_retention_policy": "use_ttl",
                "database_retention_ttl": 31536000,
            },
            {
                "endpoint": "/test/two",
                "type": "boolean",
                "database_retention_policy": "use_ttl",
                "database_retention_ttl": 31536000,
            },
        ]
        self.interface_minimal_dict["mappings"] = new_mappings
        interface_server = Interface(self.interface_minimal_dict)
        result = interface_server.validate("/test/one", 42, None)
        assert isinstance(result, ValidationError)
        assert (
            result.msg
            == "The interface com.astarte.Test is aggregate, but the payload is not a dictionary."
        )

    def test_validate_aggregate_non_existing_endpoint_err(self):
        self.interface_minimal_dict["aggregation"] = "object"
        new_mappings = [
            {
                "endpoint": "/test/one",
                "type": "integer",
                "database_retention_policy": "use_ttl",
                "database_retention_ttl": 31536000,
            },
            {
                "endpoint": "/test/two",
                "type": "boolean",
                "database_retention_policy": "use_ttl",
                "database_retention_ttl": 31536000,
            },
        ]
        self.interface_minimal_dict["mappings"] = new_mappings
        interface_server = Interface(self.interface_minimal_dict)
        payload = {
            "one": 42,
            "three": True,
        }
        result = interface_server.validate("/test", payload, None)
        assert isinstance(result, ValidationError)
        assert result.msg == "Path /test/three not in the com.astarte.Test interface."

    def test_validate_aggregate_non_valid_mapping_err(self):
        self.interface_minimal_dict["aggregation"] = "object"
        new_mappings = [
            {
                "endpoint": "/test/one",
                "type": "integer",
                "database_retention_policy": "use_ttl",
                "database_retention_ttl": 31536000,
            },
            {
                "endpoint": "/test/two",
                "type": "boolean",
                "database_retention_policy": "use_ttl",
                "database_retention_ttl": 31536000,
            },
        ]
        self.interface_minimal_dict["mappings"] = new_mappings
        interface_server = Interface(self.interface_minimal_dict)
        payload = {
            "one": True,
            "two": True,
        }
        result = interface_server.validate("/test", payload, None)
        assert isinstance(result, ValidationError)
        assert result.msg == "/test/one is integer but <class 'bool'> was provided"

    def test_validate_aggregate_missing_endpoint_err(self):
        self.interface_minimal_dict["aggregation"] = "object"
        new_mappings = [
            {
                "endpoint": "/test/one",
                "type": "integer",
                "database_retention_policy": "use_ttl",
                "database_retention_ttl": 31536000,
            },
            {
                "endpoint": "/test/two",
                "type": "boolean",
                "database_retention_policy": "use_ttl",
                "database_retention_ttl": 31536000,
            },
        ]
        self.interface_minimal_dict["mappings"] = new_mappings
        interface_server = Interface(self.interface_minimal_dict)
        payload = {
            "one": 42,
        }
        result = interface_server.validate("/test", payload, None)
        assert isinstance(result, ValidationError)
        assert result.msg == "Path /test/two of com.astarte.Test interface is not in the payload."

    def test_validate_aggregate_parametric_missing_endpoint_err(self):
        self.interface_minimal_dict["aggregation"] = "object"
        new_mappings = [
            {
                "endpoint": r"/test/%{some_id}/one",
                "type": "integer",
                "database_retention_policy": "use_ttl",
                "database_retention_ttl": 31536000,
            },
            {
                "endpoint": r"/test/%{some_id}/two",
                "type": "boolean",
                "database_retention_policy": "use_ttl",
                "database_retention_ttl": 31536000,
            },
        ]
        self.interface_minimal_dict["mappings"] = new_mappings
        interface_server = Interface(self.interface_minimal_dict)
        payload = {
            "one": 42,
        }
        result = interface_server.validate("/test/42", payload, None)
        assert isinstance(result, ValidationError)
        assert (
            result.msg
            == r"Path /test/%{some_id}/two of com.astarte.Test interface is not in the payload."
        )

    def test_get_reliability_individual(self):
        interface_simple_endpoint = Interface(self.interface_minimal_dict)
        self.assertEqual(interface_simple_endpoint.get_reliability("/test/int"), 0)

    def test_get_reliability_aggregate(self):
        self.interface_minimal_dict["aggregation"] = "object"
        interface_simple_endpoint = Interface(self.interface_minimal_dict)
        self.assertEqual(interface_simple_endpoint.get_reliability("/test/int"), 2)

    def test_get_reliability_non_existent_path(self):
        interface_simple_endpoint = Interface(self.interface_minimal_dict)
        self.assertRaises(
            InterfaceNotFoundError,
            lambda: interface_simple_endpoint.get_reliability("/missing/endpoint"),
        )
