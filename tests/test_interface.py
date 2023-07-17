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
            "ownership": "device",
            "mappings": [
                {
                    "endpoint": "/test/int",
                    "type": "integer",
                }
            ],
        }

    @mock.patch("astarte.device.interface.Mapping")
    def test_initialize(self, mock_mapping):
        basic_interface_dict = {
            "interface_name": "com.astarte.Test",
            "version_major": 0,
            "version_minor": 1,
            "type": "datastream",
            "ownership": "device",
            "mappings": [
                {"mapping": "number 1"},
            ],
        }

        basic_interface = Interface(basic_interface_dict)

        mock_mapping.assert_called_once_with({"mapping": "number 1"}, "datastream")
        self.assertEqual(basic_interface.name, "com.astarte.Test")
        self.assertEqual(basic_interface.version_major, 0)
        self.assertEqual(basic_interface.version_minor, 1)
        self.assertEqual(basic_interface.type, "datastream")
        self.assertEqual(basic_interface.ownership, "device")
        self.assertEqual(basic_interface.aggregation, "individual")
        self.assertEqual(basic_interface.mappings, [mock_mapping.return_value])

        mock_mapping.reset_mock()

        mock_instance1 = mock.MagicMock()
        mock_instance2 = mock.MagicMock()
        mock_mapping.side_effect = [mock_instance1, mock_instance2]

        basic_interface_dict = {
            "interface_name": "com.astarte.Test",
            "version_major": 0,
            "version_minor": 1,
            "type": "datastream",
            "ownership": "device",
            "aggregation": "object",
            "mappings": [
                {"mapping": "number 1"},
                {"mapping": "number 2"},
            ],
        }

        basic_interface = Interface(basic_interface_dict)

        calls = [
            mock.call({"mapping": "number 1"}, "datastream"),
            mock.call({"mapping": "number 2"}, "datastream"),
        ]
        mock_mapping.assert_has_calls(calls)
        self.assertEqual(mock_mapping.call_count, 2)
        self.assertEqual(basic_interface.name, "com.astarte.Test")
        self.assertEqual(basic_interface.version_major, 0)
        self.assertEqual(basic_interface.version_minor, 1)
        self.assertEqual(basic_interface.type, "datastream")
        self.assertEqual(basic_interface.ownership, "device")
        self.assertEqual(basic_interface.aggregation, "object")
        self.assertEqual(basic_interface.mappings, [mock_instance1, mock_instance2])

    @mock.patch("astarte.device.interface.Mapping")
    def test_initialize_missing_interface_name_raises(self, mock_mapping):
        basic_interface_dict = {
            "version_major": 0,
            "version_minor": 1,
            "type": "datastream",
            "ownership": "device",
            "mappings": [
                {"mapping": "number 1"},
            ],
        }

        self.assertRaises(InterfaceFileDecodeError, lambda: Interface(basic_interface_dict))
        mock_mapping.assert_not_called()

    @mock.patch("astarte.device.interface.Mapping")
    def test_initialize_missing_version_major_raises(self, mock_mapping):
        basic_interface_dict = {
            "interface_name": "com.astarte.Test",
            "version_minor": 1,
            "type": "datastream",
            "ownership": "device",
            "mappings": [
                {"mapping": "number 1"},
            ],
        }

        self.assertRaises(InterfaceFileDecodeError, lambda: Interface(basic_interface_dict))
        mock_mapping.assert_not_called()

    @mock.patch("astarte.device.interface.Mapping")
    def test_initialize_missing_version_minor_raises(self, mock_mapping):
        basic_interface_dict = {
            "interface_name": "com.astarte.Test",
            "version_major": 0,
            "type": "datastream",
            "ownership": "device",
            "mappings": [
                {"mapping": "number 1"},
            ],
        }

        self.assertRaises(InterfaceFileDecodeError, lambda: Interface(basic_interface_dict))
        mock_mapping.assert_not_called()

    @mock.patch("astarte.device.interface.Mapping")
    def test_initialize_incorrect_type_raises(self, mock_mapping):
        basic_interface_dict = {
            "interface_name": "com.astarte.Test",
            "version_major": 0,
            "version_minor": 1,
            "type": "foo",
            "ownership": "device",
            "mappings": [
                {"mapping": "number 1"},
            ],
        }

        self.assertRaises(InterfaceFileDecodeError, lambda: Interface(basic_interface_dict))
        mock_mapping.assert_not_called()

    @mock.patch("astarte.device.interface.Mapping")
    def test_initialize_incorrect_ownership_raises(self, mock_mapping):
        basic_interface_dict = {
            "interface_name": "com.astarte.Test",
            "version_major": 0,
            "version_minor": 1,
            "type": "datastream",
            "ownership": "foo",
            "mappings": [
                {"mapping": "number 1"},
            ],
        }

        self.assertRaises(InterfaceFileDecodeError, lambda: Interface(basic_interface_dict))
        mock_mapping.assert_not_called()

    @mock.patch("astarte.device.interface.Mapping")
    def test_initialize_same_minor_major_version_raises(self, mock_mapping):
        basic_interface_dict = {
            "interface_name": "com.astarte.Test",
            "version_major": 0,
            "version_minor": 0,
            "type": "datastream",
            "ownership": "device",
            "mappings": [
                {"mapping": "number 1"},
            ],
        }

        self.assertRaises(InterfaceFileDecodeError, lambda: Interface(basic_interface_dict))
        mock_mapping.assert_not_called()

    @mock.patch("astarte.device.interface.Mapping")
    def test_initialize_incorrect_aggregation_raises(self, mock_mapping):
        basic_interface_dict = {
            "interface_name": "com.astarte.Test",
            "version_major": 0,
            "version_minor": 1,
            "type": "datastream",
            "ownership": "device",
            "aggregation": "foo",
            "mappings": [
                {"mapping": "number 1"},
            ],
        }

        self.assertRaises(InterfaceFileDecodeError, lambda: Interface(basic_interface_dict))
        mock_mapping.assert_not_called()

    def test_initialize_duplicate_mapping_raises(self):
        self.interface_minimal_dict["mappings"].append(self.interface_minimal_dict["mappings"][0])
        self.assertRaises(InterfaceFileDecodeError, lambda: Interface(self.interface_minimal_dict))

    @mock.patch("astarte.device.interface.Mapping")
    def test_initialize_missing_mappings_raises(self, mock_mapping):
        basic_interface_dict = {
            "interface_name": "com.astarte.Test",
            "version_major": 0,
            "version_minor": 1,
            "type": "datastream",
            "ownership": "device",
            "aggregation": "individual",
        }

        self.assertRaises(InterfaceFileDecodeError, lambda: Interface(basic_interface_dict))
        mock_mapping.assert_not_called()

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

    def test_is_property_endpoint_resettable(self):
        minimal_interface_dict = {
            "interface_name": "com.astarte.Test",
            "version_major": 0,
            "version_minor": 1,
            "type": "properties",
            "ownership": "device",
            "mappings": [
                {
                    "endpoint": "/test/endpoint/one",
                    "type": "integer",
                },
                {"endpoint": "/test/endpoint/two", "type": "boolean", "allow_unset": True},
            ],
        }
        interface_individual = Interface(minimal_interface_dict)
        self.assertFalse(interface_individual.is_property_endpoint_resettable("/test/endpoint/one"))
        self.assertTrue(interface_individual.is_property_endpoint_resettable("/test/endpoint/two"))

    @mock.patch.object(Interface, "get_mapping")
    def test_is_property_endpoint_resettable_not_a_property(self, mock_get_mapping):
        minimal_interface_dict = {
            "interface_name": "com.astarte.Test",
            "version_major": 0,
            "version_minor": 1,
            "type": "datastream",
            "ownership": "device",
            "mappings": [
                {
                    "endpoint": "/test/endpoint/one",
                    "type": "integer",
                },
                {
                    "endpoint": "/test/endpoint/two",
                    "type": "boolean",
                },
            ],
        }

        interface_individual = Interface(minimal_interface_dict)

        self.assertFalse(interface_individual.is_property_endpoint_resettable("/test/endpoint/one"))
        self.assertFalse(interface_individual.is_property_endpoint_resettable("/test/endpoint/two"))
        mock_get_mapping.assert_not_called()

    def test_is_property_endpoint_resettable_invalid_endpoint(self):
        minimal_interface_dict = {
            "interface_name": "com.astarte.Test",
            "version_major": 0,
            "version_minor": 1,
            "type": "properties",
            "ownership": "device",
            "mappings": [
                {
                    "endpoint": "/test/endpoint/one",
                    "type": "integer",
                },
                {"endpoint": "/test/endpoint/two", "type": "boolean", "allow_unset": True},
            ],
        }
        interface_individual = Interface(minimal_interface_dict)
        self.assertFalse(interface_individual.is_property_endpoint_resettable("/test/endint/one"))

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

    def test_validate_path_individual(self):
        minimal_interface_dict = {
            "interface_name": "com.astarte.Test",
            "version_major": 0,
            "version_minor": 1,
            "type": "datastream",
            "ownership": "device",
            "mappings": [
                {
                    "endpoint": "/test/endpoint/one",
                    "type": "integer",
                },
                {
                    "endpoint": "/test/endpoint/two",
                    "type": "boolean",
                },
            ],
        }
        interface_individual = Interface(minimal_interface_dict)
        self.assertIsNone(interface_individual.validate_path("/test/endpoint/one", 11))

    def test_validate_path_individual_no_mapping_for_endpoint(self):
        minimal_interface_dict = {
            "interface_name": "com.astarte.Test",
            "version_major": 0,
            "version_minor": 1,
            "type": "datastream",
            "ownership": "device",
            "mappings": [
                {
                    "endpoint": "/test/endpoint/one",
                    "type": "integer",
                },
                {
                    "endpoint": "/test/endpoint/two",
                    "type": "boolean",
                },
            ],
        }
        interface_individual = Interface(minimal_interface_dict)
        res = interface_individual.validate_path("/test/endnt/one", 11)
        self.assertIsInstance(res, ValidationError)
        self.assertEqual(res.msg, "Path /test/endnt/one not in the com.astarte.Test interface.")

    def test_validate_path_aggregate(self):
        minimal_interface_dict = {
            "interface_name": "com.astarte.Test",
            "version_major": 0,
            "version_minor": 1,
            "type": "datastream",
            "ownership": "device",
            "aggregation": "object",
            "mappings": [
                {
                    "endpoint": "/test/endpoint/one",
                    "type": "integer",
                },
                {
                    "endpoint": "/test/endpoint/two",
                    "type": "boolean",
                },
            ],
        }
        interface_individual = Interface(minimal_interface_dict)
        payload = {"endpoint/one": "payload 1", "endpoint/two": "payload_2"}
        self.assertIsNone(interface_individual.validate_path("/test", payload))

    def test_validate_path_aggregate_no_mapping_for_endpoint(self):
        minimal_interface_dict = {
            "interface_name": "com.astarte.Test",
            "version_major": 0,
            "version_minor": 1,
            "type": "datastream",
            "ownership": "device",
            "aggregation": "object",
            "mappings": [
                {
                    "endpoint": "/test/endpoint/one",
                    "type": "integer",
                },
                {
                    "endpoint": "/test/endpoint/two",
                    "type": "boolean",
                },
            ],
        }
        interface_individual = Interface(minimal_interface_dict)
        payload = {"endpoint/one": "payload 1", "endnt/two": "payload_2"}
        res = interface_individual.validate_path("/test", payload)
        self.assertIsInstance(res, ValidationError)
        self.assertEqual(res.msg, "Path /test/endnt/two not in the com.astarte.Test interface.")

    @mock.patch.object(Mapping, "validate_payload", return_value=None)
    def test_validate_payload_individual(self, mock_validate_payload):
        minimal_interface_dict = {
            "interface_name": "com.astarte.Test",
            "version_major": 0,
            "version_minor": 1,
            "type": "datastream",
            "ownership": "device",
            "mappings": [
                {
                    "endpoint": "/test/endpoint/one",
                    "type": "integer",
                },
                {
                    "endpoint": "/test/endpoint/two",
                    "type": "boolean",
                },
            ],
        }
        interface_individual = Interface(minimal_interface_dict)

        payload = mock.MagicMock()
        self.assertIsNone(interface_individual.validate_payload("/test/endpoint/one", payload))

        mock_validate_payload.assert_called_once_with(payload)

    @mock.patch.object(Mapping, "validate_payload")
    def test_validate_payload_individual_incorrect_payload(self, mock_validate_payload):
        minimal_interface_dict = {
            "interface_name": "com.astarte.Test",
            "version_major": 0,
            "version_minor": 1,
            "type": "datastream",
            "ownership": "device",
            "mappings": [
                {
                    "endpoint": "/test/endpoint/one",
                    "type": "integer",
                },
                {
                    "endpoint": "/test/endpoint/two",
                    "type": "boolean",
                },
            ],
        }
        interface_individual = Interface(minimal_interface_dict)

        payload = mock.MagicMock()
        self.assertEqual(
            interface_individual.validate_payload("/test/endpoint/one", payload),
            mock_validate_payload.return_value,
        )

        mock_validate_payload.assert_called_once_with(payload)

    @mock.patch.object(Mapping, "validate_payload", return_value=None)
    def test_validate_payload_aggregate(self, mock_validate_payload):
        minimal_interface_dict = {
            "interface_name": "com.astarte.Test",
            "version_major": 0,
            "version_minor": 1,
            "type": "datastream",
            "ownership": "device",
            "aggregation": "object",
            "mappings": [
                {
                    "endpoint": "/test/endpoint/one",
                    "type": "integer",
                },
                {
                    "endpoint": "/test/endpoint/two",
                    "type": "boolean",
                },
            ],
        }
        interface_individual = Interface(minimal_interface_dict)

        payload = {"one": mock.MagicMock(), "two": mock.MagicMock()}
        self.assertIsNone(interface_individual.validate_payload("/test/endpoint", payload))

        calls = [mock.call(payload["one"]), mock.call(payload["two"])]
        mock_validate_payload.assert_has_calls(calls)
        self.assertEqual(mock_validate_payload.call_count, 2)

    @mock.patch.object(Mapping, "validate_payload")
    def test_validate_payload_aggregate_payload_not_a_dict(self, mock_validate_payload):
        minimal_interface_dict = {
            "interface_name": "com.astarte.Test",
            "version_major": 0,
            "version_minor": 1,
            "type": "datastream",
            "ownership": "device",
            "aggregation": "object",
            "mappings": [
                {
                    "endpoint": "/test/endpoint/one",
                    "type": "integer",
                },
                {
                    "endpoint": "/test/endpoint/two",
                    "type": "boolean",
                },
            ],
        }
        interface_individual = Interface(minimal_interface_dict)

        payload = 42
        res = interface_individual.validate_payload("/test/endpoint", payload)
        self.assertIsInstance(res, ValidationError)
        self.assertEqual(res.msg, "Payload not a dict for aggregated interface com.astarte.Test.")
        mock_validate_payload.assert_not_called()

    @mock.patch.object(Mapping, "validate_payload")
    def test_validate_payload_aggregate_incorrect_payload(self, mock_validate_payload):
        minimal_interface_dict = {
            "interface_name": "com.astarte.Test",
            "version_major": 0,
            "version_minor": 1,
            "type": "datastream",
            "ownership": "device",
            "aggregation": "object",
            "mappings": [
                {
                    "endpoint": "/test/endpoint/one",
                    "type": "integer",
                },
                {
                    "endpoint": "/test/endpoint/two",
                    "type": "boolean",
                },
            ],
        }
        interface_individual = Interface(minimal_interface_dict)

        mock_payload_invalid = mock.MagicMock()
        mock_validate_payload.side_effect = [None, mock_payload_invalid]

        payload = {"one": mock.MagicMock(), "two": mock.MagicMock()}
        self.assertEqual(
            interface_individual.validate_payload("/test/endpoint", payload), mock_payload_invalid
        )

        calls = [mock.call(payload["one"]), mock.call(payload["two"])]
        mock_validate_payload.assert_has_calls(calls)
        self.assertEqual(mock_validate_payload.call_count, 2)

    def test_validate_payload_aggregate_missing_one_endpoint_from_payload(self):
        minimal_interface_dict = {
            "interface_name": "com.astarte.Test",
            "version_major": 0,
            "version_minor": 1,
            "type": "datastream",
            "ownership": "device",
            "aggregation": "object",
            "mappings": [
                {
                    "endpoint": "/test/endpoint/one",
                    "type": "integer",
                },
                {
                    "endpoint": "/test/endpoint/two",
                    "type": "boolean",
                },
            ],
        }
        interface_individual = Interface(minimal_interface_dict)

        payload = {"one": 22}
        self.assertIsInstance(
            interface_individual.validate_payload("/test/endpoint", payload), ValidationError
        )

    @mock.patch.object(Mapping, "validate_payload")
    @mock.patch.object(Mapping, "validate_timestamp")
    def test_validate_individual(self, mock_validate_timestamp, mock_validate_payload):
        basic_interface_dict = {
            "interface_name": "com.astarte.Test",
            "version_major": 0,
            "version_minor": 1,
            "type": "datastream",
            "ownership": "device",
            "mappings": [
                {
                    "endpoint": "/test/int",
                    "type": "integer",
                }
            ],
        }
        interface_individual = Interface(basic_interface_dict)

        mock_validate_payload.return_value = None

        mock_payload = mock.MagicMock()
        mock_timestamp = mock.MagicMock()
        result = interface_individual.validate("/test/int", mock_payload, mock_timestamp)
        self.assertEqual(result, mock_validate_timestamp.return_value)

        mock_validate_payload.assert_called_once_with(mock_payload)
        mock_validate_timestamp.assert_called_once_with(mock_timestamp)

    @mock.patch.object(Mapping, "validate_payload")
    @mock.patch.object(Mapping, "validate_timestamp")
    def test_validate_individual_parametric(self, mock_validate_timestamp, mock_validate_payload):
        parametric_interface_dict = {
            "interface_name": "com.astarte.Test",
            "version_major": 0,
            "version_minor": 1,
            "type": "datastream",
            "ownership": "device",
            "mappings": [
                {
                    "endpoint": r"/test/%{var}/int",
                    "type": "integer",
                }
            ],
        }
        interface_individual = Interface(parametric_interface_dict)

        mock_validate_payload.return_value = None

        mock_payload = mock.MagicMock()
        mock_timestamp = mock.MagicMock()
        result = interface_individual.validate("/test/11/int", mock_payload, mock_timestamp)
        self.assertEqual(result, mock_validate_timestamp.return_value)

        mock_validate_payload.assert_called_once_with(mock_payload)
        mock_validate_timestamp.assert_called_once_with(mock_timestamp)

    @mock.patch.object(Mapping, "validate_payload")
    @mock.patch.object(Mapping, "validate_timestamp")
    def test_validate_individual_non_valid_payload_err(
        self, mock_validate_timestamp, mock_validate_payload
    ):
        basic_interface_dict = {
            "interface_name": "com.astarte.Test",
            "version_major": 0,
            "version_minor": 1,
            "type": "datastream",
            "ownership": "device",
            "mappings": [
                {
                    "endpoint": "/test/int",
                    "type": "integer",
                }
            ],
        }
        interface_individual = Interface(basic_interface_dict)

        mock_payload = mock.MagicMock()
        mock_timestamp = mock.MagicMock()
        result = interface_individual.validate("/test/int", mock_payload, mock_timestamp)
        self.assertEqual(result, mock_validate_payload.return_value)

        mock_validate_payload.assert_called_once_with(mock_payload)
        mock_validate_timestamp.assert_called_once_with(mock_timestamp)

    def test_validate_individual_endpoint_not_existing_err(self):
        basic_interface_dict = {
            "interface_name": "com.astarte.Test",
            "version_major": 0,
            "version_minor": 1,
            "type": "datastream",
            "ownership": "device",
            "mappings": [
                {
                    "endpoint": "/test/int",
                    "type": "integer",
                }
            ],
        }
        interface_individual = Interface(basic_interface_dict)
        result = interface_individual.validate("/test/something", 42, None)
        assert isinstance(result, ValidationError)
        assert result.msg == "Path /test/something not in the com.astarte.Test interface."

    @mock.patch.object(Mapping, "validate_payload")
    @mock.patch.object(Mapping, "validate_timestamp")
    def test_validate_aggregate(self, mock_validate_timestamp, mock_validate_payload):
        aggregated_interface_dict = {
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
                },
                {
                    "endpoint": "/test/two",
                    "type": "boolean",
                },
            ],
        }
        interface_aggregate = Interface(aggregated_interface_dict)

        mock_validate_payload.return_value = None
        mock_validate_timestamp.return_value = None

        payload = {
            "one": mock.MagicMock(),
            "two": mock.MagicMock(),
        }
        mock_timestamp = mock.MagicMock()
        self.assertIsNone(interface_aggregate.validate("/test", payload, mock_timestamp))

        calls = [mock.call(payload["one"]), mock.call(payload["two"])]
        mock_validate_payload.assert_has_calls(calls)
        self.assertEqual(mock_validate_payload.call_count, 2)
        calls = [mock.call(mock_timestamp), mock.call(mock_timestamp)]
        mock_validate_timestamp.assert_has_calls(calls)
        self.assertEqual(mock_validate_timestamp.call_count, 2)

    @mock.patch.object(Mapping, "validate_payload")
    @mock.patch.object(Mapping, "validate_timestamp")
    def test_validate_aggregate_parametric(self, mock_validate_timestamp, mock_validate_payload):
        aggregated_interface_dict = {
            "interface_name": "com.astarte.Test",
            "version_major": 0,
            "version_minor": 1,
            "type": "datastream",
            "ownership": "device",
            "aggregation": "object",
            "mappings": [
                {
                    "endpoint": r"/test/%{var}/one",
                    "type": "integer",
                },
                {
                    "endpoint": r"/test/%{var}/two",
                    "type": "boolean",
                },
            ],
        }
        interface_aggregate = Interface(aggregated_interface_dict)

        mock_validate_payload.return_value = None
        mock_validate_timestamp.return_value = None

        payload = {
            "one": mock.MagicMock(),
            "two": mock.MagicMock(),
        }
        mock_timestamp = mock.MagicMock()
        self.assertIsNone(interface_aggregate.validate("/test/43", payload, mock_timestamp))

        calls = [mock.call(payload["one"]), mock.call(payload["two"])]
        mock_validate_payload.assert_has_calls(calls)
        self.assertEqual(mock_validate_payload.call_count, 2)
        calls = [mock.call(mock_timestamp), mock.call(mock_timestamp)]
        mock_validate_timestamp.assert_has_calls(calls)
        self.assertEqual(mock_validate_timestamp.call_count, 2)

    def test_validate_aggregate_payload_not_dict_err(self):
        aggregated_interface_dict = {
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
                },
                {
                    "endpoint": "/test/two",
                    "type": "boolean",
                },
            ],
        }
        interface_aggregate = Interface(aggregated_interface_dict)
        result = interface_aggregate.validate("/test/one", 42, None)
        assert isinstance(result, ValidationError)
        assert (
            result.msg
            == "The interface com.astarte.Test is aggregate, but the payload is not a dictionary."
        )

    def test_validate_aggregate_non_existing_endpoint_err(self):
        aggregated_interface_dict = {
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
                },
                {
                    "endpoint": "/test/two",
                    "type": "boolean",
                },
            ],
        }
        interface_aggregate = Interface(aggregated_interface_dict)
        payload = {
            "one": 42,
            "three": True,
        }
        result = interface_aggregate.validate("/test", payload, None)
        assert isinstance(result, ValidationError)
        assert result.msg == "Path /test/three not in the com.astarte.Test interface."

    @mock.patch.object(Mapping, "validate_payload")
    @mock.patch.object(Mapping, "validate_timestamp")
    def test_validate_aggregate_non_valid_payload_err(
        self, mock_validate_timestamp, mock_validate_payload
    ):
        aggregated_interface_dict = {
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
                },
                {
                    "endpoint": "/test/two",
                    "type": "boolean",
                },
            ],
        }
        interface_aggregate = Interface(aggregated_interface_dict)

        mock_failed_validation = mock.MagicMock()
        mock_validate_payload.side_effect = [None, mock_failed_validation]
        mock_validate_timestamp.return_value = None

        payload = {
            "one": mock.MagicMock(),
            "two": mock.MagicMock(),
        }
        mock_timestamp = mock.MagicMock()
        self.assertEqual(
            interface_aggregate.validate("/test", payload, mock_timestamp), mock_failed_validation
        )

        calls = [mock.call(payload["one"]), mock.call(payload["two"])]
        mock_validate_payload.assert_has_calls(calls)
        self.assertEqual(mock_validate_payload.call_count, 2)
        calls = [mock.call(mock_timestamp), mock.call(mock_timestamp)]
        mock_validate_timestamp.assert_has_calls(calls)
        self.assertEqual(mock_validate_timestamp.call_count, 2)

    @mock.patch.object(Mapping, "validate_payload")
    @mock.patch.object(Mapping, "validate_timestamp")
    def test_validate_aggregate_non_valid_timestamp_err(
        self, mock_validate_timestamp, mock_validate_payload
    ):
        aggregated_interface_dict = {
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
                },
                {
                    "endpoint": "/test/two",
                    "type": "boolean",
                },
            ],
        }
        interface_aggregate = Interface(aggregated_interface_dict)

        mock_failed_validation = mock.MagicMock()
        mock_validate_payload.side_effect = [None, None]
        mock_validate_timestamp.side_effect = [None, mock_failed_validation]

        payload = {
            "one": mock.MagicMock(),
            "two": mock.MagicMock(),
        }
        mock_timestamp = mock.MagicMock()
        self.assertEqual(
            interface_aggregate.validate("/test", payload, mock_timestamp), mock_failed_validation
        )

        calls = [mock.call(payload["one"]), mock.call(payload["two"])]
        mock_validate_payload.assert_has_calls(calls)
        self.assertEqual(mock_validate_payload.call_count, 2)
        calls = [mock.call(mock_timestamp), mock.call(mock_timestamp)]
        mock_validate_timestamp.assert_has_calls(calls)
        self.assertEqual(mock_validate_timestamp.call_count, 2)

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
        assert result.msg == "Path /test/two of com.astarte.Test interface not in payload."

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
            result.msg == r"Path /test/%{some_id}/two of com.astarte.Test interface not in payload."
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
