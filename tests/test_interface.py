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
from unittest import mock

from astarte.device import Interface, Mapping
from astarte.device.exceptions import (
    InterfaceFileDecodeError,
    InterfaceNotFoundError,
    ValidationError,
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
    def test_interface_initialize(self, mock_mapping):
        mock_instance1 = mock.MagicMock()
        mock_instance1.explicit_timestamp = False
        mock_instance1.reliability = 0
        mock_instance1.endpoint = "endpoint mapping 1"
        mock_mapping.side_effect = [mock_instance1]

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

        mock_mapping.assert_called_once_with({"mapping": "number 1"}, True)
        self.assertEqual(basic_interface.name, "com.astarte.Test")
        self.assertEqual(basic_interface.version_major, 0)
        self.assertEqual(basic_interface.version_minor, 1)
        self.assertEqual(basic_interface.type, "datastream")
        self.assertEqual(basic_interface.ownership, "device")
        self.assertEqual(basic_interface.aggregation, "individual")
        self.assertEqual(basic_interface.mappings, [mock_instance1])

        mock_mapping.reset_mock()

        mock_instance1 = mock.MagicMock()
        mock_instance1.explicit_timestamp = False
        mock_instance1.reliability = 0
        mock_instance1.endpoint = "endpoint mapping 1"
        mock_instance2 = mock.MagicMock()
        mock_instance2.explicit_timestamp = False
        mock_instance2.reliability = 0
        mock_instance2.endpoint = "endpoint mapping 2"
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
            mock.call({"mapping": "number 1"}, True),
            mock.call({"mapping": "number 2"}, True),
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
    def test_interface_initialize_missing_interface_name_raises(self, mock_mapping):
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
    def test_interface_initialize_incorrect_name_raises(self, mock_mapping):
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

        Interface(basic_interface_dict)
        mock_mapping.assert_called_once_with({"mapping": "number 1"}, True)
        mock_mapping.reset_mock()

        basic_interface_dict["interface_name"] = ""
        self.assertRaises(InterfaceFileDecodeError, lambda: Interface(basic_interface_dict))
        mock_mapping.assert_not_called()

        basic_interface_dict["interface_name"] = "."
        self.assertRaises(InterfaceFileDecodeError, lambda: Interface(basic_interface_dict))
        mock_mapping.assert_not_called()

        basic_interface_dict["interface_name"] = "a"
        Interface(basic_interface_dict)
        mock_mapping.assert_called_once_with({"mapping": "number 1"}, True)
        mock_mapping.reset_mock()

        basic_interface_dict["interface_name"] = "1"
        self.assertRaises(InterfaceFileDecodeError, lambda: Interface(basic_interface_dict))
        mock_mapping.assert_not_called()

        basic_interface_dict["interface_name"] = "a1111"
        Interface(basic_interface_dict)
        mock_mapping.assert_called_once_with({"mapping": "number 1"}, True)
        mock_mapping.reset_mock()

        basic_interface_dict["interface_name"] = "T3ST"
        Interface(basic_interface_dict)
        mock_mapping.assert_called_once_with({"mapping": "number 1"}, True)
        mock_mapping.reset_mock()

        basic_interface_dict["interface_name"] = "T3S-T"
        self.assertRaises(InterfaceFileDecodeError, lambda: Interface(basic_interface_dict))
        mock_mapping.assert_not_called()

        basic_interface_dict["interface_name"] = "T3ST."
        self.assertRaises(InterfaceFileDecodeError, lambda: Interface(basic_interface_dict))
        mock_mapping.assert_not_called()

        basic_interface_dict["interface_name"] = "T3ST.t"
        Interface(basic_interface_dict)
        mock_mapping.assert_called_once_with({"mapping": "number 1"}, True)
        mock_mapping.reset_mock()

        basic_interface_dict["interface_name"] = "T3ST.1"
        self.assertRaises(InterfaceFileDecodeError, lambda: Interface(basic_interface_dict))
        mock_mapping.assert_not_called()

        basic_interface_dict["interface_name"] = "T3ST.t3st"
        Interface(basic_interface_dict)
        mock_mapping.assert_called_once_with({"mapping": "number 1"}, True)
        mock_mapping.reset_mock()

        basic_interface_dict["interface_name"] = "T3ST.T3ST"
        Interface(basic_interface_dict)
        mock_mapping.assert_called_once_with({"mapping": "number 1"}, True)
        mock_mapping.reset_mock()

        basic_interface_dict["interface_name"] = "T3ST.T3-T"
        self.assertRaises(InterfaceFileDecodeError, lambda: Interface(basic_interface_dict))
        mock_mapping.assert_not_called()

        basic_interface_dict["interface_name"] = "t3st.t.t3st"
        Interface(basic_interface_dict)
        mock_mapping.assert_called_once_with({"mapping": "number 1"}, True)
        mock_mapping.reset_mock()

        basic_interface_dict["interface_name"] = "t3st.t3st.t3st"
        Interface(basic_interface_dict)
        mock_mapping.assert_called_once_with({"mapping": "number 1"}, True)
        mock_mapping.reset_mock()

        basic_interface_dict["interface_name"] = "t3st.1est.t3st"
        Interface(basic_interface_dict)
        mock_mapping.assert_called_once_with({"mapping": "number 1"}, True)
        mock_mapping.reset_mock()

        basic_interface_dict["interface_name"] = "t3st.t-3st.t3st"
        Interface(basic_interface_dict)
        mock_mapping.assert_called_once_with({"mapping": "number 1"}, True)
        mock_mapping.reset_mock()

    @mock.patch("astarte.device.interface.Mapping")
    def test_interface_initialize_missing_version_major_raises(self, mock_mapping):
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
    def test_interface_initialize_missing_version_minor_raises(self, mock_mapping):
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
    def test_interface_initialize_same_minor_major_version_raises(self, mock_mapping):
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
    def test_interface_initialize_incorrect_type_raises(self, mock_mapping):
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
    def test_interface_initialize_incorrect_ownership_raises(self, mock_mapping):
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
    def test_interface_initialize_incorrect_aggregation_raises(self, mock_mapping):
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

    @mock.patch("astarte.device.interface.Mapping")
    def test_interface_initialize_property_object_raises(self, mock_mapping):
        basic_interface_dict = {
            "interface_name": "com.astarte.Test",
            "version_major": 0,
            "version_minor": 1,
            "type": "properties",
            "ownership": "device",
            "aggregation": "object",
            "mappings": [
                {"mapping": "number 1"},
            ],
        }

        self.assertRaises(InterfaceFileDecodeError, lambda: Interface(basic_interface_dict))
        mock_mapping.assert_not_called()

    @mock.patch("astarte.device.interface.Mapping")
    def test_interface_initialize_duplicate_mapping_raises(self, mock_mapping):
        mock_instance1 = mock.MagicMock()
        mock_instance1.explicit_timestamp = False
        mock_instance1.reliability = 0
        mock_instance1.endpoint = "endpoint mapping"
        mock_instance2 = mock.MagicMock()
        mock_instance2.explicit_timestamp = False
        mock_instance2.reliability = 0
        mock_instance2.endpoint = "endpoint mapping"
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

        self.assertRaises(InterfaceFileDecodeError, lambda: Interface(basic_interface_dict))

    @mock.patch("astarte.device.interface.Mapping")
    def test_interface_initialize_missing_mappings_raises(self, mock_mapping):
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

    @mock.patch("astarte.device.interface.Mapping")
    def test_interface_initialize_object_with_different_timestamps_raises(self, mock_mapping):
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

        mock_instance1 = mock.MagicMock()
        mock_instance1.explicit_timestamp = False
        mock_instance1.reliability = 0
        mock_instance2 = mock.MagicMock()
        mock_instance2.explicit_timestamp = True
        mock_instance2.reliability = 0
        mock_mapping.side_effect = [mock_instance1, mock_instance2]

        self.assertRaises(InterfaceFileDecodeError, lambda: Interface(basic_interface_dict))

        calls = [
            mock.call({"mapping": "number 1"}, True),
            mock.call({"mapping": "number 2"}, True),
        ]
        mock_mapping.assert_has_calls(calls)
        self.assertEqual(mock_mapping.call_count, 2)

    @mock.patch("astarte.device.interface.Mapping")
    def test_interface_initialize_object_with_different_reliability_raises(self, mock_mapping):
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

        mock_instance1 = mock.MagicMock()
        mock_instance1.explicit_timestamp = False
        mock_instance1.reliability = 0
        mock_instance2 = mock.MagicMock()
        mock_instance2.explicit_timestamp = False
        mock_instance2.reliability = 2
        mock_mapping.side_effect = [mock_instance1, mock_instance2]

        self.assertRaises(InterfaceFileDecodeError, lambda: Interface(basic_interface_dict))

        calls = [
            mock.call({"mapping": "number 1"}, True),
            mock.call({"mapping": "number 2"}, True),
        ]
        mock_mapping.assert_has_calls(calls)
        self.assertEqual(mock_mapping.call_count, 2)

    @mock.patch("astarte.device.interface.Mapping")
    def test_interface_is_aggregation_object(self, mock_mapping):
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

        mock_instance1 = mock.MagicMock()
        mock_instance1.explicit_timestamp = False
        mock_instance1.reliability = 0
        mock_instance1.endpoint = "endpoint mapping 1"
        mock_mapping.side_effect = [mock_instance1]

        # Defaults to individual when it misses the aggregation field
        interface_individual = Interface(basic_interface_dict)
        assert not interface_individual.is_aggregation_object()

        mock_mapping.reset_mock()

        mock_instance1 = mock.MagicMock()
        mock_instance1.explicit_timestamp = False
        mock_instance1.reliability = 0
        mock_instance1.endpoint = "endpoint mapping 1"
        mock_mapping.side_effect = [mock_instance1]

        basic_interface_dict["aggregation"] = "object"
        interface_aggregated = Interface(basic_interface_dict)
        assert interface_aggregated.is_aggregation_object()

    @mock.patch("astarte.device.interface.Mapping")
    def test_interface_is_server_owned(self, mock_mapping):
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

        mock_instance1 = mock.MagicMock()
        mock_instance1.explicit_timestamp = False
        mock_instance1.reliability = 0
        mock_instance1.endpoint = "endpoint mapping 1"
        mock_mapping.side_effect = [mock_instance1]

        interface_device_owned = Interface(basic_interface_dict)
        assert not interface_device_owned.is_server_owned()

        mock_mapping.reset_mock()

        mock_instance1 = mock.MagicMock()
        mock_instance1.explicit_timestamp = False
        mock_instance1.reliability = 0
        mock_instance1.endpoint = "endpoint mapping 1"
        mock_mapping.side_effect = [mock_instance1]

        basic_interface_dict["ownership"] = "server"
        interface_device_owned = Interface(basic_interface_dict)
        assert interface_device_owned.is_server_owned()

    @mock.patch("astarte.device.interface.Mapping")
    def test_interface_is_type_properties(self, mock_mapping):
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

        mock_instance1 = mock.MagicMock()
        mock_instance1.explicit_timestamp = False
        mock_instance1.reliability = 0
        mock_instance1.endpoint = "endpoint mapping 1"
        mock_mapping.side_effect = [mock_instance1]

        interface_datastream = Interface(basic_interface_dict)
        assert not interface_datastream.is_type_properties()

        mock_mapping.reset_mock()

        mock_instance1 = mock.MagicMock()
        mock_instance1.allow_unset = False
        mock_instance1.endpoint = "endpoint mapping 1"
        mock_mapping.side_effect = [mock_instance1]

        basic_interface_dict["type"] = "properties"
        interface_property = Interface(basic_interface_dict)
        assert interface_property.is_type_properties()

    @mock.patch("astarte.device.interface.Mapping")
    def test_interface_is_property_endpoint_resettable(self, mock_mapping):
        basic_interface_dict = {
            "interface_name": "com.astarte.Test",
            "version_major": 0,
            "version_minor": 1,
            "type": "properties",
            "ownership": "device",
            "mappings": [
                {"mapping": "number 1"},
                {"mapping": "number 2"},
            ],
        }

        mock_instance1 = mock.MagicMock()
        mock_instance1.allow_unset = False
        mock_instance1.endpoint = "/test/endpoint/one"
        mock_instance1.validate_path.side_effect = None
        mock_instance2 = mock.MagicMock()
        mock_instance2.allow_unset = True
        mock_instance2.endpoint = "/test/endpoint/two"
        mock_instance2.validate_path.side_effect = ValidationError("")
        mock_mapping.side_effect = [mock_instance1, mock_instance2]

        interface_individual = Interface(basic_interface_dict)

        self.assertFalse(interface_individual.is_property_endpoint_resettable("/test/endpoint/one"))

        mock_instance1.allow_unset = False
        mock_instance1.endpoint = "/test/endpoint/one"
        mock_instance1.validate_path.side_effect = ValidationError("")
        mock_instance2.allow_unset = True
        mock_instance2.endpoint = "/test/endpoint/two"
        mock_instance2.validate_path.side_effect = None

        self.assertTrue(interface_individual.is_property_endpoint_resettable("/test/endpoint/two"))

    @mock.patch("astarte.device.interface.Mapping")
    def test_interface_is_property_endpoint_resettable_not_a_property(self, mock_mapping):
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

        mock_instance1 = mock.MagicMock()
        mock_instance1.explicit_timestamp = False
        mock_instance1.reliability = 0
        mock_instance1.endpoint = "endpoint mapping 1"
        mock_mapping.side_effect = [mock_instance1]

        interface_individual = Interface(basic_interface_dict)

        self.assertFalse(interface_individual.is_property_endpoint_resettable("/test/endpoint/one"))
        self.assertFalse(interface_individual.is_property_endpoint_resettable("/test/endpoint/two"))

    def test_interface_is_property_endpoint_resettable_invalid_endpoint(self):
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
                {
                    "endpoint": "/test/endpoint/two",
                    "type": "boolean",
                    "allow_unset": True,
                },
            ],
        }
        interface_individual = Interface(minimal_interface_dict)
        self.assertFalse(interface_individual.is_property_endpoint_resettable("/test/endint/one"))

    @mock.patch.object(Mapping, "validate_path")
    def test_interface_get_mapping(self, mock_validate_path):
        new_mappings = [
            {"endpoint": "/test/one", "type": "integer"},
            {"endpoint": "/test/two", "type": "boolean"},
        ]
        self.interface_minimal_dict["mappings"] = new_mappings
        interface_simple_endpoint = Interface(self.interface_minimal_dict)

        path = "/test/two"
        mock_validate_path.side_effect = [ValidationError(""), None]

        self.assertIsNotNone(interface_simple_endpoint.get_mapping(path))

        mock_validate_path.assert_has_calls([mock.call("/test/two"), mock.call("/test/two")])
        self.assertEqual(mock_validate_path.call_count, 2)

    @mock.patch.object(Mapping, "validate_path")
    def test_interface_get_mapping_no_mapping(self, mock_validate_path):
        new_mappings = [
            {"endpoint": "/test/one", "type": "integer"},
            {"endpoint": "/test/two", "type": "boolean"},
        ]
        self.interface_minimal_dict["mappings"] = new_mappings
        interface_simple_endpoint = Interface(self.interface_minimal_dict)

        path = "/test/three"
        mock_validate_path.side_effect = [ValidationError(""), ValidationError("")]

        self.assertIsNone(interface_simple_endpoint.get_mapping(path))

        mock_validate_path.assert_has_calls([mock.call("/test/three"), mock.call("/test/three")])
        self.assertEqual(mock_validate_path.call_count, 2)

    def test_interface_validate_path_individual(self):
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
        interface_individual.validate_path("/test/endpoint/one", 11)

    def test_interface_validate_path_individual_no_mapping_for_endpoint(self):
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
        self.assertRaises(
            ValidationError,
            lambda: interface_individual.validate_path("/test/endnt/one", 11),
        )

    def test_interface_validate_path_aggregate(self):
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
        interface_individual.validate_path("/test", payload)

    def test_interface_validate_path_aggregate_no_mapping_for_endpoint(self):
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
        self.assertRaises(
            ValidationError,
            lambda: interface_individual.validate_path("/test", payload),
        )

    @mock.patch.object(Mapping, "validate_payload")
    def test_interface_validate_payload_individual(self, mock_validate_payload):
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
        interface_individual.validate_payload("/test/endpoint/one", payload)

        mock_validate_payload.assert_called_once_with(payload)

    @mock.patch.object(Mapping, "validate_payload")
    def test_interface_validate_payload_individual_non_existing_mapping(
        self, mock_validate_payload
    ):
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
        self.assertRaises(
            ValidationError,
            lambda: interface_individual.validate_payload("/test/endpoint/three", payload),
        )

        mock_validate_payload.assert_not_called()

    @mock.patch.object(Mapping, "validate_payload")
    def test_interface_validate_payload_individual_incorrect_payload(self, mock_validate_payload):
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

        mock_validate_payload.side_effect = ValidationError("")

        interface_individual = Interface(minimal_interface_dict)

        payload = mock.MagicMock()
        self.assertRaises(
            ValidationError,
            lambda: interface_individual.validate_payload("/test/endpoint/one", payload),
        )

        mock_validate_payload.assert_called_once_with(payload)

    @mock.patch.object(Mapping, "validate_payload", return_value=None)
    def test_interface_validate_payload_aggregate(self, mock_validate_payload):
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
    def test_interface_validate_payload_aggregate_payload_not_a_dict(self, mock_validate_payload):
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
        self.assertRaises(
            ValidationError,
            lambda: interface_individual.validate_payload("/test/endpoint", payload),
        )
        mock_validate_payload.assert_not_called()

    @mock.patch.object(Mapping, "validate_payload")
    def test_interface_validate_payload_aggregate_non_existing_mapping(self, mock_validate_payload):
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

        payload = {"one": mock.MagicMock(), "three": mock.MagicMock()}
        self.assertRaises(
            ValidationError,
            lambda: interface_individual.validate_payload("/test/endpoint", payload),
        )
        mock_validate_payload.assert_called_once_with(payload["one"])

    @mock.patch.object(Mapping, "validate_payload")
    def test_interface_validate_payload_aggregate_incorrect_payload(self, mock_validate_payload):
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

        mock_validate_payload.side_effect = [None, ValidationError("")]

        payload = {"one": mock.MagicMock(), "two": mock.MagicMock()}
        self.assertRaises(
            ValidationError,
            lambda: interface_individual.validate_payload("/test/endpoint", payload),
        )

        calls = [mock.call(payload["one"]), mock.call(payload["two"])]
        mock_validate_payload.assert_has_calls(calls)
        self.assertEqual(mock_validate_payload.call_count, 2)

    def test_interface_validate_payload_aggregate_missing_one_endpoint_from_payload(
        self,
    ):
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
        self.assertRaises(
            ValidationError,
            lambda: interface_individual.validate_payload("/test/endpoint", payload),
        )

    @mock.patch.object(Mapping, "validate_payload")
    @mock.patch.object(Mapping, "validate_timestamp")
    def test_interface_validate_payload_and_timestamp_individual(
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
        interface_individual.validate_payload_and_timestamp(
            "/test/int", mock_payload, mock_timestamp
        )

        mock_validate_payload.assert_called_once_with(mock_payload)
        mock_validate_timestamp.assert_called_once_with(mock_timestamp)

    @mock.patch.object(Mapping, "validate_payload")
    @mock.patch.object(Mapping, "validate_timestamp")
    def test_interface_validate_payload_and_timestamp_individual_parametric(
        self, mock_validate_timestamp, mock_validate_payload
    ):
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

        mock_payload = mock.MagicMock()
        mock_timestamp = mock.MagicMock()
        interface_individual.validate_payload_and_timestamp(
            "/test/s11/int", mock_payload, mock_timestamp
        )

        mock_validate_payload.assert_called_once_with(mock_payload)
        mock_validate_timestamp.assert_called_once_with(mock_timestamp)

    @mock.patch.object(Mapping, "validate_payload")
    @mock.patch.object(Mapping, "validate_timestamp")
    def test_interface_validate_payload_and_timestamp_individual_non_valid_payload_err(
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

        mock_validate_payload.side_effect = ValidationError("")

        mock_payload = mock.MagicMock()
        mock_timestamp = mock.MagicMock()
        self.assertRaises(
            ValidationError,
            lambda: interface_individual.validate_payload_and_timestamp(
                "/test/int", mock_payload, mock_timestamp
            ),
        )

        mock_validate_payload.assert_called_once_with(mock_payload)
        mock_validate_timestamp.assert_not_called()

    @mock.patch.object(Mapping, "validate_payload")
    @mock.patch.object(Mapping, "validate_timestamp")
    def test_interface_validate_payload_and_timestamp_individual_endpoint_not_existing_err(
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
        self.assertRaises(
            ValidationError,
            lambda: interface_individual.validate_payload_and_timestamp(
                "/test/something", 42, None
            ),
        )

        mock_validate_payload.assert_not_called()
        mock_validate_timestamp.assert_not_called()

    @mock.patch.object(Mapping, "validate_payload")
    @mock.patch.object(Mapping, "validate_timestamp")
    def test_interface_validate_payload_and_timestamp_aggregate(
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

        payload = {
            "one": mock.MagicMock(),
            "two": mock.MagicMock(),
        }
        mock_timestamp = mock.MagicMock()
        interface_aggregate.validate_payload_and_timestamp("/test", payload, mock_timestamp)

        calls = [mock.call(payload["one"]), mock.call(payload["two"])]
        mock_validate_payload.assert_has_calls(calls)
        self.assertEqual(mock_validate_payload.call_count, 2)
        calls = [mock.call(mock_timestamp), mock.call(mock_timestamp)]
        mock_validate_timestamp.assert_has_calls(calls)
        self.assertEqual(mock_validate_timestamp.call_count, 2)

    @mock.patch.object(Mapping, "validate_payload")
    @mock.patch.object(Mapping, "validate_timestamp")
    def test_interface_validate_payload_and_timestamp_aggregate_parametric(
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

        payload = {
            "one": mock.MagicMock(),
            "two": mock.MagicMock(),
        }
        mock_timestamp = mock.MagicMock()
        interface_aggregate.validate_payload_and_timestamp("/test/s43", payload, mock_timestamp)

        calls = [mock.call(payload["one"]), mock.call(payload["two"])]
        mock_validate_payload.assert_has_calls(calls)
        self.assertEqual(mock_validate_payload.call_count, 2)
        calls = [mock.call(mock_timestamp), mock.call(mock_timestamp)]
        mock_validate_timestamp.assert_has_calls(calls)
        self.assertEqual(mock_validate_timestamp.call_count, 2)

    @mock.patch.object(Mapping, "validate_payload")
    @mock.patch.object(Mapping, "validate_timestamp")
    def test_interface_validate_payload_and_timestamp_aggregate_payload_not_dict_err(
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
        self.assertRaises(
            ValidationError,
            lambda: interface_aggregate.validate_payload_and_timestamp("/test/one", 42, None),
        )

        mock_validate_payload.assert_not_called()
        mock_validate_timestamp.assert_not_called()

    @mock.patch.object(Mapping, "validate_payload")
    @mock.patch.object(Mapping, "validate_timestamp")
    def test_interface_validate_payload_and_timestamp_aggregate_non_existing_endpoint_err(
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
        payload = {
            "one": 42,
            "three": True,
        }
        self.assertRaises(
            ValidationError,
            lambda: interface_aggregate.validate_payload_and_timestamp("/test", payload, None),
        )

        mock_validate_payload.assert_called_once_with(payload["one"])
        mock_validate_timestamp.assert_called_once_with(None)

    @mock.patch.object(Mapping, "validate_payload")
    @mock.patch.object(Mapping, "validate_timestamp")
    def test_interface_validate_payload_and_timestamp_aggregate_non_valid_payload_err(
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

        mock_validate_payload.side_effect = [None, ValidationError("")]
        mock_validate_timestamp.return_value = None

        payload = {
            "one": mock.MagicMock(),
            "two": mock.MagicMock(),
        }
        mock_timestamp = mock.MagicMock()
        self.assertRaises(
            ValidationError,
            lambda: interface_aggregate.validate_payload_and_timestamp(
                "/test", payload, mock_timestamp
            ),
        )

        calls = [mock.call(payload["one"]), mock.call(payload["two"])]
        mock_validate_payload.assert_has_calls(calls)
        self.assertEqual(mock_validate_payload.call_count, 2)
        mock_validate_timestamp.assert_called_once_with(mock_timestamp)

    @mock.patch.object(Mapping, "validate_payload")
    @mock.patch.object(Mapping, "validate_timestamp")
    def test_interface_validate_payload_and_timestamp_aggregate_non_valid_timestamp_err(
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

        mock_validate_payload.side_effect = [None, None]
        mock_validate_timestamp.side_effect = [None, ValidationError("")]

        payload = {
            "one": mock.MagicMock(),
            "two": mock.MagicMock(),
        }
        mock_timestamp = mock.MagicMock()
        self.assertRaises(
            ValidationError,
            lambda: interface_aggregate.validate_payload_and_timestamp(
                "/test", payload, mock_timestamp
            ),
        )

        calls = [mock.call(payload["one"]), mock.call(payload["two"])]
        mock_validate_payload.assert_has_calls(calls)
        self.assertEqual(mock_validate_payload.call_count, 2)
        calls = [mock.call(mock_timestamp), mock.call(mock_timestamp)]
        mock_validate_timestamp.assert_has_calls(calls)
        self.assertEqual(mock_validate_timestamp.call_count, 2)

    @mock.patch.object(Mapping, "validate_payload")
    @mock.patch.object(Mapping, "validate_timestamp")
    def test_interface_validate_payload_and_timestamp_aggregate_missing_endpoint_err(
        self, mock_validate_timestamp, mock_validate_payload
    ):
        self.interface_minimal_dict["aggregation"] = "object"
        new_mappings = [
            {
                "endpoint": "/test/one",
                "type": "integer",
            },
            {
                "endpoint": "/test/two",
                "type": "boolean",
            },
        ]
        self.interface_minimal_dict["mappings"] = new_mappings
        interface_server = Interface(self.interface_minimal_dict)
        payload = {
            "one": 42,
        }
        self.assertRaises(
            ValidationError,
            lambda: interface_server.validate_payload_and_timestamp("/test", payload, None),
        )

        mock_validate_payload.assert_called_once_with(payload["one"])
        mock_validate_timestamp.assert_called_once_with(None)

    @mock.patch.object(Mapping, "validate_payload")
    @mock.patch.object(Mapping, "validate_timestamp")
    def test_interface_validate_payload_and_timestamp_aggregate_parametric_missing_endpoint_err(
        self, mock_validate_timestamp, mock_validate_payload
    ):
        self.interface_minimal_dict["aggregation"] = "object"
        new_mappings = [
            {
                "endpoint": r"/test/%{some_id}/one",
                "type": "integer",
            },
            {
                "endpoint": r"/test/%{some_id}/two",
                "type": "boolean",
            },
        ]
        self.interface_minimal_dict["mappings"] = new_mappings
        interface_server = Interface(self.interface_minimal_dict)
        payload = {
            "one": 42,
        }
        self.assertRaises(
            ValidationError,
            lambda: interface_server.validate_payload_and_timestamp("/test/s42", payload, None),
        )

        mock_validate_payload.assert_called_once_with(payload["one"])
        mock_validate_timestamp.assert_called_once_with(None)

    def test_interface_get_reliability_individual(self):
        interface_simple_endpoint = Interface(self.interface_minimal_dict)
        self.assertEqual(interface_simple_endpoint.get_reliability("/test/int"), 0)

    def test_interface_get_reliability_property(self):
        self.interface_minimal_dict["aggregation"] = "individual"
        self.interface_minimal_dict["type"] = "properties"
        interface_simple_endpoint = Interface(self.interface_minimal_dict)
        self.assertEqual(interface_simple_endpoint.get_reliability("/test/int"), 2)

    def test_interface_get_reliability_aggregate_datastream(self):
        self.interface_minimal_dict["aggregation"] = "object"
        self.interface_minimal_dict["type"] = "datastream"
        self.interface_minimal_dict["mappings"][0]["reliability"] = "guaranteed"
        interface_simple_endpoint = Interface(self.interface_minimal_dict)
        self.assertEqual(interface_simple_endpoint.get_reliability("/test/int"), 1)

    def test_interface_get_reliability_individual_datastream(self):
        self.interface_minimal_dict["aggregation"] = "individual"
        self.interface_minimal_dict["type"] = "datastream"
        self.interface_minimal_dict["mappings"][0]["reliability"] = "guaranteed"
        interface_simple_endpoint = Interface(self.interface_minimal_dict)
        self.assertEqual(interface_simple_endpoint.get_reliability("/test/int"), 1)

    def test_interface_get_reliability_non_existent_path(self):
        interface_simple_endpoint = Interface(self.interface_minimal_dict)
        self.assertRaises(
            InterfaceNotFoundError,
            lambda: interface_simple_endpoint.get_reliability("/missing/endpoint"),
        )
