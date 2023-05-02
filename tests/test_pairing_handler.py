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
from uuid import UUID
from astarte.device import generate_device_id, generate_random_device_id


class UnitTests(unittest.TestCase):
    def test_generate_device_id(self):
        namespace = UUID("f79ad91f-c638-4889-ae74-9d001a3b4cf8")
        mac_address = "11:22:33:44:55:66"
        device_uuid = generate_device_id(namespace=namespace, unique_data=mac_address)
        target_device_id = "F94uGRPGVDGN6bGzEjtNhw"
        self.assertEqual(device_uuid, target_device_id)

    def test_generate_random_device_id(self):
        device_uuid = generate_random_device_id()
        self.assertIsNotNone(device_uuid)
