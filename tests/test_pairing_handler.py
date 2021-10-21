#!/usr/bin/env python
# -*- coding: utf-8 -*-

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
