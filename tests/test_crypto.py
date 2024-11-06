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
# pylint: disable=too-many-statements,too-many-instance-attributes,missing-return-doc
# pylint: disable=missing-return-type-doc,no-value-for-parameter,protected-access,
# pylint: disable=too-many-public-methods,no-self-use

import unittest
from unittest import mock

from cryptography import x509
from cryptography.hazmat.primitives import serialization
from cryptography.x509.oid import NameOID

from astarte.device import crypto


class UnitTests(unittest.TestCase):
    def setUp(self):
        pass

    @mock.patch("astarte.device.crypto.hashes.SHA256")
    @mock.patch("astarte.device.crypto.x509.CertificateSigningRequestBuilder")
    @mock.patch("astarte.device.crypto.serialization.load_pem_private_key")
    @mock.patch("astarte.device.crypto.open", new_callable=mock.mock_open)
    @mock.patch("astarte.device.crypto.path.exists", return_value=True)
    def test_generate_csr_existing_key(
        self,
        mock_exist,
        open_mock,
        mock_load_pem_private_key,
        mock_certificate_signing_request_builder,
        mock_sha256,
    ):
        fp_instance = open_mock.return_value
        fp_instance.read.return_value = bytes("key file content", "utf-8")

        private_key = mock.MagicMock()
        mock_load_pem_private_key.return_value = private_key

        csr_builder_instance = mock_certificate_signing_request_builder.return_value
        csr_builder_instance.subject_name.return_value = csr_builder_instance
        csr_instance = mock.MagicMock()
        csr_builder_instance.sign.return_value = csr_instance

        csr_instance.public_bytes.return_value = bytes("csr public bytes", "utf-8")

        x509_name = x509.Name(
            [
                # Provide various details about who we are.
                x509.NameAttribute(NameOID.ORGANIZATION_NAME, "Devices"),
                x509.NameAttribute(NameOID.COMMON_NAME, "realm name/device id"),
            ]
        )

        crypto.generate_csr("realm name", "device id", "crypto store dir")
        # Checks over the loading of the key
        mock_exist.assert_called_once_with("crypto store dir/device.key")
        open_mock.assert_called_once_with("crypto store dir/device.key", "rb")
        fp_instance.read.assert_called_once()
        mock_load_pem_private_key.assert_called_once_with(
            fp_instance.read.return_value, password=None
        )

        # Checks over the creation of the CSR
        mock_certificate_signing_request_builder.assert_called_once()
        csr_builder_instance.subject_name.assert_called_once_with(x509_name)
        csr_builder_instance.sign.assert_called_once_with(private_key, mock_sha256.return_value)

        # Generic checks

        csr_instance.public_bytes.assert_called_once_with(serialization.Encoding.PEM)

    @mock.patch("astarte.device.crypto.hashes.SHA256")
    @mock.patch("astarte.device.crypto.x509.CertificateSigningRequestBuilder")
    @mock.patch("astarte.device.crypto.serialization.NoEncryption")
    @mock.patch("astarte.device.crypto.open", new_callable=mock.mock_open)
    @mock.patch("astarte.device.crypto.ec.SECP256R1")
    @mock.patch("astarte.device.crypto.ec.generate_private_key")
    @mock.patch("astarte.device.crypto.path.exists", return_value=False)
    def test_generate_csr_new_key(
        self,
        mock_exist,
        mock_generate_private_key,
        mock_SECP256R1,
        open_mock,
        mock_no_encryption,
        mock_certificate_signing_request_builder,
        mock_sha256,
    ):
        private_key = mock.MagicMock()
        mock_generate_private_key.return_value = private_key

        fp_instance = open_mock.return_value
        fp_instance.write.return_value = bytes("key file content", "utf-8")

        csr_builder_instance = mock_certificate_signing_request_builder.return_value
        csr_builder_instance.subject_name.return_value = csr_builder_instance
        csr_instance = mock.MagicMock()
        csr_builder_instance.sign.return_value = csr_instance

        csr_instance.public_bytes.return_value = bytes("csr public bytes", "utf-8")

        x509_name = x509.Name(
            [
                # Provide various details about who we are.
                x509.NameAttribute(NameOID.ORGANIZATION_NAME, "Devices"),
                x509.NameAttribute(NameOID.COMMON_NAME, "realm name/device id"),
            ]
        )

        crypto.generate_csr("realm name", "device id", "crypto store dir")
        # Checks over the creation of the key
        mock_exist.assert_called_once_with("crypto store dir/device.key")
        mock_generate_private_key.assert_called_once_with(curve=mock_SECP256R1.return_value)
        open_mock.assert_called_once_with("crypto store dir/device.key", "wb")
        private_key.private_bytes.assert_called_once_with(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=mock_no_encryption.return_value,
        )
        open_mock.return_value.write.assert_called_once_with(private_key.private_bytes.return_value)

        # Checks over the creation of the CSR
        mock_certificate_signing_request_builder.assert_called_once()
        csr_builder_instance.subject_name.assert_called_once_with(x509_name)
        csr_builder_instance.sign.assert_called_once_with(private_key, mock_sha256.return_value)

        # Generic checks

        csr_instance.public_bytes.assert_called_once_with(serialization.Encoding.PEM)

    @mock.patch("astarte.device.crypto.open", new_callable=mock.mock_open)
    @mock.patch("astarte.device.crypto.x509.load_pem_x509_certificate")
    def test_import_device_certificate(self, mock_load_pem_x509_certificate, open_mock):
        crypto.import_device_certificate("client serialized certificate", "store directory")

        mock_load_pem_x509_certificate.assert_called_once_with(
            "client serialized certificate".encode("ascii")
        )
        open_mock.assert_called_once_with("store directory/device.crt", "wb")
        mock_load_pem_x509_certificate.return_value.public_bytes.assert_called_once_with(
            encoding=serialization.Encoding.PEM
        )
        open_mock.return_value.write.assert_called_once_with(
            mock_load_pem_x509_certificate.return_value.public_bytes.return_value
        )

    @mock.patch("astarte.device.crypto.certificate_is_valid", return_value=True)
    @mock.patch("astarte.device.crypto.path.exists", side_effect=[True, True])
    def test_device_has_certificate(self, mock_exists, mock_certificate_is_valid):
        # Has certificate
        self.assertTrue(
            crypto.device_has_certificate(
                "device_id",
                "realm_name",
                "credential_secret",
                "pairing_base_url",
                False,
                "store directory",
            )
        )

        mock_exists.assert_has_calls(
            [mock.call("store directory/device.crt"), mock.call("store directory/device.key")]
        )
        self.assertEqual(mock_exists.call_count, 2)
        mock_certificate_is_valid.assert_called_once_with(
            "device_id",
            "realm_name",
            "credential_secret",
            "pairing_base_url",
            False,
            "store directory",
        )

        # Certificate is not valid
        mock_exists.reset_mock()
        mock_certificate_is_valid.reset_mock()

        mock_certificate_is_valid.return_value = False
        mock_exists.side_effect = [True, True]

        self.assertFalse(
            crypto.device_has_certificate(
                "device_id",
                "realm_name",
                "credential_secret",
                "pairing_base_url",
                False,
                "store directory",
            )
        )

        mock_exists.assert_has_calls(
            [mock.call("store directory/device.crt"), mock.call("store directory/device.key")]
        )
        self.assertEqual(mock_exists.call_count, 2)
        mock_certificate_is_valid.assert_called_once_with(
            "device_id",
            "realm_name",
            "credential_secret",
            "pairing_base_url",
            False,
            "store directory",
        )

        # Certificate file is not present
        mock_exists.reset_mock()
        mock_certificate_is_valid.reset_mock()

        mock_certificate_is_valid.return_value = True
        mock_exists.side_effect = [False, True]

        self.assertFalse(
            crypto.device_has_certificate(
                "device_id",
                "realm_name",
                "credential_secret",
                "pairing_base_url",
                False,
                "store directory",
            )
        )

        mock_exists.assert_called_once_with("store directory/device.crt")
        mock_certificate_is_valid.assert_not_called()

        # Key file is not present
        mock_exists.reset_mock()
        mock_certificate_is_valid.reset_mock()

        mock_certificate_is_valid.return_value = True
        mock_exists.side_effect = [True, False]

        self.assertFalse(
            crypto.device_has_certificate(
                "device_id",
                "realm_name",
                "credential_secret",
                "pairing_base_url",
                False,
                "store directory",
            )
        )

        mock_exists.assert_has_calls(
            [mock.call("store directory/device.crt"), mock.call("store directory/device.key")]
        )
        self.assertEqual(mock_exists.call_count, 2)
        mock_certificate_is_valid.assert_not_called()

    @mock.patch("astarte.device.crypto.pairing_handler.verify_device_certificate")
    @mock.patch("astarte.device.crypto.open", new_callable=mock.mock_open)
    def test_certificate_is_valid(self, open_mock, mock_verify_device_certificate):
        open_mock.return_value.read.return_value = "certificate content"
        mock_verify_device_certificate.return_value = True

        self.assertTrue(
            crypto.certificate_is_valid(
                "device_id",
                "realm_name",
                "credential_secret",
                "pairing_base_url",
                False,
                "store directory",
            )
        )

        open_mock.assert_called_once_with("store directory/device.crt", "r", encoding="utf-8")
        mock_verify_device_certificate.assert_called_once_with(
            "device_id",
            "realm_name",
            "credential_secret",
            "pairing_base_url",
            False,
            "certificate content",
        )

    @mock.patch("astarte.device.crypto.pairing_handler.verify_device_certificate")
    @mock.patch("astarte.device.crypto.open", new_callable=mock.mock_open)
    def test_certificate_is_valid_empty_certificate(
        self, open_mock, mock_verify_device_certificate
    ):
        open_mock.return_value.read.return_value = ""
        mock_verify_device_certificate.return_value = True

        self.assertFalse(
            crypto.certificate_is_valid(
                "device_id",
                "realm_name",
                "credential_secret",
                "pairing_base_url",
                False,
                "store directory",
            )
        )

        open_mock.assert_called_once_with("store directory/device.crt", "r", encoding="utf-8")
        mock_verify_device_certificate.assert_not_called()

    @mock.patch("astarte.device.crypto.pairing_handler.verify_device_certificate")
    @mock.patch("astarte.device.crypto.open", new_callable=mock.mock_open)
    def test_certificate_is_invalid(self, open_mock, mock_verify_device_certificate):
        open_mock.return_value.read.return_value = "certificate content"
        mock_verify_device_certificate.return_value = False

        self.assertFalse(
            crypto.certificate_is_valid(
                "device_id",
                "realm_name",
                "credential_secret",
                "pairing_base_url",
                False,
                "store directory",
            )
        )

        open_mock.assert_called_once_with("store directory/device.crt", "r", encoding="utf-8")
        mock_verify_device_certificate.assert_called_once_with(
            "device_id",
            "realm_name",
            "credential_secret",
            "pairing_base_url",
            False,
            "certificate content",
        )
