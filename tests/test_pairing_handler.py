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

import datetime
import unittest
from http import HTTPStatus
from unittest import mock
from uuid import UUID

from jwt import exceptions

from astarte.device import pairing_handler
from astarte.device.exceptions import (
    APIError,
    AuthorizationError,
    DeviceAlreadyRegisteredError,
    JWTGenerationError,
)


class UnitTests(unittest.TestCase):
    def setUp(self):
        pass

    @mock.patch("astarte.device.pairing_handler.requests.post")
    @mock.patch("astarte.device.pairing_handler.jwt.encode")
    @mock.patch("astarte.device.pairing_handler.datetime")
    @mock.patch("astarte.device.pairing_handler.timedelta")
    @mock.patch("astarte.device.pairing_handler.open", new_callable=mock.mock_open)
    def test_register_device_with_private_key(
        self, mock_open, mock_timedelta, mock_datetime, mock_jwt_encode, mock_request_post
    ):
        # Mock return values for __generate_token
        mock_open.return_value.read.return_value = "<private key content>"
        mock_datetime.now.return_value = datetime.datetime.now()
        mock_timedelta.return_value = datetime.timedelta(seconds=30)

        # Mock return values for __register_device
        mock_request_post.return_value.status_code = HTTPStatus.CREATED
        mock_request_post.return_value.json.return_value = {
            "data": {"credentials_secret": "<credential secret>"}
        }

        result = pairing_handler.register_device_with_private_key(
            "<device id>",
            "<realm name>",
            "<private key file>",
            "<pairing base URL>",
            ignore_ssl_errors=False,
        )

        # Checks for __generate_token
        mock_open.assert_called_once_with("<private key file>", "r", encoding="utf-8")
        mock_open.return_value.read.assert_called_once()
        mock_datetime.now.assert_called_once()
        mock_timedelta.assert_called_once_with(seconds=30)
        expected_claims = {
            "a_pa": [".*::.*"],
            "iat": mock_datetime.now.return_value,
            "exp": mock_datetime.now.return_value + mock_timedelta.return_value,
        }
        mock_jwt_encode.assert_called_once_with(
            expected_claims, "<private key content>", algorithm="RS256"
        )
        mock_jwt_encode.return_value.decode.assert_called_once()

        # Checks for __register_device
        expected_json = {"data": {"hw_id": "<device id>"}}
        expected_headers = {
            "Authorization": f"Bearer {mock_jwt_encode.return_value.decode.return_value}"
        }
        mock_request_post.assert_called_once_with(
            "<pairing base URL>/v1/<realm name>/agent/devices",
            json=expected_json,
            headers=expected_headers,
            verify=True,
            timeout=30,
        )
        self.assertEqual(result, "<credential secret>")

    @mock.patch("astarte.device.pairing_handler.requests.post")
    @mock.patch("astarte.device.pairing_handler.jwt.encode")
    @mock.patch("astarte.device.pairing_handler.datetime")
    @mock.patch("astarte.device.pairing_handler.timedelta")
    @mock.patch("astarte.device.pairing_handler.open", new_callable=mock.mock_open)
    def test_register_device_with_private_key_ignore_ssl_errors(
        self, mock_open, mock_timedelta, mock_datetime, mock_jwt_encode, mock_request_post
    ):
        # Mock return values for __generate_token
        mock_open.return_value.read.return_value = "<private key content>"
        mock_datetime.now.return_value = datetime.datetime.now()
        mock_timedelta.return_value = datetime.timedelta(seconds=30)

        # Mock return values for __register_device
        mock_request_post.return_value.status_code = HTTPStatus.CREATED
        mock_request_post.return_value.json.return_value = {
            "data": {"credentials_secret": "<credential secret>"}
        }

        result = pairing_handler.register_device_with_private_key(
            "<device id>",
            "<realm name>",
            "<private key file>",
            "<pairing base URL>",
            ignore_ssl_errors=True,
        )

        # Checks for __generate_token
        mock_open.assert_called_once_with("<private key file>", "r", encoding="utf-8")
        mock_open.return_value.read.assert_called_once()
        mock_datetime.now.assert_called_once()
        mock_timedelta.assert_called_once_with(seconds=30)
        expected_claims = {
            "a_pa": [".*::.*"],
            "iat": mock_datetime.now.return_value,
            "exp": mock_datetime.now.return_value + mock_timedelta.return_value,
        }
        mock_jwt_encode.assert_called_once_with(
            expected_claims, "<private key content>", algorithm="RS256"
        )
        mock_jwt_encode.return_value.decode.assert_called_once()

        # Checks for __register_device
        expected_json = {"data": {"hw_id": "<device id>"}}
        expected_headers = {
            "Authorization": f"Bearer {mock_jwt_encode.return_value.decode.return_value}"
        }
        mock_request_post.assert_called_once_with(
            "<pairing base URL>/v1/<realm name>/agent/devices",
            json=expected_json,
            headers=expected_headers,
            verify=False,
            timeout=30,
        )
        self.assertEqual(result, "<credential secret>")

    @mock.patch("astarte.device.pairing_handler.requests.post")
    @mock.patch("astarte.device.pairing_handler.jwt.encode")
    @mock.patch("astarte.device.pairing_handler.datetime")
    @mock.patch("astarte.device.pairing_handler.timedelta")
    @mock.patch("astarte.device.pairing_handler.open", new_callable=mock.mock_open)
    def test_register_device_with_private_key_open_raises(
        self, mock_open, mock_timedelta, mock_datetime, mock_jwt_encode, mock_request_post
    ):
        # Mock return values for __generate_token
        mock_open.side_effect = mock.Mock(side_effect=FileNotFoundError("Msg"))
        mock_open.return_value.read.return_value = "<private key content>"
        mock_datetime.now.return_value = datetime.datetime.now()
        mock_timedelta.return_value = datetime.timedelta(seconds=30)

        # Mock return values for __register_device
        mock_request_post.return_value.status_code = HTTPStatus.CREATED
        mock_request_post.return_value.json.return_value = {
            "data": {"credentials_secret": "<credential secret>"}
        }

        self.assertRaises(
            JWTGenerationError,
            lambda: pairing_handler.register_device_with_private_key(
                "<device id>",
                "<realm name>",
                "<private key file>",
                "<pairing base URL>",
                ignore_ssl_errors=False,
            ),
        )

        # Checks for __generate_token
        mock_open.assert_called_once_with("<private key file>", "r", encoding="utf-8")
        mock_open.return_value.read.assert_not_called()
        mock_datetime.now.assert_not_called()
        mock_timedelta.assert_not_called()
        mock_jwt_encode.assert_not_called()
        mock_jwt_encode.return_value.decode.assert_not_called()

        # Checks for __register_device
        mock_request_post.assert_not_called()

    @mock.patch("astarte.device.pairing_handler.requests.post")
    @mock.patch("astarte.device.pairing_handler.jwt.encode")
    @mock.patch("astarte.device.pairing_handler.datetime")
    @mock.patch("astarte.device.pairing_handler.timedelta")
    @mock.patch("astarte.device.pairing_handler.open", new_callable=mock.mock_open)
    def test_register_device_with_private_key_jwt_encode_raises(
        self, mock_open, mock_timedelta, mock_datetime, mock_jwt_encode, mock_request_post
    ):
        # Mock return values for __generate_token
        mock_open.return_value.read.return_value = "<private key content>"
        mock_datetime.now.return_value = datetime.datetime.now()
        mock_timedelta.return_value = datetime.timedelta(seconds=30)
        mock_jwt_encode.side_effect = mock.Mock(side_effect=exceptions.DecodeError("Msg"))

        # Mock return values for __register_device
        mock_request_post.return_value.status_code = HTTPStatus.CREATED
        mock_request_post.return_value.json.return_value = {
            "data": {"credentials_secret": "<credential secret>"}
        }

        self.assertRaises(
            JWTGenerationError,
            lambda: pairing_handler.register_device_with_private_key(
                "<device id>",
                "<realm name>",
                "<private key file>",
                "<pairing base URL>",
                ignore_ssl_errors=False,
            ),
        )

        # Checks for __generate_token
        mock_open.assert_called_once_with("<private key file>", "r", encoding="utf-8")
        mock_open.return_value.read.assert_called_once()
        mock_datetime.now.assert_called_once()
        mock_timedelta.assert_called_once_with(seconds=30)
        expected_claims = {
            "a_pa": [".*::.*"],
            "iat": mock_datetime.now.return_value,
            "exp": mock_datetime.now.return_value + mock_timedelta.return_value,
        }
        mock_jwt_encode.assert_called_once_with(
            expected_claims, "<private key content>", algorithm="RS256"
        )
        mock_jwt_encode.return_value.decode.assert_not_called()

        # Checks for __register_device
        mock_request_post.assert_not_called()

    @mock.patch("astarte.device.pairing_handler.requests.post")
    @mock.patch("astarte.device.pairing_handler.jwt.encode")
    @mock.patch("astarte.device.pairing_handler.datetime")
    @mock.patch("astarte.device.pairing_handler.timedelta")
    @mock.patch("astarte.device.pairing_handler.open", new_callable=mock.mock_open)
    def test_register_device_with_private_key_jwt_decode_raises(
        self, mock_open, mock_timedelta, mock_datetime, mock_jwt_encode, mock_request_post
    ):
        # Mock return values for __generate_token
        mock_open.return_value.read.return_value = "<private key content>"
        mock_datetime.now.return_value = datetime.datetime.now()
        mock_timedelta.return_value = datetime.timedelta(seconds=30)
        mock_jwt_encode.return_value.decode.side_effect = mock.Mock(
            side_effect=exceptions.DecodeError("Msg")
        )

        # Mock return values for __register_device
        mock_request_post.return_value.status_code = HTTPStatus.CREATED
        mock_request_post.return_value.json.return_value = {
            "data": {"credentials_secret": "<credential secret>"}
        }

        self.assertRaises(
            JWTGenerationError,
            lambda: pairing_handler.register_device_with_private_key(
                "<device id>",
                "<realm name>",
                "<private key file>",
                "<pairing base URL>",
                ignore_ssl_errors=False,
            ),
        )

        # Checks for __generate_token
        mock_open.assert_called_once_with("<private key file>", "r", encoding="utf-8")
        mock_open.return_value.read.assert_called_once()
        mock_datetime.now.assert_called_once()
        mock_timedelta.assert_called_once_with(seconds=30)
        expected_claims = {
            "a_pa": [".*::.*"],
            "iat": mock_datetime.now.return_value,
            "exp": mock_datetime.now.return_value + mock_timedelta.return_value,
        }
        mock_jwt_encode.assert_called_once_with(
            expected_claims, "<private key content>", algorithm="RS256"
        )
        mock_jwt_encode.return_value.decode.assert_called_once()

        # Checks for __register_device
        mock_request_post.assert_not_called()

    @mock.patch("astarte.device.pairing_handler.requests.post")
    @mock.patch("astarte.device.pairing_handler.jwt.encode")
    @mock.patch("astarte.device.pairing_handler.datetime")
    @mock.patch("astarte.device.pairing_handler.timedelta")
    @mock.patch("astarte.device.pairing_handler.open", new_callable=mock.mock_open)
    def test_register_device_with_private_key_http_post_unauthorized_raises(
        self, mock_open, mock_timedelta, mock_datetime, mock_jwt_encode, mock_request_post
    ):
        # Mock return values for __generate_token
        mock_open.return_value.read.return_value = "<private key content>"
        mock_datetime.now.return_value = datetime.datetime.now()
        mock_timedelta.return_value = datetime.timedelta(seconds=30)

        # Mock return values for __register_device
        mock_request_post.return_value.status_code = HTTPStatus.UNAUTHORIZED
        mock_request_post.return_value.json.return_value = {
            "data": {"credentials_secret": "<credential secret>"}
        }

        self.assertRaises(
            AuthorizationError,
            lambda: pairing_handler.register_device_with_private_key(
                "<device id>",
                "<realm name>",
                "<private key file>",
                "<pairing base URL>",
                ignore_ssl_errors=False,
            ),
        )

        # Checks for __generate_token
        mock_open.assert_called_once_with("<private key file>", "r", encoding="utf-8")
        mock_open.return_value.read.assert_called_once()
        mock_datetime.now.assert_called_once()
        mock_timedelta.assert_called_once_with(seconds=30)
        expected_claims = {
            "a_pa": [".*::.*"],
            "iat": mock_datetime.now.return_value,
            "exp": mock_datetime.now.return_value + mock_timedelta.return_value,
        }
        mock_jwt_encode.assert_called_once_with(
            expected_claims, "<private key content>", algorithm="RS256"
        )
        mock_jwt_encode.return_value.decode.assert_called_once()

        # Checks for __register_device
        expected_json = {"data": {"hw_id": "<device id>"}}
        expected_headers = {
            "Authorization": f"Bearer {mock_jwt_encode.return_value.decode.return_value}"
        }
        mock_request_post.assert_called_once_with(
            "<pairing base URL>/v1/<realm name>/agent/devices",
            json=expected_json,
            headers=expected_headers,
            verify=True,
            timeout=30,
        )

    @mock.patch("astarte.device.pairing_handler.requests.post")
    @mock.patch("astarte.device.pairing_handler.jwt.encode")
    @mock.patch("astarte.device.pairing_handler.datetime")
    @mock.patch("astarte.device.pairing_handler.timedelta")
    @mock.patch("astarte.device.pairing_handler.open", new_callable=mock.mock_open)
    def test_register_device_with_private_key_http_post_forbidden_raises(
        self, mock_open, mock_timedelta, mock_datetime, mock_jwt_encode, mock_request_post
    ):
        # Mock return values for __generate_token
        mock_open.return_value.read.return_value = "<private key content>"
        mock_datetime.now.return_value = datetime.datetime.now()
        mock_timedelta.return_value = datetime.timedelta(seconds=30)

        # Mock return values for __register_device
        mock_request_post.return_value.status_code = HTTPStatus.FORBIDDEN
        mock_request_post.return_value.json.return_value = {
            "data": {"credentials_secret": "<credential secret>"}
        }

        self.assertRaises(
            AuthorizationError,
            lambda: pairing_handler.register_device_with_private_key(
                "<device id>",
                "<realm name>",
                "<private key file>",
                "<pairing base URL>",
                ignore_ssl_errors=False,
            ),
        )

        # Checks for __generate_token
        mock_open.assert_called_once_with("<private key file>", "r", encoding="utf-8")
        mock_open.return_value.read.assert_called_once()
        mock_datetime.now.assert_called_once()
        mock_timedelta.assert_called_once_with(seconds=30)
        expected_claims = {
            "a_pa": [".*::.*"],
            "iat": mock_datetime.now.return_value,
            "exp": mock_datetime.now.return_value + mock_timedelta.return_value,
        }
        mock_jwt_encode.assert_called_once_with(
            expected_claims, "<private key content>", algorithm="RS256"
        )
        mock_jwt_encode.return_value.decode.assert_called_once()

        # Checks for __register_device
        expected_json = {"data": {"hw_id": "<device id>"}}
        expected_headers = {
            "Authorization": f"Bearer {mock_jwt_encode.return_value.decode.return_value}"
        }
        mock_request_post.assert_called_once_with(
            "<pairing base URL>/v1/<realm name>/agent/devices",
            json=expected_json,
            headers=expected_headers,
            verify=True,
            timeout=30,
        )

    @mock.patch("astarte.device.pairing_handler.requests.post")
    @mock.patch("astarte.device.pairing_handler.jwt.encode")
    @mock.patch("astarte.device.pairing_handler.datetime")
    @mock.patch("astarte.device.pairing_handler.timedelta")
    @mock.patch("astarte.device.pairing_handler.open", new_callable=mock.mock_open)
    def test_register_device_with_private_key_http_post_unprocessable_entity_raises(
        self, mock_open, mock_timedelta, mock_datetime, mock_jwt_encode, mock_request_post
    ):
        # Mock return values for __generate_token
        mock_open.return_value.read.return_value = "<private key content>"
        mock_datetime.now.return_value = datetime.datetime.now()
        mock_timedelta.return_value = datetime.timedelta(seconds=30)

        # Mock return values for __register_device
        mock_request_post.return_value.status_code = HTTPStatus.UNPROCESSABLE_ENTITY
        mock_request_post.return_value.json.return_value = {
            "data": {"credentials_secret": "<credential secret>"}
        }

        self.assertRaises(
            DeviceAlreadyRegisteredError,
            lambda: pairing_handler.register_device_with_private_key(
                "<device id>",
                "<realm name>",
                "<private key file>",
                "<pairing base URL>",
                ignore_ssl_errors=False,
            ),
        )

        # Checks for __generate_token
        mock_open.assert_called_once_with("<private key file>", "r", encoding="utf-8")
        mock_open.return_value.read.assert_called_once()
        mock_datetime.now.assert_called_once()
        mock_timedelta.assert_called_once_with(seconds=30)
        expected_claims = {
            "a_pa": [".*::.*"],
            "iat": mock_datetime.now.return_value,
            "exp": mock_datetime.now.return_value + mock_timedelta.return_value,
        }
        mock_jwt_encode.assert_called_once_with(
            expected_claims, "<private key content>", algorithm="RS256"
        )
        mock_jwt_encode.return_value.decode.assert_called_once()

        # Checks for __register_device
        expected_json = {"data": {"hw_id": "<device id>"}}
        expected_headers = {
            "Authorization": f"Bearer {mock_jwt_encode.return_value.decode.return_value}"
        }
        mock_request_post.assert_called_once_with(
            "<pairing base URL>/v1/<realm name>/agent/devices",
            json=expected_json,
            headers=expected_headers,
            verify=True,
            timeout=30,
        )

    @mock.patch("astarte.device.pairing_handler.requests.post")
    @mock.patch("astarte.device.pairing_handler.jwt.encode")
    @mock.patch("astarte.device.pairing_handler.datetime")
    @mock.patch("astarte.device.pairing_handler.timedelta")
    @mock.patch("astarte.device.pairing_handler.open", new_callable=mock.mock_open)
    def test_register_device_with_private_key_http_post_other_raises(
        self, mock_open, mock_timedelta, mock_datetime, mock_jwt_encode, mock_request_post
    ):
        # Mock return values for __generate_token
        mock_open.return_value.read.return_value = "<private key content>"
        mock_datetime.now.return_value = datetime.datetime.now()
        mock_timedelta.return_value = datetime.timedelta(seconds=30)

        # Mock return values for __register_device
        mock_request_post.return_value.status_code = HTTPStatus.REQUEST_TIMEOUT
        mock_request_post.return_value.json.return_value = {
            "data": {"credentials_secret": "<credential secret>"}
        }

        self.assertRaises(
            APIError,
            lambda: pairing_handler.register_device_with_private_key(
                "<device id>",
                "<realm name>",
                "<private key file>",
                "<pairing base URL>",
                ignore_ssl_errors=False,
            ),
        )

        # Checks for __generate_token
        mock_open.assert_called_once_with("<private key file>", "r", encoding="utf-8")
        mock_open.return_value.read.assert_called_once()
        mock_datetime.now.assert_called_once()
        mock_timedelta.assert_called_once_with(seconds=30)
        expected_claims = {
            "a_pa": [".*::.*"],
            "iat": mock_datetime.now.return_value,
            "exp": mock_datetime.now.return_value + mock_timedelta.return_value,
        }
        mock_jwt_encode.assert_called_once_with(
            expected_claims, "<private key content>", algorithm="RS256"
        )
        mock_jwt_encode.return_value.decode.assert_called_once()

        # Checks for __register_device
        expected_json = {"data": {"hw_id": "<device id>"}}
        expected_headers = {
            "Authorization": f"Bearer {mock_jwt_encode.return_value.decode.return_value}"
        }
        mock_request_post.assert_called_once_with(
            "<pairing base URL>/v1/<realm name>/agent/devices",
            json=expected_json,
            headers=expected_headers,
            verify=True,
            timeout=30,
        )

    @mock.patch("astarte.device.pairing_handler.requests.post")
    def test_register_device_with_jwt_token(self, mock_request_post):
        # Mock return values for __register_device
        mock_request_post.return_value.status_code = HTTPStatus.CREATED
        mock_request_post.return_value.json.return_value = {
            "data": {"credentials_secret": "<credential secret>"}
        }

        result = pairing_handler.register_device_with_jwt_token(
            device_id="<device id>",
            realm="<realm name>",
            jwt_token="<jwt token>",
            pairing_base_url="<pairing base URL>",
            ignore_ssl_errors=False,
        )

        # Checks for __register_device
        expected_json = {"data": {"hw_id": "<device id>"}}
        expected_headers = {"Authorization": "Bearer <jwt token>"}
        mock_request_post.assert_called_once_with(
            "<pairing base URL>/v1/<realm name>/agent/devices",
            json=expected_json,
            headers=expected_headers,
            verify=True,
            timeout=30,
        )
        self.assertEqual(result, "<credential secret>")

    @mock.patch("astarte.device.pairing_handler.urlsafe_b64encode")
    @mock.patch("astarte.device.pairing_handler.uuid5")
    def test_generate_device_id(self, mock_uuid5, mock_urlsafe_b64encode):
        namespace = UUID("f79ad91f-c638-4889-ae74-9d001a3b4cf8")
        result = pairing_handler.generate_device_id(
            namespace=namespace, unique_data="<unique data>"
        )

        mock_uuid5.assert_called_once_with(namespace=namespace, name="<unique data>")
        mock_urlsafe_b64encode.assert_called_once_with(mock_uuid5.return_value.bytes)
        mock_urlsafe_b64encode.return_value.replace.assert_called_once_with(b"=", b"")
        mock_urlsafe_b64encode.return_value.replace.return_value.decode.assert_called_once_with(
            "utf-8"
        )

        self.assertEqual(
            result, mock_urlsafe_b64encode.return_value.replace.return_value.decode.return_value
        )

    @mock.patch("astarte.device.pairing_handler.urlsafe_b64encode")
    @mock.patch("astarte.device.pairing_handler.uuid4")
    def test_generate_random_device_id(self, mock_uuid4, mock_urlsafe_b64encode):
        result = pairing_handler.generate_random_device_id()

        mock_uuid4.assert_called_once_with()
        mock_urlsafe_b64encode.assert_called_once_with(mock_uuid4.return_value.bytes)
        mock_urlsafe_b64encode.return_value.replace.assert_called_once_with(b"=", b"")
        mock_urlsafe_b64encode.return_value.replace.return_value.decode.assert_called_once_with(
            "utf-8"
        )

        self.assertEqual(
            result, mock_urlsafe_b64encode.return_value.replace.return_value.decode.return_value
        )

    @mock.patch("astarte.device.pairing_handler.requests.post")
    @mock.patch("astarte.device.pairing_handler.crypto")
    def test_obtain_device_certificate(self, mock_crypto, mock_request_post):
        mock_request_post.return_value.status_code = HTTPStatus.CREATED
        mock_request_post.return_value.json.return_value = {"data": {"client_crt": "<client crt>"}}

        pairing_handler.obtain_device_certificate(
            device_id="<device id>",
            realm="<realm name>",
            credentials_secret="<credentials secret>",
            pairing_base_url="<pairing base URL>",
            crypto_store_dir="<crypto store dir>",
            ignore_ssl_errors=True,
        )

        mock_crypto.generate_csr.assert_called_once_with(
            "<realm name>", "<device id>", "<crypto store dir>"
        )
        expected_json = {"data": {"csr": mock_crypto.generate_csr.return_value.decode.return_value}}
        expected_headers = {"Authorization": "Bearer <credentials secret>"}
        mock_request_post.assert_called_once_with(
            "<pairing base URL>/v1/<realm name>/devices/<device id>/protocols/astarte_mqtt_v1/credentials",
            json=expected_json,
            headers=expected_headers,
            verify=False,
            timeout=30,
        )
        mock_crypto.import_device_certificate.assert_called_once_with(
            "<client crt>", "<crypto store dir>"
        )

    @mock.patch("astarte.device.pairing_handler.requests.post")
    @mock.patch("astarte.device.pairing_handler.crypto")
    def test_obtain_device_certificate_http_post_unautorized_raises(
        self, mock_crypto, mock_request_post
    ):
        mock_request_post.return_value.status_code = HTTPStatus.UNAUTHORIZED
        mock_request_post.return_value.json.return_value = {"data": {"client_crt": "<client crt>"}}

        self.assertRaises(
            AuthorizationError,
            lambda: pairing_handler.obtain_device_certificate(
                device_id="<device id>",
                realm="<realm name>",
                credentials_secret="<credentials secret>",
                pairing_base_url="<pairing base URL>",
                crypto_store_dir="<crypto store dir>",
                ignore_ssl_errors=True,
            ),
        )

        mock_crypto.generate_csr.assert_called_once_with(
            "<realm name>", "<device id>", "<crypto store dir>"
        )
        expected_json = {"data": {"csr": mock_crypto.generate_csr.return_value.decode.return_value}}
        expected_headers = {"Authorization": "Bearer <credentials secret>"}
        mock_request_post.assert_called_once_with(
            "<pairing base URL>/v1/<realm name>/devices/<device id>/protocols/astarte_mqtt_v1/credentials",
            json=expected_json,
            headers=expected_headers,
            verify=False,
            timeout=30,
        )
        mock_crypto.import_device_certificate.assert_not_called()

    @mock.patch("astarte.device.pairing_handler.requests.post")
    @mock.patch("astarte.device.pairing_handler.crypto")
    def test_obtain_device_certificate_http_post_forbidden_raises(
        self, mock_crypto, mock_request_post
    ):
        mock_request_post.return_value.status_code = HTTPStatus.FORBIDDEN
        mock_request_post.return_value.json.return_value = {"data": {"client_crt": "<client crt>"}}

        self.assertRaises(
            AuthorizationError,
            lambda: pairing_handler.obtain_device_certificate(
                device_id="<device id>",
                realm="<realm name>",
                credentials_secret="<credentials secret>",
                pairing_base_url="<pairing base URL>",
                crypto_store_dir="<crypto store dir>",
                ignore_ssl_errors=True,
            ),
        )

        mock_crypto.generate_csr.assert_called_once_with(
            "<realm name>", "<device id>", "<crypto store dir>"
        )
        expected_json = {"data": {"csr": mock_crypto.generate_csr.return_value.decode.return_value}}
        expected_headers = {"Authorization": "Bearer <credentials secret>"}
        mock_request_post.assert_called_once_with(
            "<pairing base URL>/v1/<realm name>/devices/<device id>/protocols/astarte_mqtt_v1/credentials",
            json=expected_json,
            headers=expected_headers,
            verify=False,
            timeout=30,
        )
        mock_crypto.import_device_certificate.assert_not_called()

    @mock.patch("astarte.device.pairing_handler.requests.post")
    @mock.patch("astarte.device.pairing_handler.crypto")
    def test_obtain_device_certificate_http_post_other_raises(self, mock_crypto, mock_request_post):
        mock_request_post.return_value.status_code = HTTPStatus.REQUEST_TIMEOUT
        mock_request_post.return_value.json.return_value = {"data": {"client_crt": "<client crt>"}}

        self.assertRaises(
            APIError,
            lambda: pairing_handler.obtain_device_certificate(
                device_id="<device id>",
                realm="<realm name>",
                credentials_secret="<credentials secret>",
                pairing_base_url="<pairing base URL>",
                crypto_store_dir="<crypto store dir>",
                ignore_ssl_errors=True,
            ),
        )

        mock_crypto.generate_csr.assert_called_once_with(
            "<realm name>", "<device id>", "<crypto store dir>"
        )
        expected_json = {"data": {"csr": mock_crypto.generate_csr.return_value.decode.return_value}}
        expected_headers = {"Authorization": "Bearer <credentials secret>"}
        mock_request_post.assert_called_once_with(
            "<pairing base URL>/v1/<realm name>/devices/<device id>/protocols/astarte_mqtt_v1/credentials",
            json=expected_json,
            headers=expected_headers,
            verify=False,
            timeout=30,
        )
        mock_crypto.import_device_certificate.assert_not_called()

    @mock.patch("astarte.device.pairing_handler.requests.get")
    def test_obtain_device_transport_information(self, mock_request_get):
        mock_request_get.return_value.status_code = HTTPStatus.OK
        mock_request_get.return_value.json.return_value = {"data": "<device transport information>"}

        result = pairing_handler.obtain_device_transport_information(
            device_id="<device id>",
            realm="<realm name>",
            credentials_secret="<credentials secret>",
            pairing_base_url="<pairing base URL>",
            ignore_ssl_errors=True,
        )

        expected_headers = {"Authorization": "Bearer <credentials secret>"}
        mock_request_get.assert_called_once_with(
            "<pairing base URL>/v1/<realm name>/devices/<device id>",
            headers=expected_headers,
            verify=False,
            timeout=30,
        )
        self.assertEqual(result, "<device transport information>")

    @mock.patch("astarte.device.pairing_handler.requests.get")
    def test_obtain_device_transport_information_http_get_forbidden_raises(self, mock_request_get):
        mock_request_get.return_value.status_code = HTTPStatus.FORBIDDEN
        mock_request_get.return_value.json.return_value = {"data": "<device transport information>"}

        self.assertRaises(
            AuthorizationError,
            lambda: pairing_handler.obtain_device_transport_information(
                device_id="<device id>",
                realm="<realm name>",
                credentials_secret="<credentials secret>",
                pairing_base_url="<pairing base URL>",
                ignore_ssl_errors=True,
            ),
        )

        expected_headers = {"Authorization": "Bearer <credentials secret>"}
        mock_request_get.assert_called_once_with(
            "<pairing base URL>/v1/<realm name>/devices/<device id>",
            headers=expected_headers,
            verify=False,
            timeout=30,
        )

    @mock.patch("astarte.device.pairing_handler.requests.get")
    def test_obtain_device_transport_information_http_get_unautohrized_raises(
        self, mock_request_get
    ):
        mock_request_get.return_value.status_code = HTTPStatus.UNAUTHORIZED
        mock_request_get.return_value.json.return_value = {"data": "<device transport information>"}

        self.assertRaises(
            AuthorizationError,
            lambda: pairing_handler.obtain_device_transport_information(
                device_id="<device id>",
                realm="<realm name>",
                credentials_secret="<credentials secret>",
                pairing_base_url="<pairing base URL>",
                ignore_ssl_errors=True,
            ),
        )

        expected_headers = {"Authorization": "Bearer <credentials secret>"}
        mock_request_get.assert_called_once_with(
            "<pairing base URL>/v1/<realm name>/devices/<device id>",
            headers=expected_headers,
            verify=False,
            timeout=30,
        )

    @mock.patch("astarte.device.pairing_handler.requests.get")
    def test_obtain_device_transport_information_http_get_other_raises(self, mock_request_get):
        mock_request_get.return_value.status_code = HTTPStatus.REQUEST_TIMEOUT
        mock_request_get.return_value.json.return_value = {"data": "<device transport information>"}

        self.assertRaises(
            APIError,
            lambda: pairing_handler.obtain_device_transport_information(
                device_id="<device id>",
                realm="<realm name>",
                credentials_secret="<credentials secret>",
                pairing_base_url="<pairing base URL>",
                ignore_ssl_errors=True,
            ),
        )

        expected_headers = {"Authorization": "Bearer <credentials secret>"}
        mock_request_get.assert_called_once_with(
            "<pairing base URL>/v1/<realm name>/devices/<device id>",
            headers=expected_headers,
            verify=False,
            timeout=30,
        )
