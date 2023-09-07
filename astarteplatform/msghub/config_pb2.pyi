from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class ConfigMessage(_message.Message):
    __slots__ = ["realm", "device_id", "credentials_secret", "pairing_url", "pairing_token", "grpc_socket_port"]
    REALM_FIELD_NUMBER: _ClassVar[int]
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    CREDENTIALS_SECRET_FIELD_NUMBER: _ClassVar[int]
    PAIRING_URL_FIELD_NUMBER: _ClassVar[int]
    PAIRING_TOKEN_FIELD_NUMBER: _ClassVar[int]
    GRPC_SOCKET_PORT_FIELD_NUMBER: _ClassVar[int]
    realm: str
    device_id: str
    credentials_secret: str
    pairing_url: str
    pairing_token: str
    grpc_socket_port: int
    def __init__(self, realm: _Optional[str] = ..., device_id: _Optional[str] = ..., credentials_secret: _Optional[str] = ..., pairing_url: _Optional[str] = ..., pairing_token: _Optional[str] = ..., grpc_socket_port: _Optional[int] = ...) -> None: ...
