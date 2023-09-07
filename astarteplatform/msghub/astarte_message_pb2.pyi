from google.protobuf import timestamp_pb2 as _timestamp_pb2
from astarteplatform.msghub import astarte_type_pb2 as _astarte_type_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class AstarteMessage(_message.Message):
    __slots__ = ["interface_name", "path", "astarte_data", "astarte_unset", "timestamp"]
    INTERFACE_NAME_FIELD_NUMBER: _ClassVar[int]
    PATH_FIELD_NUMBER: _ClassVar[int]
    ASTARTE_DATA_FIELD_NUMBER: _ClassVar[int]
    ASTARTE_UNSET_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    interface_name: str
    path: str
    astarte_data: _astarte_type_pb2.AstarteDataType
    astarte_unset: AstarteUnset
    timestamp: _timestamp_pb2.Timestamp
    def __init__(self, interface_name: _Optional[str] = ..., path: _Optional[str] = ..., astarte_data: _Optional[_Union[_astarte_type_pb2.AstarteDataType, _Mapping]] = ..., astarte_unset: _Optional[_Union[AstarteUnset, _Mapping]] = ..., timestamp: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class AstarteUnset(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...
