from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class AstarteDoubleArray(_message.Message):
    __slots__ = ["values"]
    VALUES_FIELD_NUMBER: _ClassVar[int]
    values: _containers.RepeatedScalarFieldContainer[float]
    def __init__(self, values: _Optional[_Iterable[float]] = ...) -> None: ...

class AstarteIntegerArray(_message.Message):
    __slots__ = ["values"]
    VALUES_FIELD_NUMBER: _ClassVar[int]
    values: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, values: _Optional[_Iterable[int]] = ...) -> None: ...

class AstarteBooleanArray(_message.Message):
    __slots__ = ["values"]
    VALUES_FIELD_NUMBER: _ClassVar[int]
    values: _containers.RepeatedScalarFieldContainer[bool]
    def __init__(self, values: _Optional[_Iterable[bool]] = ...) -> None: ...

class AstarteLongIntegerArray(_message.Message):
    __slots__ = ["values"]
    VALUES_FIELD_NUMBER: _ClassVar[int]
    values: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, values: _Optional[_Iterable[int]] = ...) -> None: ...

class AstarteStringArray(_message.Message):
    __slots__ = ["values"]
    VALUES_FIELD_NUMBER: _ClassVar[int]
    values: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, values: _Optional[_Iterable[str]] = ...) -> None: ...

class AstarteBinaryBlobArray(_message.Message):
    __slots__ = ["values"]
    VALUES_FIELD_NUMBER: _ClassVar[int]
    values: _containers.RepeatedScalarFieldContainer[bytes]
    def __init__(self, values: _Optional[_Iterable[bytes]] = ...) -> None: ...

class AstarteDateTimeArray(_message.Message):
    __slots__ = ["values"]
    VALUES_FIELD_NUMBER: _ClassVar[int]
    values: _containers.RepeatedCompositeFieldContainer[_timestamp_pb2.Timestamp]
    def __init__(self, values: _Optional[_Iterable[_Union[_timestamp_pb2.Timestamp, _Mapping]]] = ...) -> None: ...

class AstarteDataTypeObject(_message.Message):
    __slots__ = ["object_data"]
    class ObjectDataEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: AstarteDataTypeIndividual
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[AstarteDataTypeIndividual, _Mapping]] = ...) -> None: ...
    OBJECT_DATA_FIELD_NUMBER: _ClassVar[int]
    object_data: _containers.MessageMap[str, AstarteDataTypeIndividual]
    def __init__(self, object_data: _Optional[_Mapping[str, AstarteDataTypeIndividual]] = ...) -> None: ...

class AstarteDataTypeIndividual(_message.Message):
    __slots__ = ["astarte_double", "astarte_integer", "astarte_boolean", "astarte_long_integer", "astarte_string", "astarte_binary_blob", "astarte_date_time", "astarte_double_array", "astarte_integer_array", "astarte_boolean_array", "astarte_long_integer_array", "astarte_string_array", "astarte_binary_blob_array", "astarte_date_time_array"]
    ASTARTE_DOUBLE_FIELD_NUMBER: _ClassVar[int]
    ASTARTE_INTEGER_FIELD_NUMBER: _ClassVar[int]
    ASTARTE_BOOLEAN_FIELD_NUMBER: _ClassVar[int]
    ASTARTE_LONG_INTEGER_FIELD_NUMBER: _ClassVar[int]
    ASTARTE_STRING_FIELD_NUMBER: _ClassVar[int]
    ASTARTE_BINARY_BLOB_FIELD_NUMBER: _ClassVar[int]
    ASTARTE_DATE_TIME_FIELD_NUMBER: _ClassVar[int]
    ASTARTE_DOUBLE_ARRAY_FIELD_NUMBER: _ClassVar[int]
    ASTARTE_INTEGER_ARRAY_FIELD_NUMBER: _ClassVar[int]
    ASTARTE_BOOLEAN_ARRAY_FIELD_NUMBER: _ClassVar[int]
    ASTARTE_LONG_INTEGER_ARRAY_FIELD_NUMBER: _ClassVar[int]
    ASTARTE_STRING_ARRAY_FIELD_NUMBER: _ClassVar[int]
    ASTARTE_BINARY_BLOB_ARRAY_FIELD_NUMBER: _ClassVar[int]
    ASTARTE_DATE_TIME_ARRAY_FIELD_NUMBER: _ClassVar[int]
    astarte_double: float
    astarte_integer: int
    astarte_boolean: bool
    astarte_long_integer: int
    astarte_string: str
    astarte_binary_blob: bytes
    astarte_date_time: _timestamp_pb2.Timestamp
    astarte_double_array: AstarteDoubleArray
    astarte_integer_array: AstarteIntegerArray
    astarte_boolean_array: AstarteBooleanArray
    astarte_long_integer_array: AstarteLongIntegerArray
    astarte_string_array: AstarteStringArray
    astarte_binary_blob_array: AstarteBinaryBlobArray
    astarte_date_time_array: AstarteDateTimeArray
    def __init__(self, astarte_double: _Optional[float] = ..., astarte_integer: _Optional[int] = ..., astarte_boolean: bool = ..., astarte_long_integer: _Optional[int] = ..., astarte_string: _Optional[str] = ..., astarte_binary_blob: _Optional[bytes] = ..., astarte_date_time: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., astarte_double_array: _Optional[_Union[AstarteDoubleArray, _Mapping]] = ..., astarte_integer_array: _Optional[_Union[AstarteIntegerArray, _Mapping]] = ..., astarte_boolean_array: _Optional[_Union[AstarteBooleanArray, _Mapping]] = ..., astarte_long_integer_array: _Optional[_Union[AstarteLongIntegerArray, _Mapping]] = ..., astarte_string_array: _Optional[_Union[AstarteStringArray, _Mapping]] = ..., astarte_binary_blob_array: _Optional[_Union[AstarteBinaryBlobArray, _Mapping]] = ..., astarte_date_time_array: _Optional[_Union[AstarteDateTimeArray, _Mapping]] = ...) -> None: ...

class AstarteDataType(_message.Message):
    __slots__ = ["astarte_individual", "astarte_object"]
    ASTARTE_INDIVIDUAL_FIELD_NUMBER: _ClassVar[int]
    ASTARTE_OBJECT_FIELD_NUMBER: _ClassVar[int]
    astarte_individual: AstarteDataTypeIndividual
    astarte_object: AstarteDataTypeObject
    def __init__(self, astarte_individual: _Optional[_Union[AstarteDataTypeIndividual, _Mapping]] = ..., astarte_object: _Optional[_Union[AstarteDataTypeObject, _Mapping]] = ...) -> None: ...
