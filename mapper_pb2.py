# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: mapper.proto
# Protobuf Python Version: 4.25.0
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x0cmapper.proto\x12\x06mapper\"\x1d\n\x05Point\x12\t\n\x01x\x18\x01 \x01(\x02\x12\t\n\x01y\x18\x02 \x01(\x02\"\x81\x01\n\nMapRequest\x12\x13\n\x0bstart_index\x18\x01 \x01(\x05\x12\x11\n\tend_index\x18\x02 \x01(\x05\x12 \n\tcentroids\x18\x03 \x03(\x0b\x32\r.mapper.Point\x12\x13\n\x0bnum_mappers\x18\x04 \x01(\x05\x12\x14\n\x0cnum_reducers\x18\x05 \x01(\x05\"\x1e\n\x0bMapResponse\x12\x0f\n\x07success\x18\x01 \x01(\x08\"6\n\x08KeyValue\x12\x0b\n\x03key\x18\x01 \x01(\x05\x12\x1d\n\x06values\x18\x02 \x03(\x0b\x32\r.mapper.Point\"\x0f\n\rReduceRequest\"N\n\x0eReduceResponse\x12\x0f\n\x07success\x18\x01 \x01(\x08\x12+\n\x11updated_centroids\x18\x02 \x03(\x0b\x32\x10.mapper.KeyValue\"9\n\x10PartitionRequest\x12\x12\n\nreducer_id\x18\x01 \x01(\x05\x12\x11\n\tmapper_id\x18\x02 \x01(\x05\"3\n\x11PartitionResponse\x12\x1e\n\x04\x64\x61ta\x18\x01 \x03(\x0b\x32\x10.mapper.KeyValue2\x85\x01\n\x06Mapper\x12\x36\n\x0bProcessData\x12\x12.mapper.MapRequest\x1a\x13.mapper.MapResponse\x12\x43\n\x0cGetPartition\x12\x18.mapper.PartitionRequest\x1a\x19.mapper.PartitionResponse2F\n\x07Reducer\x12;\n\nReduceData\x12\x15.mapper.ReduceRequest\x1a\x16.mapper.ReduceResponseb\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'mapper_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
  DESCRIPTOR._options = None
  _globals['_POINT']._serialized_start=24
  _globals['_POINT']._serialized_end=53
  _globals['_MAPREQUEST']._serialized_start=56
  _globals['_MAPREQUEST']._serialized_end=185
  _globals['_MAPRESPONSE']._serialized_start=187
  _globals['_MAPRESPONSE']._serialized_end=217
  _globals['_KEYVALUE']._serialized_start=219
  _globals['_KEYVALUE']._serialized_end=273
  _globals['_REDUCEREQUEST']._serialized_start=275
  _globals['_REDUCEREQUEST']._serialized_end=290
  _globals['_REDUCERESPONSE']._serialized_start=292
  _globals['_REDUCERESPONSE']._serialized_end=370
  _globals['_PARTITIONREQUEST']._serialized_start=372
  _globals['_PARTITIONREQUEST']._serialized_end=429
  _globals['_PARTITIONRESPONSE']._serialized_start=431
  _globals['_PARTITIONRESPONSE']._serialized_end=482
  _globals['_MAPPER']._serialized_start=485
  _globals['_MAPPER']._serialized_end=618
  _globals['_REDUCER']._serialized_start=620
  _globals['_REDUCER']._serialized_end=690
# @@protoc_insertion_point(module_scope)
