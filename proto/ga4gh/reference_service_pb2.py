# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: proto/ga4gh/reference_service.proto

import sys
_b=sys.version_info[0]<3 and (lambda x:x) or (lambda x:x.encode('latin1'))
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
from google.protobuf import descriptor_pb2
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from proto.ga4gh import references_pb2 as proto_dot_ga4gh_dot_references__pb2


DESCRIPTOR = _descriptor.FileDescriptor(
  name='proto/ga4gh/reference_service.proto',
  package='ga4gh',
  syntax='proto3',
  serialized_pb=_b('\n#proto/ga4gh/reference_service.proto\x12\x05ga4gh\x1a\x1cproto/ga4gh/references.proto\"\x80\x01\n\x1aSearchReferenceSetsRequest\x12\x13\n\x0bmd5checksum\x18\x01 \x01(\t\x12\x11\n\taccession\x18\x02 \x01(\t\x12\x13\n\x0b\x61ssembly_id\x18\x03 \x01(\t\x12\x11\n\tpage_size\x18\x04 \x01(\x05\x12\x12\n\npage_token\x18\x05 \x01(\t\"c\n\x1bSearchReferenceSetsResponse\x12+\n\x0ereference_sets\x18\x01 \x03(\x0b\x32\x13.ga4gh.ReferenceSet\x12\x17\n\x0fnext_page_token\x18\x02 \x01(\t\"2\n\x16GetReferenceSetRequest\x12\x18\n\x10reference_set_id\x18\x01 \x01(\t\"\x82\x01\n\x17SearchReferencesRequest\x12\x18\n\x10reference_set_id\x18\x01 \x01(\t\x12\x13\n\x0bmd5checksum\x18\x02 \x01(\t\x12\x11\n\taccession\x18\x03 \x01(\t\x12\x11\n\tpage_size\x18\x04 \x01(\x05\x12\x12\n\npage_token\x18\x05 \x01(\t\"Y\n\x18SearchReferencesResponse\x12$\n\nreferences\x18\x01 \x03(\x0b\x32\x10.ga4gh.Reference\x12\x17\n\x0fnext_page_token\x18\x02 \x01(\t\"+\n\x13GetReferenceRequest\x12\x14\n\x0creference_id\x18\x01 \x01(\t\"a\n\x19ListReferenceBasesRequest\x12\x14\n\x0creference_id\x18\x01 \x01(\t\x12\r\n\x05start\x18\x02 \x01(\x03\x12\x0b\n\x03\x65nd\x18\x03 \x01(\x03\x12\x12\n\npage_token\x18\x04 \x01(\t\"W\n\x1aListReferenceBasesResponse\x12\x0e\n\x06offset\x18\x01 \x01(\x03\x12\x10\n\x08sequence\x18\x02 \x01(\t\x12\x17\n\x0fnext_page_token\x18\x03 \x01(\t2\xa5\x03\n\x10ReferenceService\x12\\\n\x13SearchReferenceSets\x12!.ga4gh.SearchReferenceSetsRequest\x1a\".ga4gh.SearchReferenceSetsResponse\x12\x45\n\x0fGetReferenceSet\x12\x1d.ga4gh.GetReferenceSetRequest\x1a\x13.ga4gh.ReferenceSet\x12S\n\x10SearchReferences\x12\x1e.ga4gh.SearchReferencesRequest\x1a\x1f.ga4gh.SearchReferencesResponse\x12<\n\x0cGetReference\x12\x1a.ga4gh.GetReferenceRequest\x1a\x10.ga4gh.Reference\x12Y\n\x12ListReferenceBases\x12 .ga4gh.ListReferenceBasesRequest\x1a!.ga4gh.ListReferenceBasesResponseb\x06proto3')
  ,
  dependencies=[proto_dot_ga4gh_dot_references__pb2.DESCRIPTOR,])
_sym_db.RegisterFileDescriptor(DESCRIPTOR)




_SEARCHREFERENCESETSREQUEST = _descriptor.Descriptor(
  name='SearchReferenceSetsRequest',
  full_name='ga4gh.SearchReferenceSetsRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='md5checksum', full_name='ga4gh.SearchReferenceSetsRequest.md5checksum', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='accession', full_name='ga4gh.SearchReferenceSetsRequest.accession', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='assembly_id', full_name='ga4gh.SearchReferenceSetsRequest.assembly_id', index=2,
      number=3, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='page_size', full_name='ga4gh.SearchReferenceSetsRequest.page_size', index=3,
      number=4, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='page_token', full_name='ga4gh.SearchReferenceSetsRequest.page_token', index=4,
      number=5, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=77,
  serialized_end=205,
)


_SEARCHREFERENCESETSRESPONSE = _descriptor.Descriptor(
  name='SearchReferenceSetsResponse',
  full_name='ga4gh.SearchReferenceSetsResponse',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='reference_sets', full_name='ga4gh.SearchReferenceSetsResponse.reference_sets', index=0,
      number=1, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='next_page_token', full_name='ga4gh.SearchReferenceSetsResponse.next_page_token', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=207,
  serialized_end=306,
)


_GETREFERENCESETREQUEST = _descriptor.Descriptor(
  name='GetReferenceSetRequest',
  full_name='ga4gh.GetReferenceSetRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='reference_set_id', full_name='ga4gh.GetReferenceSetRequest.reference_set_id', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=308,
  serialized_end=358,
)


_SEARCHREFERENCESREQUEST = _descriptor.Descriptor(
  name='SearchReferencesRequest',
  full_name='ga4gh.SearchReferencesRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='reference_set_id', full_name='ga4gh.SearchReferencesRequest.reference_set_id', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='md5checksum', full_name='ga4gh.SearchReferencesRequest.md5checksum', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='accession', full_name='ga4gh.SearchReferencesRequest.accession', index=2,
      number=3, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='page_size', full_name='ga4gh.SearchReferencesRequest.page_size', index=3,
      number=4, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='page_token', full_name='ga4gh.SearchReferencesRequest.page_token', index=4,
      number=5, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=361,
  serialized_end=491,
)


_SEARCHREFERENCESRESPONSE = _descriptor.Descriptor(
  name='SearchReferencesResponse',
  full_name='ga4gh.SearchReferencesResponse',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='references', full_name='ga4gh.SearchReferencesResponse.references', index=0,
      number=1, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='next_page_token', full_name='ga4gh.SearchReferencesResponse.next_page_token', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=493,
  serialized_end=582,
)


_GETREFERENCEREQUEST = _descriptor.Descriptor(
  name='GetReferenceRequest',
  full_name='ga4gh.GetReferenceRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='reference_id', full_name='ga4gh.GetReferenceRequest.reference_id', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=584,
  serialized_end=627,
)


_LISTREFERENCEBASESREQUEST = _descriptor.Descriptor(
  name='ListReferenceBasesRequest',
  full_name='ga4gh.ListReferenceBasesRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='reference_id', full_name='ga4gh.ListReferenceBasesRequest.reference_id', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='start', full_name='ga4gh.ListReferenceBasesRequest.start', index=1,
      number=2, type=3, cpp_type=2, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='end', full_name='ga4gh.ListReferenceBasesRequest.end', index=2,
      number=3, type=3, cpp_type=2, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='page_token', full_name='ga4gh.ListReferenceBasesRequest.page_token', index=3,
      number=4, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=629,
  serialized_end=726,
)


_LISTREFERENCEBASESRESPONSE = _descriptor.Descriptor(
  name='ListReferenceBasesResponse',
  full_name='ga4gh.ListReferenceBasesResponse',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='offset', full_name='ga4gh.ListReferenceBasesResponse.offset', index=0,
      number=1, type=3, cpp_type=2, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='sequence', full_name='ga4gh.ListReferenceBasesResponse.sequence', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='next_page_token', full_name='ga4gh.ListReferenceBasesResponse.next_page_token', index=2,
      number=3, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=728,
  serialized_end=815,
)

_SEARCHREFERENCESETSRESPONSE.fields_by_name['reference_sets'].message_type = proto_dot_ga4gh_dot_references__pb2._REFERENCESET
_SEARCHREFERENCESRESPONSE.fields_by_name['references'].message_type = proto_dot_ga4gh_dot_references__pb2._REFERENCE
DESCRIPTOR.message_types_by_name['SearchReferenceSetsRequest'] = _SEARCHREFERENCESETSREQUEST
DESCRIPTOR.message_types_by_name['SearchReferenceSetsResponse'] = _SEARCHREFERENCESETSRESPONSE
DESCRIPTOR.message_types_by_name['GetReferenceSetRequest'] = _GETREFERENCESETREQUEST
DESCRIPTOR.message_types_by_name['SearchReferencesRequest'] = _SEARCHREFERENCESREQUEST
DESCRIPTOR.message_types_by_name['SearchReferencesResponse'] = _SEARCHREFERENCESRESPONSE
DESCRIPTOR.message_types_by_name['GetReferenceRequest'] = _GETREFERENCEREQUEST
DESCRIPTOR.message_types_by_name['ListReferenceBasesRequest'] = _LISTREFERENCEBASESREQUEST
DESCRIPTOR.message_types_by_name['ListReferenceBasesResponse'] = _LISTREFERENCEBASESRESPONSE

SearchReferenceSetsRequest = _reflection.GeneratedProtocolMessageType('SearchReferenceSetsRequest', (_message.Message,), dict(
  DESCRIPTOR = _SEARCHREFERENCESETSREQUEST,
  __module__ = 'proto.ga4gh.reference_service_pb2'
  # @@protoc_insertion_point(class_scope:ga4gh.SearchReferenceSetsRequest)
  ))
_sym_db.RegisterMessage(SearchReferenceSetsRequest)

SearchReferenceSetsResponse = _reflection.GeneratedProtocolMessageType('SearchReferenceSetsResponse', (_message.Message,), dict(
  DESCRIPTOR = _SEARCHREFERENCESETSRESPONSE,
  __module__ = 'proto.ga4gh.reference_service_pb2'
  # @@protoc_insertion_point(class_scope:ga4gh.SearchReferenceSetsResponse)
  ))
_sym_db.RegisterMessage(SearchReferenceSetsResponse)

GetReferenceSetRequest = _reflection.GeneratedProtocolMessageType('GetReferenceSetRequest', (_message.Message,), dict(
  DESCRIPTOR = _GETREFERENCESETREQUEST,
  __module__ = 'proto.ga4gh.reference_service_pb2'
  # @@protoc_insertion_point(class_scope:ga4gh.GetReferenceSetRequest)
  ))
_sym_db.RegisterMessage(GetReferenceSetRequest)

SearchReferencesRequest = _reflection.GeneratedProtocolMessageType('SearchReferencesRequest', (_message.Message,), dict(
  DESCRIPTOR = _SEARCHREFERENCESREQUEST,
  __module__ = 'proto.ga4gh.reference_service_pb2'
  # @@protoc_insertion_point(class_scope:ga4gh.SearchReferencesRequest)
  ))
_sym_db.RegisterMessage(SearchReferencesRequest)

SearchReferencesResponse = _reflection.GeneratedProtocolMessageType('SearchReferencesResponse', (_message.Message,), dict(
  DESCRIPTOR = _SEARCHREFERENCESRESPONSE,
  __module__ = 'proto.ga4gh.reference_service_pb2'
  # @@protoc_insertion_point(class_scope:ga4gh.SearchReferencesResponse)
  ))
_sym_db.RegisterMessage(SearchReferencesResponse)

GetReferenceRequest = _reflection.GeneratedProtocolMessageType('GetReferenceRequest', (_message.Message,), dict(
  DESCRIPTOR = _GETREFERENCEREQUEST,
  __module__ = 'proto.ga4gh.reference_service_pb2'
  # @@protoc_insertion_point(class_scope:ga4gh.GetReferenceRequest)
  ))
_sym_db.RegisterMessage(GetReferenceRequest)

ListReferenceBasesRequest = _reflection.GeneratedProtocolMessageType('ListReferenceBasesRequest', (_message.Message,), dict(
  DESCRIPTOR = _LISTREFERENCEBASESREQUEST,
  __module__ = 'proto.ga4gh.reference_service_pb2'
  # @@protoc_insertion_point(class_scope:ga4gh.ListReferenceBasesRequest)
  ))
_sym_db.RegisterMessage(ListReferenceBasesRequest)

ListReferenceBasesResponse = _reflection.GeneratedProtocolMessageType('ListReferenceBasesResponse', (_message.Message,), dict(
  DESCRIPTOR = _LISTREFERENCEBASESRESPONSE,
  __module__ = 'proto.ga4gh.reference_service_pb2'
  # @@protoc_insertion_point(class_scope:ga4gh.ListReferenceBasesResponse)
  ))
_sym_db.RegisterMessage(ListReferenceBasesResponse)


# @@protoc_insertion_point(module_scope)
