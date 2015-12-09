"""
Definitions of the GA4GH protocol types.
"""
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import datetime
import json

import google.protobuf.json_format as json_format
from google.protobuf import reflection as reflection
import google.protobuf.message as message

import ga4gh.pb as pb

from proto import version
from proto.ga4gh.common_pb2 import *
from proto.ga4gh.metadata_pb2 import *
from proto.ga4gh.read_service_pb2 import *
from proto.ga4gh.reads_pb2 import *
from proto.ga4gh.reference_service_pb2 import *
from proto.ga4gh.references_pb2 import *
from proto.ga4gh.variant_service_pb2 import *
from proto.ga4gh.variants_pb2 import *
from proto.google.protobuf.struct_pb2 import Value

# A map of response objects to the name of the attribute used to
# store the values returned.
_valueListNameMap = {
    SearchVariantSetsResponse: "variant_sets",
    SearchVariantsResponse: "variants",
    SearchDatasetsResponse: "datasets",
    SearchReferenceSetsResponse: "reference_sets",
    SearchReferencesResponse: "references",
    SearchReadGroupSetsResponse: "read_group_sets",
    SearchReadsResponse: "alignments",
    SearchCallSetsResponse: "call_sets",
}


def getValueListName(protocolResponseClass):
    """
    Returns the name of the attribute in the specified protocol class
    that is used to hold the values in a search response.
    """
    return _valueListNameMap[protocolResponseClass]


def convertDatetime(t):
    """
    Converts the specified datetime object into its appropriate protocol
    value. This is the number of milliseconds from the epoch.
    """
    epoch = datetime.datetime.utcfromtimestamp(0)
    delta = t - epoch
    millis = delta.total_seconds() * 1000
    return int(millis)

def getValueFromValue(value):
    if type(value) != Value:
        raise Exception, type(value)
    if value.WhichOneof("kind") == None:
        raise Exception, "Nothing set for %s" % value
    return getattr(value, value.WhichOneof("kind"))

def mergeLists(lists):
    return reduce(list.__add__, lists)

def getDictFromMessageMap(messageMap):
    return dict([(k, [getValueFromValue(x) for x in v.values]) for (k,v) in messageMap._values.items()])

def toJson(protoObject, indent = 2):
    """
    Serialises a proto-buf object as json
    """
    # Using the internal method because this way we can reformat the JSON
    js = json_format._MessageToJsonObject(protoObject, False)
    return json.dumps(js, indent = indent)

def fromJsonDict(jsonDict, protoClass):
    return fromJson(json.dumps(jsonDict), protoClass)

def fromJson(json, protoClass):
    """
    Deserialise json into an instance of proto-buf class
    """
    return json_format.Parse(json, protoClass())

def toJsonDict(protoObject):
    return json.loads(toJson(protoObject))

def validate(json, protoClass):
    try:
        fromJson(json, protoClass) # The json conversion automatically validates
        return True
    except Exception, e:
        return False

class SearchResponseBuilder(object):
    """
    A class to allow sequential building of SearchResponse objects.
    """
    def __init__(self, responseClass, pageSize, maxResponseLength):
        """
        Allocates a new SearchResponseBuilder for the specified
        responseClass, user-requested pageSize and the system mandated
        maxResponseLength (in bytes). The maxResponseLength is an
        approximate limit on the overall length of the serialised
        response.
        """
        self._responseClass = responseClass
        self._pageSize = pageSize
        self._maxResponseLength = maxResponseLength
        self._numElements = 0
        self._nextPageToken = None
        self._protoObject = responseClass()
        self._valueListName = getValueListName(responseClass)

    def getPageSize(self):
        """
        Returns the page size for this SearchResponseBuilder. This is the
        user-requested maximum size for the number of elements in the
        value list.
        """
        return self._pageSize

    def getMaxResponseLength(self):
        """
        Returns the approximate maximum response length. More precisely,
        this is the total length (in bytes) of the concatenated JSON
        representations of the values in the value list after which
        we consider the buffer to be full.
        """
        return self._maxResponseLength

    def getNextPageToken(self):
        """
        Returns the value of the nextPageToken for this
        SearchResponseBuilder.
        """
        return self._nextPageToken

    def setNextPageToken(self, nextPageToken):
        """
        Sets the nextPageToken to the specified value.
        """
        self._nextPageToken = nextPageToken

    def addValue(self, protocolElement):
        """
        Appends the specified protocolElement to the value list for this
        response.
        """
        self._numElements += 1
        attr = getattr(self._protoObject, self._valueListName)
        obj = attr.add()
        obj.CopyFrom(protocolElement)

    def isFull(self):
        """
        Returns True if the response buffer is full, and False otherwise.
        The buffer is full if either (1) the number of items in the value
        list is >= pageSize or (2) the total length of the serialised
        elements in the page is >= maxResponseLength.

        If page_size or max_response_length were not set in the request
        then they're not checked.
        """
        return (
            (self._pageSize > 0 and self._numElements >= self._pageSize) or
            (self._maxResponseLength > 0 and
             self._protoObject.ByteSize() >= self._maxResponseLength)
        )

    def getSerializedResponse(self):
        """
        Returns a string version of the SearchResponse that has
        been built by this SearchResponseBuilder.
        """
        self._protoObject.next_page_token = pb.string(self._nextPageToken)
        s = toJson(self._protoObject)
        return s

def getProtocolClasses(superclass=message.Message):
    """
    Returns all the protocol classes that are subclasses of the
    specified superclass. Only 'leaf' classes are returned,
    corresponding directly to the classes defined in the protocol.
    """
    # We keep a manual list of the superclasses that we define here
    # so we can filter them out when we're getting the protocol
    # classes.
    superclasses = set([
        message.Message])
    thisModule = sys.modules[__name__]
    subclasses = []
    for name, class_ in inspect.getmembers(thisModule):
        if ((inspect.isclass(class_) and
                issubclass(class_, superclass) and
                class_ not in superclasses)):
            subclasses.append(class_)
    return subclasses
