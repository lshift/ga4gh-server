"""
Provides additional functionality based on Protocol Buffers
"""
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import random
import string

import ga4gh.protocol as protocol

import google.protobuf.descriptor as descriptor
import google.protobuf.json_format as json_format

INT_MIN_VALUE = -(1 << 31)
INT_MAX_VALUE = (1 << 31) - 1
LONG_MIN_VALUE = -(1 << 63)
LONG_MAX_VALUE = (1 << 63) - 1


class PrototoolsException(Exception):
    """
    Something that went wrong in the Prototools classes
    """


class ProtoTool(object):
    """
    Base class for ProtoTool classes
    """
    def __init__(self, class_):
        self.class_ = class_
        self.assertProtocolSubclass()
        self.schema = class_.DESCRIPTOR

    def assertProtocolSubclass(self):
        if not issubclass(self.class_, protocol.message.Message):
            message = "class '{}' is not subclass of Message".format(
                self.class_.__name__)
            raise PrototoolsException(message)


class Creator(ProtoTool):
    """
    Provides methods for creating instances of protocol classes
    according to different rules
    """
    def __init__(self, class_):
        self.class_ = class_
        self.assertProtocolSubclass()
        self.schema = class_.DESCRIPTOR

    def getTypicalInstance(self):
        """
        Return a typical instance of the class
        """
        return self._getInstance(TypicalInstanceCreator)

    def getRandomInstance(self):
        """
        Return a random instance of the class
        """
        return self._getInstance(RandomInstanceCreator)

    def getInvalidInstance(self):
        """
        Return an invalid instance of the class
        """
        return self._getInstance(InvalidInstanceCreator)

    def getDefaultInstance(self):
        """
        Return an instance of the class with required values set
        """
        return self._getInstance(DefaultInstanceCreator)

    def _getInstance(self, creatorClass):
        creator = creatorClass(self.class_)
        instance = creator.getInstance()
        return instance

    def getInvalidField(self, fieldName):
        """
        Return an invalid value of the attribute with name fieldName
        """
        return self._getField(InvalidInstanceCreator, fieldName)

    def _getField(self, creatorClass, fieldName):
        creator = creatorClass(self.class_)
        value = creator.getFieldValue(fieldName)
        return value


class ProtoTypeSwitch(object):
    """
    Provides reusable logic for branching based on Proto types
    """

    def __init__(self, class_):
        self.class_ = class_
        self.schema = class_.DESCRIPTOR

    def getInstance(self):
        """
        Return an instance of the class instantiated according to
        the rules defined by the subclass
        """
        jsonDict = self.handleSchema(self.schema)
        instance = protocol.fromJsonDict(jsonDict, self.class_)
        return instance

    def getFieldValue(self, fieldName):
        """
        Return the value of a field of the class instantiated according
        to the rules defined by the subclass
        """
        for field in self.schema.fields:
            if field.name == fieldName:
                break
        value = self.handleField(field)
        return value

    def handleSchema(self, schema, extra=None):
        """
        Return a jsonDict representation of an instance of an object
        defined by schema instantiated according to the rules defined by the
        subclass
        """
        class_switch = {
            descriptor.Descriptor: self.handleMessage,
            descriptor.FieldDescriptor: self.handleField,
        }

        handler = class_switch[schema.__class__]
        return handler(schema)

    def handleField(self, fieldDescriptor):
        field_switch = {
            'null': self.handleNull,
            descriptor.FieldDescriptor.TYPE_BOOL: self.handleBoolean,
            descriptor.FieldDescriptor.TYPE_STRING: self.handleString,
            'bytes': self.handleBytes,
            descriptor.FieldDescriptor.TYPE_INT64: self.handleLong,
            descriptor.FieldDescriptor.TYPE_INT32: self.handleInt,
            descriptor.FieldDescriptor.TYPE_FLOAT: self.handleFloat,
            descriptor.FieldDescriptor.TYPE_DOUBLE: self.handleDouble,
            'fixed': self.handleFixed,
            descriptor.FieldDescriptor.TYPE_ENUM: self.handleEnum,
            'union': self.handleUnion,
            'error_union': self.handleErrorUnion,
            'error': self.handleError,
            descriptor.FieldDescriptor.TYPE_MESSAGE: self.handleMessage,
        }

        handler = field_switch[fieldDescriptor.type]

        if json_format._IsMapEntry(fieldDescriptor):
            return self._runField(fieldDescriptor, handler)
        elif fieldDescriptor.label == \
                descriptor.FieldDescriptor.LABEL_REPEATED:
            return [self._runField(fieldDescriptor, handler) for _ in range(2)]
        else:
            return self._runField(fieldDescriptor, handler)

    def _runField(self, fieldDescriptor, handler):
        if json_format._IsMapEntry(fieldDescriptor):
            return {
                self.handleField(fieldDescriptor.message_type.fields[0]):
                self.handleField(fieldDescriptor.message_type.fields[1])}
        elif fieldDescriptor.type == descriptor.FieldDescriptor.TYPE_MESSAGE:
            return handler(fieldDescriptor.message_type)
        elif fieldDescriptor.type == descriptor.FieldDescriptor.TYPE_ENUM:
            return handler(fieldDescriptor.enum_type)
        else:
            return handler()

    # the below methods just delegate to other methods that the subclasses
    # implement; there's no need to re-implement them in (most) subclasses

    def handleErrorUnion(self, schema):
        return self.handleUnion(schema)

    def handleError(self, schema):
        return self.handleRecord(schema)

    def handleRequest(self, schema):
        return self.handleRecord(schema)

    @staticmethod
    def toCamelCase(snakeStr):
        components = snakeStr.split('_')
        return components[0] + "".join(x.title() for x in components[1:])

    @staticmethod
    def toSnakeCase(camelStr):
        components = []
        current = ""
        for c in camelStr:
            if (not c.isnumeric()) and c.upper() == c:  # new word
                components.append(current.lower())
                current = c
            else:
                current += c
        components.append(current.lower())
        return "_".join(components)

    def handleMessage(self, schema):
        message = {}
        if schema.full_name == "google.protobuf.Value":  # avoid nesting!
            message["numberValue"] = self.handleDouble()
        else:
            for field in schema.fields:
                # Protobuf JSON import assumes camel case fields, which then
                # get converted back to snake case...
                name = self.toCamelCase(field.name)
                message[name] = self.handleSchema(field)
        return message


class RandomInstanceCreator(ProtoTypeSwitch):
    """
    Generates random instances and values
    """
    def handleNull(self):
        return None

    def handleBoolean(self):
        return random.choice([True, False])

    def handleString(self, length=10, characters=string.printable):
        return ''.join(
            [random.choice(characters) for _ in range(length)])

    def handleBytes(self):
        return self.handleString()

    def handleInt(self, min_=INT_MIN_VALUE, max_=INT_MAX_VALUE):
        return random.randint(min_, max_)

    def handleLong(self, min_=LONG_MIN_VALUE, max_=LONG_MAX_VALUE):
        return random.randint(min_, max_)

    def handleFloat(self, min_=INT_MIN_VALUE, max_=INT_MAX_VALUE):
        return random.uniform(min_, max_)

    def handleDouble(self, min_=LONG_MIN_VALUE, max_=LONG_MAX_VALUE):
        return random.uniform(min_, max_)

    def handleFixed(self, schema):
        return self.handleString(schema.size)

    def handleEnum(self, schema):
        return random.choice(schema.values).name

    def handleArray(self, schema, length=10):
        return [self.handleSchema(schema.items) for _ in range(length)]

    def handleMap(self, schema, size=10):
        return dict(
            (self.handleString(), self.handleSchema(schema.values))
            for _ in range(size))

    def handleUnion(self, schema):
        chosenSchema = random.choice(schema.schemas)
        return self.handleSchema(chosenSchema)

    def handleRecord(self, schema):
        return dict(
            (field.name, self.handleSchema(field.type))
            for field in schema.fields)


class TypicalInstanceCreator(ProtoTypeSwitch):
    """
    Generates typical instances and values
    """
    def handleNull(self):
        raise PrototoolsException()

    def handleBoolean(self):
        return True

    def handleString(self):
        return 'aString'

    def handleBytes(self):
        return b'someBytes'

    def handleInt(self):
        return 5

    def handleLong(self):
        return 6

    def handleFloat(self):
        return 7.0

    def handleDouble(self):
        return 8.0

    def handleFixed(self, schema):
        return 'x' * schema.size

    def handleEnum(self, schema):
        return schema.values[0].name

    def handleArray(self, schema):
        return [self.handleSchema(schema.items) for _ in range(2)]

    def handleMap(self, schema):
        return {'key': self.handleSchema(schema.values)}

    def handleUnion(self, schema):
        # return an instance of the first non-null schema in the union
        for memberSchema in schema.schemas:
            if memberSchema.type != 'null':
                return self.handleSchema(memberSchema)
        raise PrototoolsException()  # should never happen


class InvalidInstanceCreator(ProtoTypeSwitch):
    """
    Generates invalid instances and values
    """
    def handleNull(self):
        return 1

    def handleBoolean(self):
        return "invalidBoolean"

    def handleString(self):
        return 2

    def handleBytes(self):
        return 3

    def handleInt(self):
        return "invalidInt"

    def handleLong(self):
        return "invalidLong"

    def handleFloat(self):
        return "invalidFloat"

    def handleDouble(self):
        return "invalidDouble"

    def handleFixed(self, schema):
        return 4

    def handleEnum(self, schema):
        return "invalidEnum"

    def handleArray(self, schema):
        return "invalidArray"

    def handleMap(self, schema):
        return "invalidMap"

    def handleUnion(self, schema):
        validTypes = set()
        for subSchema in schema.schemas:
            validTypes.add(subSchema.type)
        if 'string' not in validTypes:
            return 'invalidString'
        elif 'map' not in validTypes:
            return {}
        elif 'array' not in validTypes:
            return []
        # no union should have this many types not in it...
        raise PrototoolsException()

    def handleRecord(self, schema):
        return "invalidRecord"


class DefaultInstanceCreator(TypicalInstanceCreator):
    """
    Generates typical instances with only the default fields set
    """
    def getInstance(self):
        defaultInstance = self.class_()
        jsonDict = protocol.toJsonDict(defaultInstance)
        for fieldName in defaultInstance.__slots__:
            if fieldName in [
                    f for f in defaultInstance.DESCRIPTOR.fields
                    if f.label == descriptor.FieldDescriptor.LABEL_REQUIRED]:
                typicalValue = self.getFieldValue(fieldName)
                jsonDict[fieldName] = typicalValue
        instance = protocol.fromJsonDict(jsonDict, self.class_)
        return instance


class Validator(ProtoTool):
    def getInvalidFields(self, jsonDict):
        # FIXME: get the proper list of fields
        protocol.fromJsonDict(jsonDict, self.class_)
        return []
