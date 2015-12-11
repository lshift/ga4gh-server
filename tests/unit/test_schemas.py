"""
Tests for auto generated schemas and conversion to and from JSON.
"""
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import string
import random
import unittest
import pprint

import ga4gh.prototools as prototools
import ga4gh.protocol as protocol
import tests.utils as utils

import google.protobuf.descriptor as descriptor
import google.protobuf.json_format as json_format
import proto.google.protobuf.struct_pb2 as struct_pb2


def randomString():
    """
    Returns a randomly generated short string.
    """
    randInt = random.randint(0, 10)
    randStr = ''.join(random.choice(
        string.ascii_letters) for _ in range(randInt))
    return randStr


class SchemaTest(unittest.TestCase):
    """
    Superclass of schema tests.
    """
    typicalValueMap = {
        "string": "string value",
        "int": 1000,
        "long": 10000,
        "boolean": True,
        "double": 0.125,
        "float": 0.25
    }

    # def getAvroSchema(self, cls, fieldName):
    #     """
    #     Returns the avro schema for the specified field.
    #     """
    #     field = None
    #     for fld in cls.schema.fields:
    #         if fld.name == fieldName:
    #             field = fld
    #     return field

    def getInvalidValue(self, cls, fieldName):
        """
        Returns a value that should trigger a schema validation failure.
        """
        value = prototools.Creator(cls).getInvalidField(fieldName)
        return value

    def getTypicalValue(self, cls, fieldName):
        """
        Returns a typical value for the specified field on the specified
        Protocol class.
        """
        # We make some simplifying assumptions about how the schema is
        # structured which fits the way the GA4GH protocol is currently
        # designed but may break in the future. We try to at least flag
        # this fact here.
        err = "Schema structure assumptions violated"
        field = self.getAvroSchema(cls, fieldName)
        typ = field.type
        if isinstance(typ, avro.schema.UnionSchema):
            t0 = typ.schemas[0]
            t1 = typ.schemas[1]
            if isinstance(t0, avro.schema.PrimitiveSchema):
                if t0.type == "null":
                    typ = typ.schemas[1]
                elif t1.type == "null":
                    typ = typ.schemas[0]
                else:
                    raise Exception(err)
        ret = None
        if isinstance(typ, avro.schema.MapSchema):
            ret = {"key": ["value1", "value2"]}
            if not isinstance(typ.values, avro.schema.ArraySchema):
                raise Exception(err)
            if not isinstance(typ.values.items, avro.schema.PrimitiveSchema):
                raise Exception(err)
            if typ.values.items.type != "string":
                raise Exception(err)
        elif isinstance(typ, avro.schema.ArraySchema):
            if cls.isEmbeddedType(field.name):
                embeddedClass = cls.getEmbeddedType(field.name)
                ret = [self.getTypicalInstance(embeddedClass)]
            else:
                if not isinstance(typ.items, avro.schema.PrimitiveSchema):
                    raise Exception(err)
                ret = [self.typicalValueMap[typ.items.type]]
        elif isinstance(typ, avro.schema.EnumSchema):
            ret = typ.symbols[0]
        elif isinstance(typ, avro.schema.RecordSchema):
            self.assertTrue(cls.isEmbeddedType(fieldName))
            embeddedClass = cls.getEmbeddedType(fieldName)
            ret = self.getTypicalInstance(embeddedClass)
        elif typ.type in self.typicalValueMap:
            ret = self.typicalValueMap[typ.type]
        else:
            raise Exception(err)

        return ret

    def getTypicalInstance(self, cls):
        """
        Returns a typical instance of the specified protocol class.
        """
        tool = prototools.Creator(cls)
        instance = tool.getTypicalInstance()
        return instance

    def getRandomInstance(self, cls):
        """
        Returns an instance of the specified class with randomly generated
        values conforming to the schema.
        """
        tool = prototools.Creator(cls)
        instance = tool.getRandomInstance()
        return instance

    def getDefaultInstance(self, cls):
        """
        Returns a new instance with the required values set.
        """
        tool = prototools.Creator(cls)
        instance = tool.getDefaultInstance()
        return instance


class EqualityTest(SchemaTest):
    def snakeKeyDict(self, item, field):
        snake_test_val = {}
        for k in item.keys():
            k_field = [
                f for f in field.message_type.fields
                if prototools.ProtoTypeSwitch.toCamelCase(f.name) == k][0]
            if json_format._IsMapEntry(k_field):
                new_item = {}
                for x in item[k].keys():
                    values = item[k][x]["values"]
                    new_values = []
                    for v in values:
                        key = v.keys()[0]
                        snake_key = prototools.ProtoTypeSwitch.toSnakeCase(key)
                        new_values.append({snake_key: v[key]})
                    new_item[x] = struct_pb2.ListValue(values=new_values)
                item[k] = new_item
            elif type(item[k]) == dict:
                item[k] = self.snakeKeyDict(item[k], k_field)
            elif type(item[k]) == list:
                new_item = []
                for i in item[k]:
                    if type(i) == dict:
                        new_item.append(self.snakeKeyDict(i, k_field))
                    else:
                        new_item.append(i)
                item[k] = new_item

            snake_test_val[k_field.name] = item[k]
        return snake_test_val

    def setField(self, i1, field, test_val):
        if field.label == descriptor.FieldDescriptor.LABEL_REPEATED:
            repeated = getattr(i1, field.name)
            if hasattr(repeated, "append"):
                repeated.extend(test_val)
            elif hasattr(repeated, "add"):
                for item in test_val:
                    snake_test_val = self.snakeKeyDict(item, field)
                    try:
                        repeated.add(**snake_test_val)
                    except Exception, e:
                        print(field.full_name, test_val[0].keys())
                        print("snake test val")
                        pprint.pprint(snake_test_val)
                        raise Exception(
                            (e,
                                field.message_type.full_name,
                                type(repeated),
                                [f.name for f in field.message_type.fields]))
            elif hasattr(repeated, "get_or_create"):
                v = repeated["bar"]
                if type(v) == struct_pb2.Value:
                    v.string_value = "foo"
                else:
                    v.values.add(string_value="foo")
            else:
                raise Exception((type(repeated), dir(repeated)))
        elif type(test_val) == dict:
            value = getattr(i1, field.name)
            for k in test_val.keys():
                k_field = [
                    f for f in field.message_type.fields
                    if prototools.ProtoTypeSwitch.toCamelCase(f.name) == k][0]

                if k_field.type == descriptor.FieldDescriptor.TYPE_ENUM:
                    setattr(value, k_field.name, 2)
                elif k_field.type == descriptor.FieldDescriptor.TYPE_MESSAGE:
                    self.setField(value, k_field, test_val[k])
                else:
                    setattr(value, k_field.name, test_val[k])
        else:
            setattr(i1, field.name, test_val)

    """
    Tests equality is correctly calculated for different protocol elements.
    """
    def verifyEqualityOperations(self, i1, i2):
        self.assertEqual(i1, i1)
        self.assertTrue(i1 == i1)
        self.assertFalse(i1 != i1)
        self.assertEqual(i1, i2)
        self.assertTrue(i1 == i2)
        self.assertFalse(i1 != i2)
        for val in [None, {}, [], object, ""]:
            self.assertFalse(i1 == val)
            self.assertTrue(i1 != val)
            self.assertFalse(val == i1)
            self.assertTrue(val != i1)
        # Now change an attribute on one and check if equality fails.
        r = prototools.RandomInstanceCreator(i1)
        for field in i1.DESCRIPTOR.fields:
            if field.type == descriptor.FieldDescriptor.TYPE_ENUM:
                test_val = getattr(i1, field.name) + 1
            else:
                test_val = r.handleField(field)

            self.setField(i1, field, test_val)
            self.assertFalse(i1 == i2, (field.name, field.type, i1, i2))
            self.assertTrue(i1 != i2)

    def testSameClasses(self):
        factories = [self.getDefaultInstance, self.getTypicalInstance,
                     self.getRandomInstance]
        for cls in protocol.getProtocolClasses():
            for factory in factories:
                i1 = factory(cls)
                i2 = protocol.fromJsonDict(protocol.toJsonDict(i1), cls)
                self.verifyEqualityOperations(i1, i2)

    def testDifferentValues(self):
        def factory(cls):
            return cls()
        factories = [factory, self.getTypicalInstance, self.getDefaultInstance,
                     self.getRandomInstance]
        classes = list(protocol.getProtocolClasses())
        c1 = classes[0]
        for c2 in classes[1:]:
            for factory in factories:
                i1 = factory(c1)
                i2 = factory(c2)
                self.assertFalse(i1 == i2)
                self.assertTrue(i1 != i2)

    def testDifferentLengthArrays(self):
        i1 = self.getTypicalInstance(protocol.CallSet)
        i2 = protocol.fromJsonDict(protocol.toJsonDict(i1), protocol.CallSet)
        i2.variant_set_ids.append("extra")
        self.assertFalse(i1 == i2)

    def testKeywordInstantiation(self):
        for cls in protocol.getProtocolClasses():
            kwargs = {}
            instance = self.getDefaultInstance(cls)
            for key in protocol.toJsonDict(instance).keys():
                val = getattr(instance, key)
                kwargs[key] = val
            secondInstance = cls(**kwargs)
            self.assertEqual(instance, secondInstance)


class SerialisationTest(SchemaTest):
    """
    Tests the serialisation and deserialisation code for the schema classes
    """
    def validateClasses(self, factory):
        for cls in protocol.getProtocolClasses():
            instance = factory(cls)
            jsonStr = protocol.toJson(instance)
            otherInstance = protocol.fromJson(jsonStr, cls)
            self.assertEqual(instance, otherInstance)

            jsonDict = protocol.toJsonDict(instance)
            otherInstance = protocol.fromJsonDict(jsonDict, cls)
            self.assertEqual(instance, otherInstance)

    def testSerialiseDefaultValues(self):
        self.validateClasses(self.getDefaultInstance)

    def testSerialiseTypicalValues(self):
        self.validateClasses(self.getTypicalInstance)

    def testSerialiseRandomValues(self):
        self.validateClasses(self.getRandomInstance)


class ValidatorTest(SchemaTest):
    """
    Tests the validator to see if it will correctly identify instances
    that do not match the schema and also that it correctly identifies
    instances that do match the schema
    """
    def validateClasses(self, factory):
        for cls in protocol.getProtocolClasses():
            instance = factory(cls)
            jsonDict = protocol.toJson(instance)
            self.assertTrue(protocol.validate(jsonDict, cls))

    def testValidateDefaultValues(self):
        self.validateClasses(self.getDefaultInstance)

    def testValidateTypicalValues(self):
        self.validateClasses(self.getTypicalInstance)

    def testValidateRandomValues(self):
        self.validateClasses(self.getRandomInstance)

    def testValidateBadValues(self):
        for cls in protocol.getProtocolClasses():
            instance = self.getTypicalInstance(cls)
            jsonDict = protocol.toJsonDict(instance)
            self.assertFalse(protocol.validate(None, cls))
            self.assertFalse(protocol.validate([], cls))
            self.assertFalse(protocol.validate(1, cls))
            # setting values to bad values should be invalid
            for key in jsonDict.keys():
                dct = dict(jsonDict)
                dct[key] = self.getInvalidValue(cls, key)
                self.assertFalse(protocol.validate(dct, cls))
            for c in utils.powerset(jsonDict.keys(), 10):
                if len(c) > 0:
                    dct = dict(jsonDict)
                    for f in c:
                        dct[f] = self.getInvalidValue(cls, f)
                    self.assertFalse(protocol.validate(dct, cls))


class GetProtocolClassesTest(SchemaTest):
    """
    Tests the protocol.getProtocolClasses() function to ensure it
    works correctly.
    """
    def testAllClasses(self):
        classes = protocol.getProtocolClasses()
        self.assertGreater(len(classes), 0)
        for class_ in classes:
            self.assertTrue(issubclass(class_, protocol.message.Message))

    @unittest.skip("Protoc doesn't allow for the customisation Avro did")
    def testRequestAndResponseClasses(self):
        requestClasses = protocol.getProtocolClasses(protocol.SearchRequest)
        responseClasses = protocol.getProtocolClasses(protocol.SearchResponse)
        self.assertEqual(len(requestClasses), len(responseClasses))
        self.assertGreater(len(requestClasses), 0)
        for class_ in requestClasses:
            self.assertTrue(issubclass(class_, protocol.SearchRequest))
        for class_ in responseClasses:
            self.assertTrue(issubclass(class_, protocol.SearchResponse))
            valueListName = class_.getValueListName()
            self.assertGreater(len(valueListName), 0)


@unittest.skip("Protocol buffers don't have embedded types")
class EmbeddedValuesTest(SchemaTest):
    """
    Tests for the embedded values maps in ProtocolElements.
    """
    def testEmbeddedValues(self):
        for cls in protocol.getProtocolClasses():
            for member in cls.__slots__:
                if cls.isEmbeddedType(member):
                    instance = cls.getEmbeddedType(member)()
                    self.assertIsInstance(instance, protocol.ProtocolElement)
                else:
                    self.assertRaises(KeyError, cls.getEmbeddedType, member)


class SearchResponseBuilderTest(SchemaTest):
    """
    Tests the SearchResponseBuilder class to ensure that it behaves
    correctly.
    """
    @unittest.skip("Protoc doesn't allow for the customisation Avro did")
    def testIntegrity(self):
        # Verifies that the values we put in are exactly what we get
        # back across all subclasses of SearchResponse
        for class_ in protocol.getProtocolClasses(protocol.SearchResponse):
            instances = [
                self.getTypicalInstance(class_),
                self.getRandomInstance(class_)]
            for instance in instances:
                valueList = getattr(instance, class_.getValueListName())
                builder = protocol.SearchResponseBuilder(
                    class_, len(valueList), 2**32)
                for value in valueList:
                    builder.addValue(value)
                builder.setNextPageToken(instance.nextPageToken)
                otherInstance = protocol.fromJson(
                    builder.getSerializedResponse(), class_)
                self.assertEqual(instance,  otherInstance)

    def testPageSizeOverflow(self):
        # Verifies that the page size behaviour is correct when we keep
        # filling after full is True.
        responseClass = protocol.SearchVariantsResponse
        valueClass = protocol.Variant
        for pageSize in range(1, 10):
            builder = protocol.SearchResponseBuilder(
                responseClass, pageSize, 2**32)
            self.assertEqual(builder.getPageSize(), pageSize)
            self.assertFalse(builder.isFull())
            for listLength in range(1, 2 * pageSize):
                builder.addValue(self.getTypicalInstance(valueClass))
                instance = protocol.fromJson(
                    builder.getSerializedResponse(), responseClass)
                valueList = getattr(
                    instance, protocol.getValueListName(responseClass))
                self.assertEqual(len(valueList), listLength)
                if listLength < pageSize:
                    self.assertFalse(builder.isFull())
                else:
                    self.assertTrue(builder.isFull())

    def testPageSizeExactFill(self):
        responseClass = protocol.SearchVariantsResponse
        valueClass = protocol.Variant
        for pageSize in range(1, 10):
            builder = protocol.SearchResponseBuilder(
                responseClass, pageSize, 2**32)
            self.assertEqual(builder.getPageSize(), pageSize)
            while not builder.isFull():
                builder.addValue(self.getTypicalInstance(valueClass))
            instance = protocol.fromJson(
                builder.getSerializedResponse(), responseClass)
            valueList = getattr(instance, protocol.getValueListName(responseClass))
            self.assertEqual(len(valueList), pageSize)

    def testMaxResponseLengthOverridesPageSize(self):
        responseClass = protocol.SearchVariantsResponse
        valueClass = protocol.Variant
        typicalValue = self.getTypicalInstance(valueClass)
        typicalValueLength = len(protocol.toJson(typicalValue))
        for numValues in range(1, 10):
            maxResponseLength = numValues * typicalValueLength
            builder = protocol.SearchResponseBuilder(
                responseClass, 1000, maxResponseLength)
            self.assertEqual(
                maxResponseLength, builder.getMaxResponseLength())
            while not builder.isFull():
                builder.addValue(typicalValue)
            instance = protocol.fromJson(
                builder.getSerializedResponse(), responseClass)
            valueList = getattr(instance, protocol.getValueListName(responseClass))
            self.assertEqual(len(valueList), numValues)

    def testNextPageToken(self):
        responseClass = protocol.SearchVariantsResponse
        builder = protocol.SearchResponseBuilder(
            responseClass, 100, 2**32)
        # If not set, pageToken should be None
        self.assertIsNone(builder.getNextPageToken())
        instance = protocol.fromJson(
            builder.getSerializedResponse(), responseClass)
        self.assertEqual("", instance.next_page_token)
        # page tokens can be None or any string.
        for nextPageToken in ["", "string"]:
            builder.setNextPageToken(nextPageToken)
            self.assertEqual(nextPageToken, builder.getNextPageToken())
            instance = protocol.fromJson(
                builder.getSerializedResponse(), responseClass)
            self.assertEqual(nextPageToken, instance.next_page_token)
