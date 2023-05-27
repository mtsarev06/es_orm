from unittest import TestCase
from datetime import datetime

from src.es_orm.fields import (Date, Choices, List, Integer, Dict,
                               ConvertedDict, Text, InnerDocument,
                               Float, Bytes)
from src.es_orm.exceptions import (ValidationException, InitializationRequired)


class DateFieldTest(TestCase):
    def test_init(self):
        Date()
        Date("%Y-%m-d")
        self.assertRaises(ValueError, Date, "%123 123")

    def test_clean_set_iso_date(self):
        test_object = Date()
        test_object.set('1970-01-01T03:00:00')
        test_object.clean()
        self.assertEqual(datetime.fromtimestamp(0), test_object.value)

    def test_clean_set_custom_format(self):
        test_object = Date('%d.%m.%Y')
        test_object.set('10.01.2022')
        test_object.clean()
        self.assertEqual(datetime.fromisoformat("2022-01-10T00:00:00"),
                         test_object.value)
        test_object.set('1010.10.2000')
        self.assertRaises(ValidationException, test_object.clean)

    def test_clean_set_timestamp(self):
        test_object = Date()
        test_object.set(0)
        test_object.clean()
        self.assertEqual(datetime.fromtimestamp(0), test_object.value)

        test_object.set(1000)
        test_object.clean()
        self.assertEqual(datetime.fromtimestamp(1000), test_object.value)


class ChoicesFieldTest(TestCase):
    def test_init(self):
        Choices([])
        Choices([1, 2, 3])
        Choices((1, 2, 3))
        self.assertRaises(TypeError, Choices)
        self.assertRaises(TypeError, Choices, 123)

    def test_clean(self):
        test_object = Choices([1, 2, 3])
        test_object.set(1)
        test_object.clean()

        test_object.set(2)
        test_object.clean()

        test_object.set("1")
        self.assertRaises(ValidationException, test_object.clean)

        test_object.set([1, 2, 3])
        self.assertRaises(ValidationException, test_object.clean)

        test_object.set(None)
        test_object.clean()


class ListFieldTest(TestCase):
    def test_init(self):
        List()
        List(Integer)
        List(Integer())
        self.assertRaises(TypeError, List, 123)

    def test_clean_with_invalid_data(self):
        test_object = List(Integer())
        test_object.set(123)
        self.assertRaises(ValidationException, test_object.clean)

        test_object.set(["my_str"])
        self.assertRaises(ValidationException, test_object.clean)

        test_object.set([123, 555, "str", 23])
        self.assertRaises(ValidationException, test_object.clean)

    def test_clean_with_valid_data(self):
        test_object = List(Integer())
        test_object.set([123])
        test_object.clean()

        test_object.set([321, "123", 555, "2"])
        test_object.clean()

        test_object.set([])
        test_object.clean()

        test_object.set({})
        test_object.clean()

        test_object.set(None)
        test_object.clean()

    def test_clean_complex_object(self):
        test_object = List(Dict(properties={
            "inner_dict": Dict(properties={
                "inner_int": Integer()
            }),
            "int": Integer()
        }))

        test_object.set([{"int": 123}, {"inner_dict": {"inner_int": 22}},
                         {"int": 321, "inner_dict": {'inner_int': 23}}])
        test_object.clean()

        test_object.set([{"int": "s123"}, {"inner_dict": {"inner_int": 22}},
                         {"int": 321, "inner_dict": {'inner_int': 23}}])
        self.assertRaises(ValidationException, test_object.clean)

        test_object.set([{"int": 123}, {"inner_dict": {"inner_int": "l22"}},
                         {"int": 321, "inner_dict": {'inner_int': 23}}])
        self.assertRaises(ValidationException, test_object.clean)

        test_object.set([{"int": 123}, {"inner_dict": {"inner_int": 22}},
                         {"int": "j321", "inner_dict": {'inner_int': 23}}])
        self.assertRaises(ValidationException, test_object.clean)

    def test_clean_complex_object_with_warning_validation(self):
        test_object = List(Dict(properties={
            "inner_dict": Dict(properties={
                "inner_int": Integer()
            }),
            "int": Integer()
        }), validation_level=List.ValidationLevel.WARNING)

        test_object.set([{"int": 123}, {"inner_dict": {"inner_int": 22}},
                         {"int": 321, "inner_dict": {'inner_int': 23}}])
        test_object.clean()
        self.assertTrue('warning' not in test_object._attrs)

        test_object.set([{"int": "s123"}, {"inner_dict": {"inner_int": 22}},
                         {"int": 321, "inner_dict": {'inner_int': 23}}])
        test_object.clean()
        self.assertTrue('warning' in test_object._attrs)
        self.assertTrue('int' in test_object._attrs['warning'])

        test_object.set([{"int": 123}, {"inner_dict": {"inner_int": "l22"}},
                         {"int": 321, "inner_dict": {'inner_int': 23}}])
        test_object.clean()
        self.assertTrue('warning' in test_object._attrs)
        self.assertTrue('inner_int' in test_object._attrs['warning'])

        test_object.set([{"int": 123}, {"inner_dict": {"inner_int": 22}},
                         {"int": "j321", "inner_dict": {'inner_int': 23}}])
        test_object.clean()
        self.assertTrue('warning' in test_object._attrs)
        self.assertTrue('int' in test_object._attrs['warning'])


class ConvertedDictTest(TestCase):
    def test_init(self):
        ConvertedDict()

    def test_clean(self):
        test_object = ConvertedDict()
        test_object.set({'int': 123, 'inner_dict': {'inner_int': 42}})

        self.assertRaises(InitializationRequired, test_object.clean)
        self.assertEqual(test_object._ConvertedDict__depth, 2)
        self.assertEqual(list(test_object._properties.keys()),
                         ['key', 'value', 'inner_dict'])
        self.assertEqual(list(test_object._properties['inner_dict'].keys()),
                         ['key', 'value'])

    def test_serialize(self):
        test_object = ConvertedDict()
        test_object.set({'int': 123, 'inner_dict': {'inner_int': 42}})

        self.assertEqual(test_object.serialize(),
                         {
                             "value": [
                                 {"key": "int", "value": 123},
                                 {
                                     "key": "inner_dict",
                                      "inner_dict": [
                                          {"key": "inner_int", "value": 42}
                                      ]
                                 }
                             ]
                         })

    def test_deserialize(self):
        test_object = ConvertedDict()
        self.assertEqual(test_object._ConvertedDict__depth, 1)
        test_object.deserialize([
                                 {"key": "int", "value": 123},
                                 {
                                     "key": "inner_dict",
                                      "inner_dict": [
                                          {"key": "inner_int", "value": 42}
                                      ]
                                 }
                               ], auto_set=True)
        self.assertEqual(test_object.value,
                         {'int': 123, 'inner_dict': {'inner_int': 42}})
        self.assertEqual(test_object._ConvertedDict__depth, 2)

        test_object.deserialize({
            "value": [
                {"key": "int", "value": 321},
                {
                    "key": "inner_dict",
                    "inner_dict": [
                        {"key": "inner_int", "value": 22}
                    ]
                }
            ]
        }, auto_set=True)
        self.assertEqual(test_object.value,
                         {'int': 321, 'inner_dict': {'inner_int': 22}})

    def test_validation_level_warning(self):
        test_object = ConvertedDict(
            validation_level=ConvertedDict.ValidationLevel.WARNING)
        test_object.set("some_test_data")
        test_object.clean()
        self.assertTrue('warning' in test_object._attrs.keys())
        self.assertEqual(test_object.serialize()['value'],
                         {
                             "value": "some_test_data"
                         })
        self.assertEqual(test_object.value, "some_test_data")

        test_object.deserialize("some_test_data")
        self.assertEqual(test_object.value, "some_test_data")

        test_object.deserialize({"value": {"value": "some_test_data"}})
        self.assertEqual(test_object.value, "some_test_data")

        test_object.deserialize({"value": "some_test_data"})
        self.assertEqual(test_object.value, "some_test_data")


class InnerDocumentTest(TestCase):
    def test_inner_document(self):
        class MyInnerDocument(InnerDocument):
            int = Integer()
            float = Float()

        test_object = MyInnerDocument()
        self.assertEqual(type(test_object), Dict)
        self.assertEqual(sorted(list(test_object._properties.keys())),
                         ['float', 'int'])

        for field_name, field_object in test_object._properties.items():
            if field_object._attrs_enabled:
                self.fail(f'There is attrs enabled to the field {field_name}')


class BytesTest(TestCase):
    def test_bytes(self):
        test_bytes = b'\xd01234'
        test_bytes_str = '\\xd01234'
        test_object = Bytes()
        test_object.set(test_bytes)
        self.assertEqual(test_object._serialize(), test_bytes_str)
        self.assertEqual(test_object._deserialize(test_bytes_str),
                         test_bytes)