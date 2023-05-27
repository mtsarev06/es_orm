from unittest import TestCase

from src.es_orm.fields import Dict, Field, Text
from src.es_orm.exceptions import ValidationException


class TestDictField(TestCase):
    def test_init(self):
        test_object = Dict()
        self.assertEqual(test_object._properties, {})
        test_object = Dict(properties={})
        self.assertEqual(test_object._properties, {})
        test_object = Dict(properties={"something": "field"})
        self.assertEqual(test_object._properties, {})
        test_object = Dict(properties={
            "text": Text(),
            "field": Field(),
            "wrong": "field"
        })
        self.assertEqual(list(test_object._properties.keys()),
                         ["text", "field"])

    def test_init_with_invalid_data(self):
        self.assertRaises(TypeError, Dict, properties="1")

    def test_assigning_data(self):
        test_object = Dict()
        self.assertRaises(TypeError, test_object.set, "123")
        self.assertRaises(TypeError, setattr, test_object, 'value', "123")
        self.assertEqual(test_object.value, {})
        test_object.set({'something': "new"}, include_undefined=True)
        self.assertEqual(list(test_object._properties.keys()), ['something'])
        self.assertEqual(test_object._properties['something'].value, "new")
        self.assertEqual(test_object.something, "new")

        self.assertRaises(TypeError, test_object.set, **{'value': "123"})
        test_object.set({'value': {'something': 'new2'}})
        self.assertEqual(test_object.something, "new2")

        test_object.something = 10
        self.assertEqual(test_object._properties['something'].value, 10)
        self.assertEqual(test_object._properties['something'].value,
                         test_object.something)

        test_object.test_data = 10
        self.assertTrue('test_data' in test_object._properties)
        self.assertTrue(type(test_object._properties['test_data']) is Text)
        self.assertTrue(test_object._properties['test_data'].value, 10)

        self.assertEqual(test_object.serialize(),
                         {
                             "value": {
                                 "something": 10,
                                 "test_data": 10
                             }
                         })


    def test_set(self):
        test_object = Dict(properties={"test": Text(), "test2": Text()})
        test_object.test = 10
        test_object.set({'test2': 10})
        self.assertEqual(test_object.serialize(),
                         {
                             "value": {
                                 "test2": 10
                             }
                         })

        test_object.set({'test3': 10})
        self.assertEqual(test_object.serialize(), None)

        test_object.set({
            'value': {'test': 10},
            'pretty_name': 123
        })
        self.assertEqual(test_object.serialize(),
                         {
                             "value": {
                                 "test": 10
                             },
                             'pretty_name': 123
                         })

        test_object.set({
            'value': {'test': 10},
            'pretty_name': 123,
            '123': 123
        })
        self.assertEqual(test_object.serialize(),
                         {
                             "value": {
                                 "test": 10
                             },
                             'pretty_name': 123,
                             '123': 123
                         })

    def test_set_including_undefined(self):
        test_object = Dict(properties={"test": Text()})
        test_object.set({"test": 123, "test2": 321})
        self.assertEqual(test_object.value, {"test": 123})

        test_object.set({"test": 1234, "test2": 321}, include_undefined=True)
        self.assertEqual(test_object.value, {"test": 1234, "test2": 321})

    def test_work_with_square_brackets(self):
        test_object = Dict(properties={'text': Text()})
        test_object['text'] = 10
        self.assertEqual(test_object.text, 10)

        self.assertEqual(test_object['text']['value'], 10)
        self.assertEqual(test_object['text']['pretty_name'], 'Поле')

        self.assertEqual(test_object.serialize(),
                         {
                            "value": {
                                "text": 10,
                            }
                         })
        try:
            test_object['pretty_name'] = "new_name"
        except KeyError:
            pass
        else:
            self.fail("There was no error raised when trying to set an "
                      "attribute using square brackets.")

    def test_update(self):
        test_object = Dict(properties={"test": Text(), "test2": Text()})
        test_object.update({'test': 123}, test2=321, test3=333)
        self.assertEqual(test_object.serialize(),
                         {
                             "value": {
                                 "test": 123,
                                 "test2": 321
                             }
                         })
        test_object.update(test3=333, include_undefined=True)
        self.assertEqual(test_object.serialize(),
                         {
                             "value": {
                                 "test": 123,
                                 "test2": 321,
                                 "test3": 333
                             }
                         })

    def test_clear(self):
        test_object = Dict(properties={'test': Text(), 'test2': Text()})
        test_object.set({'test': 123, 'test2': 321})
        test_object.clear()
        self.assertEqual(test_object.test, None)
        self.assertEqual(test_object.test2, None)
        self.assertEqual(test_object.serialize(), None)

    def test_get(self):
        test_object = Dict(properties={'test': Text(), 'test2': Text()})
        test_object.set({'test': 123, 'test2': 321})
        self.assertEqual(test_object.get('test'), 123)
        self.assertEqual(test_object.get('test3', None), None)
        self.assertRaises(KeyError, test_object.get, 'test3')

    def test_items(self):
        test_object = Dict(properties={'test': Text(), 'test2': Text()})
        test_object.set({'test': 123, 'test2': 321})
        self.assertEqual(list(test_object.items()),
                         [('test', 123), ('test2', 321)])

    def test_keys(self):
        test_object = Dict(properties={'test': Text(), 'test2': Text()})
        test_object.set({'test': 123, 'test2': 321})
        self.assertEqual(list(test_object.keys()),
                         ['test', 'test2'])

    def test_pop(self):
        test_object = Dict(properties={'test': Text(), 'test2': Text()})
        test_object.set({'test': 123, 'test2': 321})
        self.assertEqual(test_object.pop(123, None), None)
        self.assertRaises(KeyError, test_object.pop, 123)
        self.assertEqual(test_object.pop("test2"), 321)
        self.assertEqual(test_object.serialize(),
                         {
                             'value': {
                                 "test": 123
                             }
                         })

    def test_values(self):
        test_object = Dict(properties={'test': Text(), 'test2': Text()})
        test_object.set({'test': 123, 'test2': 321})
        self.assertEqual(list(test_object.values()), [123, 321])

    def test_clean(self):
        test_object = Dict(properties={
            'test': Field(),
            'test2': Field(),
            'dict': Dict(properties={
                "test": Field()
            })
        })
        test_object._properties['test']._value_type = int
        test_object._properties['test2']._value_type = float
        test_object._properties['dict']._properties["test"]._value_type = float

        try:
            test_object.clean()
        except Exception as error:
            self.fail(f"There was an error validating an empty dictionary: "
                      f"{error}.")

        test_object.set({'test': "123", "test2": "123.22"})
        try:
            test_object.clean()
        except Exception:
            self.fail("There was an error validating an valid dictionary.")

        self.assertEqual(test_object.test, 123)
        self.assertEqual(test_object.test2, 123.22)

        test_object.set({'dict': {'test': "123.22"}})
        test_object.clean()
        self.assertEqual(test_object.dict.test, 123.22)

        test_object.set({'dict': {'test': "test"}})
        self.assertRaises(ValidationException, test_object.clean)

        test_object = Dict(properties={
            'test': Field(),
            'test2': Field(),
            'dict': Dict(properties={
                "test": Field()
            })
        }, validation_level=Field.ValidationLevel.WARNING)
        test_object._properties['test']._value_type = int
        test_object._properties['test2']._value_type = float
        test_object._properties['dict']._properties["test"]._value_type = float

        test_object.set({'dict': {'test': "test"}})
        test_object.update({'test': "test"})
        test_object.clean()
        self.assertTrue('test' in test_object._attrs['warning'])
        self.assertTrue('dict' in test_object._attrs['warning'])





