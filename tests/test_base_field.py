from unittest import TestCase

from src.es_orm import Document
from src.es_orm.fields import Dict, Field, Text
from src.es_orm.exceptions import ValidationException


class BaseFieldTest(TestCase):
    def test_init(self):
        Field()
        Field(10, {}, 0, True)

    def test_invalid_init(self):
        self.assertRaises(Exception, Field, default_attrs=1)
        self.assertRaises(Exception, Field, default_attrs=[1])
        self.assertRaises(ValueError, Field, validation_level="something")

    def test_init_with_different_validation_levels(self):
        try:
            Field(validation_level=0)
            Field(validation_level=Field.ValidationLevel.STRICT)
            Field(validation_level=1)
            Field(validation_level=Field.ValidationLevel.WARNING)
            Field(validation_level=2)
            Field(validation_level=Field.ValidationLevel.DISABLED)
            Field(validation_level="STRICT")
            Field(validation_level="WARNING")
            Field(validation_level="DISABLED")
        except Exception as error:
            self.fail(f'There was an error initializing a Field object with '
                      f'a valid validation level: {error}.')
        self.assertRaises(ValueError, Field, validation_level=3)
        self.assertRaises(ValueError, Field, validation_level="1")
        self.assertRaises(ValueError, Field, validation_level="VERY STRICT")

    def test_init_with_different_default_attrs(self):
        test_field = Field(default_attrs={})
        self.assertEqual({"pretty_name": "Поле", "content_type": "text"},
                         test_field._default_attrs)
        test_field = Field(default_attrs={'pretty_name': '123'})
        self.assertEqual({"pretty_name": "123", "content_type": "text"},
                         test_field._default_attrs)
        test_field = Field(default_attrs={'content_type': '123'})
        self.assertEqual({"pretty_name": "Поле", "content_type": "123"},
                         test_field._default_attrs)
        test_field = Field(default_attrs={
            "pretty_name": "123",
            "content_type": "123"
        })
        self.assertEqual({"pretty_name": "123", "content_type": "123"},
                         test_field._default_attrs)
        test_field = Field(default_attrs={'123': '123'})
        self.assertEqual({
            "pretty_name": "Поле", "content_type": "text", "123": "123"
        }, test_field._default_attrs)

        self.assertEqual(Field._default_attrs,
                         {"pretty_name": "Поле", "content_type": "text"})

    def test_init_with_different_attrs_enabled(self):
        test_object = Field(attrs_enabled=True)
        self.assertTrue(test_object._attrs_enabled)

        test_object = Field(attrs_enabled=False)
        self.assertFalse(test_object._attrs_enabled)

        test_object = Field(attrs_enabled=0)
        self.assertFalse(test_object._attrs_enabled)

        test_object = Field(attrs_enabled="")
        self.assertFalse(test_object._attrs_enabled)

        test_object = Field(attrs_enabled="True")
        self.assertTrue(test_object._attrs_enabled)

    def test_init_with_default_value(self):
        test_object = Field(default=123, attrs_enabled=False)
        self.assertEqual(test_object.value, None)
        self.assertEqual(test_object.serialize(True), 123)

        test_object = Field(default="123", attrs_enabled=False)
        self.assertEqual(test_object.value, None)
        self.assertEqual(test_object.serialize(True), "123")

        test_object.value = 10
        self.assertEqual(test_object.value, 10)
        self.assertEqual(test_object.serialize(True), 10)

    def test_serialize(self):
        test_object = Field()
        self.assertEqual(test_object.serialize(True), None)

        test_object = Field(default=123)
        self.assertEqual(test_object.serialize(True),
                         {
                             "value": 123,
                             "pretty_name": "Поле",
                             "content_type": "text"
                         })
        self.assertEqual(test_object.value, None)
        self.assertEqual(test_object.serialize(),
                         {
                             "value": 123,
                         })

        test_object.set("321")
        self.assertEqual(test_object.serialize(True),
                         {
                             "value": "321",
                             "pretty_name": "Поле",
                             "content_type": "text"
                         })
        self.assertEqual(test_object.serialize(),
                         {
                             "value": "321"
                         })

        test_object.set({'value': 123})

    def test_set(self):
        test_object = Field()
        test_object.set(None)
        self.assertEqual(test_object.value, None)

        test_object.set(1)
        self.assertEqual(test_object.value, 1)

        test_object.set("1")
        self.assertEqual(test_object.value, "1")

        test_object.set("1")
        test_object.set_attrs(pretty_name="123")
        self.assertEqual(test_object._attrs,
                         {"pretty_name": "123"})
        self.assertEqual(test_object.serialize(True),
                         {
                             "value": "1",
                             "pretty_name": "123",
                             "content_type": "text"
                         })

        test_object.set_attrs({"123": "123"})
        self.assertEqual(test_object._attrs, {})
        self.assertEqual(test_object.serialize(True),
                         {
                             "value": "1",
                             "pretty_name": "Поле",
                             "content_type": "text",
                         })

        test_object = Field()
        test_object.set({'value': 5})
        self.assertEqual(test_object.value, 5)

        test_object.set({'value': 5, 'pretty_name': 10})
        self.assertEqual(test_object._attrs, {'pretty_name': 10})

        test_object.set({'value': 5, '123': 123})
        self.assertEqual(test_object._attrs, {'123': 123})

        test_object.set_attrs(attrs={'content_type': 20, '123': "123"})
        self.assertEqual(test_object._attrs,
                         {
                             'content_type': 20,
                             '123': '123'
                         })

    def test_update(self):
        test_object = Field()
        test_object.update(10)
        self.assertEqual(test_object.value, 10)

        test_object.update({'value': 11})
        self.assertEqual(test_object.value, 11)
        self.assertEqual(test_object._attrs, {})

        test_object.update({'value': 12, 'pretty_name': 'something'})
        self.assertEqual(test_object.value, 12)
        self.assertEqual(test_object._attrs, {'pretty_name': 'something'})

        test_object.update({'value': 14, '123': '123'})
        self.assertEqual(test_object.value, 14)
        self.assertEqual(test_object._attrs,
                         {
                             'pretty_name': 'something',
                             '123': '123'
                         })

    def test_set_attrs(self):
        self.assertRaises(TypeError, Field, default_attrs="1")
        test_object = Field(default="123", default_attrs={})
        self.assertEqual(test_object.serialize(True),
                         {
                             "value": "123",
                             "pretty_name": "Поле",
                             "content_type": "text"
                         })

        self.assertRaises(TypeError, test_object.set_attrs, "1")
        test_object.set_attrs({'123': "123"})
        self.assertEqual(test_object._attrs, {})

        test_object.set_attrs({'pretty_name': "123"})
        self.assertEqual(test_object._attrs, {"pretty_name": "123"})

    def test_update_attrs(self):
        test_object = Field()
        test_object.update_attrs({'pretty_name': 123})
        self.assertEqual(test_object._attrs, {'pretty_name': 123})

        test_object.update_attrs({'something': 123})
        self.assertEqual(test_object._attrs, {'pretty_name': 123})

        test_object.update_attrs(content_type="something")
        self.assertEqual(test_object._attrs,
                         {
                             'pretty_name': 123,
                             'content_type': 'something'
                         })

        test_object.update_attrs(content_typee="something")
        self.assertEqual(test_object._attrs,
                         {
                             'pretty_name': 123,
                             'content_type': 'something'
                         })

        test_object.update_attrs({'something': 123}, include_undefined=True)
        self.assertEqual(test_object._attrs,
                         {
                             'pretty_name': 123,
                             'content_type': 'something',
                             'something': 123
                         })

        test_object.update_attrs(something_more=123, include_undefined=True)
        self.assertEqual(test_object._attrs,
                         {
                             'pretty_name': 123,
                             'content_type': 'something',
                             'something': 123,
                             'something_more': 123
                         })

    def test_set_pretty_name_during_class_creation(self):
        class TestClass:
            test_field = Field()
            test_field2 = Dict()
            test_field3 = Text()

        self.assertEqual(TestClass.test_field._attrs, {})
        self.assertEqual(TestClass.test_field._default_attrs,
                         {
                             "pretty_name": "test_field",
                             "content_type": "text"
                         })

        self.assertEqual(TestClass.test_field2._default_attrs,
                         {
                             "pretty_name": "test_field2",
                             "content_type": "text"
                         })
        self.assertEqual(TestClass.test_field3._default_attrs,
                         {
                             "pretty_name": "test_field3",
                             "content_type": "text"
                         })

    def test_clean_with_strict_validation(self):
        test_field = Field()
        test_field._value_type = int
        try:
            test_field.clean()
        except Exception as error:
            self.fail(f"There was an error validating empty field: {error}")

        test_field.value = "123"
        try:
            test_field.clean()
        except Exception as error:
            self.fail(f"There was an error validating empty field: {error}")
        self.assertEqual(test_field.value, 123)

        test_field.value = 321
        try:
            test_field.clean()
        except Exception as error:
            self.fail(f"There was an error validating empty field: {error}")
        self.assertEqual(test_field.value, 321)

        test_field.value = "test_field"
        self.assertRaises(ValidationException, test_field.clean)

    def test_clean_with_warning_validation(self):
        test_field = Field(validation_level=Field.ValidationLevel.WARNING)
        test_field._value_type = int
        test_field.clean()

        test_field.value = 123
        test_field.clean()
        self.assertTrue('warning' not in test_field._attrs)

        test_field.value = "123"
        test_field.clean()
        self.assertTrue('warning' not in test_field._attrs)

        test_field.value = "test"
        test_field.clean()
        self.assertTrue('warning' in test_field._attrs)
        self.assertEqual(test_field.value, "test")

    def test_clean_with_disabled_validation(self):
        test_field = Field(validation_level=Field.ValidationLevel.DISABLED)
        test_field._value_type = int
        test_field.clean()

        test_field.value = 123
        test_field.clean()
        self.assertTrue('warning' not in test_field._attrs)

        test_field.value = "123"
        test_field.clean()
        self.assertTrue('warning' not in test_field._attrs)

        test_field.value = "test"
        test_field.clean()
        self.assertTrue('warning' not in test_field._attrs)
        self.assertEqual(test_field.value, "test")

    def test_clean_with_required_field(self):
        test_field = Field()
        try:
            test_field.clean()
            test_field.set()
        except Exception as error:
            self.fail(f"There was an error validation an empty field: {error}")

        test_field = Field(required=True)
        self.assertRaises(ValidationException, test_field.clean)

    def test_adding_of_analyzer(self):
        from elasticsearch_dsl import analyzer, char_filter

        code_char_filter = char_filter("code_char_filter", "mapping",
                                       mappings=[
                                           ": => \\u0020 : \\u0020",
                                       ])

        test_analyzer = analyzer("test_analyzer",
                                 tokenizer="whitespace",
                                 filter=["lowercase"],
                                 char_filter=code_char_filter)

        class TestClass(Document):
            test = Text(analyzer=test_analyzer, attrs_enabled=False)

        test_object = TestClass()
        self.assertEqual(test_object._index.to_dict()['settings']['analysis'],
                         test_analyzer.get_analysis_definition())
        self.assertEqual(test_object['test'].mapping(),
                         {
                             "type": "text",
                             'fields': {
                                 'keyword': {
                                     'ignore_above': 256,
                                     'type': 'keyword'
                                 }
                             },
                             "analyzer": "test_analyzer"
                         })

    def test_setting_of_attrs(self):
        test_object = Field()
        test_object.set_attrs({'pretty_name': 'test', '123': 123})
        self.assertEqual(test_object._attrs, {'pretty_name': 'test'})

        test_object.set_attrs(pretty_name='test2', some_more=123)
        self.assertEqual(test_object._attrs, {'pretty_name': 'test2'})

        test_object.set_attrs({'pretty_name': 'test', '123': 123},
                              include_undefined=True)
        self.assertEqual(test_object._attrs, {'pretty_name': 'test',
                                              '123': 123})

        test_object.set_attrs(pretty_name='test2', some_more=123,
                              include_undefined=True)
        self.assertEqual(test_object._attrs,
                         {
                             'pretty_name': 'test2',
                             'some_more': 123,
                         })

    def test_updating_of_attrs(self):
        pass