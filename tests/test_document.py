from unittest import TestCase

from elasticsearch_dsl import Integer as DslInteger

from src.es_orm import Document, fields


class DocumentTest(TestCase):
    def test_init(self):
        Document()
        Document(meta={'id': 123})
        Document(json_data={'something': 123})
        Document(json_data='{"something": 123}')

        try:
            class TestDocument(Document):
                test_int = DslInteger()
        except TypeError:
            pass
        else:
            self.fail("There wasn't any error while creating a class with a "
                      "DSL field.")

    def test_init_with_initial_value(self):
        test_object = Document(json_data={'something': 123})
        self.assertEqual(test_object._fields_dict.serialize(),
                         {'something': {'value': 123}})

        class TestDocument(Document):
            test_int = fields.Integer()

        test_object = TestDocument(json_data={
            'test_int': 123,
            'something': 123
        })
        self.assertEqual(test_object._fields_dict.serialize(),
                         {'test_int': {'value': 123}})

        test_object = TestDocument(json_data='{"test_int": 123}')
        self.assertEqual(test_object._fields_dict.serialize(),
                         {'test_int': {'value': 123}})

    def test_setting_attributes(self):
        class TestAttrsDocument(Document):
            test_int = fields.Integer()

        test_object = TestAttrsDocument()
        test_object.test_int = 1
        self.assertEqual(test_object._fields_dict.test_int, 1)

        test_object = TestAttrsDocument()
        test_object.test_something = 1
        self.assertEqual(test_object._fields_dict.test_something, 1)

        test_object.test_dict = {'something': 123}
        self.assertEqual(test_object._fields_dict.test_dict.value,
                         {'something': 123})
        self.assertEqual(test_object._fields_dict.test_dict.something, 123)

    def test_attrs_disabled(self):
        class TestClass(Document):
            test_field = fields.Text()
            test_field2 = fields.Dict()

            class Index:
                attrs_enabled = False

        self.assertEqual(TestClass()['test_field']._attrs_enabled, False)
        self.assertEqual(TestClass()['test_field2']._attrs_enabled, False)