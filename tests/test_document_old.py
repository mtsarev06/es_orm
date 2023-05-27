import os
import unittest
import json
from datetime import datetime
from time import sleep

import elasticsearch_dsl

from src.es_orm import Document, connect
from src.es_orm.fields import (Integer, Text, Date, Choices, Field, Dict, List,
                               Float, InnerDocument, ConvertedDict)
from src.es_orm.exceptions import (ValidationException, InitializationRequired)


class DocumentTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        es_host = os.environ.get('ES_HOST', None)
        if not es_host:
            raise EnvironmentError('Please provide ES_HOST environment '
                                   'variable to start unittesting.')
        connect(es_host)

        class TestStrictClass(Document):
            array = List()
            int_array = List(Integer)
            integer = Integer()
            text = Text()
            date = Date()
            choices = Choices(['1 option', '2 option', '3 option'])
            dict = Dict()
            converted_dict = ConvertedDict()

            class Index:
                name = 'test_strict_index'

        class TestWarningClass(Document):
            array = List(validation_level=Field.ValidationLevel.WARNING)
            int_array = List(Integer,
                             validation_level=Field.ValidationLevel.WARNING)
            integer = Integer(validation_level=Field.ValidationLevel.WARNING)
            text = Text(validation_level=Field.ValidationLevel.WARNING)
            date = Date(validation_level=Field.ValidationLevel.WARNING)
            choices = Choices(
                ['1 option', '2 option', '3 option'],
                validation_level=Field.ValidationLevel.WARNING)
            dict = Dict(validation_level=Field.ValidationLevel.WARNING)
            converted_dict = ConvertedDict(
                validation_level=Field.ValidationLevel.WARNING)

            class Index:
                name = 'test_warning_index'

        class TestDisabledClass(Document):
            array = List(validation_level=Field.ValidationLevel.DISABLED)
            int_array = List(Integer,
                             validation_level=Field.ValidationLevel.DISABLED)
            integer = Integer(validation_level=Field.ValidationLevel.DISABLED)
            text = Text(validation_level=Field.ValidationLevel.DISABLED)
            date = Date(validation_level=Field.ValidationLevel.DISABLED)
            choices = Choices(['1 option', '2 option', '3 option'],
                              validation_level=Field.ValidationLevel.DISABLED)
            dict = Dict(validation_level=Field.ValidationLevel.DISABLED)
            converted_dict = ConvertedDict(
                validation_level=Field.ValidationLevel.DISABLED)

            class Index:
                name = 'test_disabled_index'

        class WithoutConversion(Document):
            dict = Dict(properties={
                'text': Text(),
                'int': Integer(),
            })

            class Index:
                name = "test_dict_in_dict_without_conversion"

        class TimestampTest(Document):
            test_field = Text()

            class Index:
                name = "test_timestamp_index"
                timestamp_enabled = True

        class TestValueDisabled(Document):
            text = Text(attrs_enabled=False)
            integer = Integer(attrs_enabled=False)
            array = List(Float, attrs_enabled=False)
            converted_dict = ConvertedDict(attrs_enabled=False)
            usual_dict = Dict(attrs_enabled=False, properties={
                "key1": Text(attrs_enabled=False),
                "key2": Integer(attrs_enabled=False),
                "key3": Float(),
                "key4": ConvertedDict(),
                "key5": Dict(attrs_enabled=False, properties={
                    "inner_key1": Date(attrs_enabled=False),
                    "inner_key2": Text(attrs_enabled=False)
                })
            })

            class Index:
                name = "test_value_disabled_index"

        class TestLists(Document):
            array = List(Dict(properties={
                "key1": Integer(attrs_enabled=False),
                "key2": Float(attrs_enabled=False)
            }))
            list_of_choices = List(Choices(choices=["1 option", "2 option"]))
            list_of_dates = List(Date)
            list_of_documents = List(TestValueDisabled.to_field())
            list_of_lists = List(List(Integer))
            list_of_lists_of_lists = List(List(List(Integer)))

            class Index:
                name = "test_lists_index"

        if elasticsearch_dsl.Index('test_strict_index').exists():
            elasticsearch_dsl.Index('test_strict_index').delete()
        TestStrictClass.init()

        if elasticsearch_dsl.Index('test_warning_index').exists():
            elasticsearch_dsl.Index('test_warning_index').delete()
        TestWarningClass.init()

        if elasticsearch_dsl.Index('test_disabled_index').exists():
            elasticsearch_dsl.Index('test_disabled_index').delete()
        TestDisabledClass.init()

        if elasticsearch_dsl.Index('test_dict_in_dict_without_conversion').\
                exists():
            elasticsearch_dsl.Index('test_dict_in_dict_without_conversion').\
                delete()
        WithoutConversion.init()
        
        if elasticsearch_dsl.Index('test_timestamp_index').exists():
            elasticsearch_dsl.Index('test_timestamp_index').delete()
        TimestampTest.init()

        if elasticsearch_dsl.Index('test_value_disabled_index').exists():
            elasticsearch_dsl.Index('test_value_disabled_index').delete()
        TestValueDisabled.init()

        if elasticsearch_dsl.Index('test_lists_index').exists():
            elasticsearch_dsl.Index('test_lists_index').delete()
        TestLists.init()

        cls.test_strict_class = TestStrictClass
        cls.test_warning_class = TestWarningClass
        cls.test_disabled_class = TestDisabledClass
        cls.test_without_conversion_class = WithoutConversion
        cls.test_timestamp_class = TimestampTest
        cls.test_value_disabled_class = TestValueDisabled
        cls.test_lists_class = TestLists

    @classmethod
    def tearDownClass(cls) -> None:
        elasticsearch_dsl.Index(cls.test_strict_class.Index.name).delete()
        elasticsearch_dsl.Index(cls.test_warning_class.Index.name).delete()
        elasticsearch_dsl.Index(cls.test_disabled_class.Index.name).delete()
        elasticsearch_dsl.Index(cls.test_without_conversion_class.Index.name).\
            delete()
        elasticsearch_dsl.Index(cls.test_timestamp_class.Index.name).delete()
        elasticsearch_dsl.Index(cls.test_lists_class.Index.name).delete()

    def test_init(self):
        try:
            test_data = {
                "array": [1, 2, '3'],
                "int_array": [1, 2, 3],
                "integer": 512,
                "text": "Text message",
                "date": "00:00:00 02.02.02",
                "choices": "2 option",
                "dict": {'first': 'value 1', 'second': 'value 2'},
                "converted_dict": {'first': 'value 1', 'second': 'value 2'}
            }
            self.test_strict_class(**test_data)
            self.test_warning_class(**test_data)
            self.test_disabled_class(**test_data)
        except Exception as error:
            self.fail(error)

    def test_set(self):
        test_object = self.test_strict_class()
        test_object.set({'integer': 123}, text='test')
        self.assertEqual(test_object.integer, 123)
        self.assertEqual(test_object.text, 'test')

        test_object.set({'undefined_integer': 123},
                        undefined_text='test', array=[1, 2, 3])
        self.assertRaises(AttributeError, getattr, test_object,
                          'undefined_integer')
        self.assertRaises(AttributeError, getattr, test_object,
                          'undefined_text')
        self.assertEqual(test_object.array, [1, 2, 3])

    def test_valid_data_assertion(self):
        valid_data = {
            "array": None,
            "int_array": None,
            "integer": None,
            "text": None,
            "date": None,
            "choices": None,
            "dict": None,
            "converted_dict": None,
            "undefined_field": None,
            "undefined_field_with_value": None
        }

        try:
            strict_instance = self.test_strict_class(**valid_data)
            warning_instance = self.test_warning_class(**valid_data)
            disabled_instance = self.test_disabled_class(**valid_data)
            strict_instance.clean()
            warning_instance.clean()
            disabled_instance.clean()
        except ValidationException as error:
            self.fail(error)
        except Exception as error:
            self.fail(f"There was an error validation "
                      f"empty valid object: {error}")

        valid_data = {
            "array": [1, 2, "3"],
            "int_array": [1, 2, 3],
            "integer": 512,
            "text": "Text message",
            "date": "00:00:00 02.02.02",
            "choices": "2 option",
            "dict": {'first': 'value 1', 'second': 'value 2'},
            "converted_dict": {'first': 'value 1', 'second': 'value 2'},
            "undefined_field": 123.512,
            "undefined_field_with_value": {
                "value": 123.11
            }
        }

        json_data = json.dumps(valid_data)
        # Initialize using json

        try:
            strict_instance = self.test_strict_class(json_data=json_data)
            warning_instance = self.test_warning_class(json_data=json_data)
            disabled_instance = self.test_disabled_class(json_data=json_data)
            strict_instance.clean()
            warning_instance.clean()
            disabled_instance.clean()
        except ValidationException as error:
            self.fail(error)

        expected_data = {
            "array": ['1', '2', '3'],
            "int_array": [1, 2, 3],
            "integer": 512,
            "text": "Text message",
            "date": datetime(2002, 2, 2, 0, 0),
            "choices": "2 option",
            "dict": {'first': 'value 1', 'second': 'value 2'},
            "converted_dict": {'first': 'value 1', 'second': 'value 2'}
        }

        for field in expected_data.keys():
            value_to_check = expected_data[field]
            self.assertEqual(strict_instance[field].value, value_to_check)
            self.assertEqual(warning_instance[field].value, value_to_check)
            self.assertEqual(disabled_instance[field].value, value_to_check)

        self.assertTrue('undefined_field' not in strict_instance)
        self.assertTrue('undefined_field' not in warning_instance)
        self.assertTrue('undefined_field' not in disabled_instance)

        # Assertion via initialisator
        while True:
            try:
                strict_instance = self.test_strict_class(**valid_data)
                warning_instance = self.test_warning_class(**valid_data)
                disabled_instance = self.test_disabled_class(**valid_data)
                strict_instance.clean()
                warning_instance.clean()
                disabled_instance.clean()
            except ValidationException as error:
                self.fail(error)
            except InitializationRequired:
                continue
            break

        for field in expected_data.keys():
            value_to_check = expected_data[field]
            self.assertEqual(strict_instance[field].value, value_to_check)
            self.assertEqual(warning_instance[field].value, value_to_check)
            self.assertEqual(disabled_instance[field].value, value_to_check)

        self.assertTrue('undefined_field' not in strict_instance)
        self.assertTrue('undefined_field' not in warning_instance)
        self.assertTrue('undefined_field' not in disabled_instance)

        # Direct assertion
        try:
            strict_instance = self.test_strict_class()
            warning_instance = self.test_warning_class()
            disabled_instance = self.test_disabled_class()

            for field in valid_data.keys():
                value_to_set = valid_data[field]
                setattr(strict_instance, field, value_to_set)
                setattr(warning_instance, field, value_to_set)
                setattr(disabled_instance, field, value_to_set)

            strict_instance.clean()
            warning_instance.clean()
            disabled_instance.clean()
        except ValidationException as error:
            self.fail(error)

        for field in expected_data.keys():
            value_to_check = expected_data[field]
            self.assertEqual(strict_instance[field].value, value_to_check)
            self.assertEqual(warning_instance[field].value, value_to_check)
            self.assertEqual(disabled_instance[field].value, value_to_check)

        self.assertTrue('undefined_field' not in strict_instance)
        self.assertTrue('undefined_field' not in warning_instance)
        self.assertTrue('undefined_field' not in disabled_instance)

        try:
            strict_instance.save()
            warning_instance.save()
            disabled_instance.save()
        except Exception as error:
            self.fail('Не удалось сохранить данные в БД: '+str(error.args))

        try:
            strict_instance.to_dict()
            warning_instance.to_dict()
            disabled_instance.to_dict()
        except Exception as error:
            self.fail('Не удалось перевести валидные данные в словарь: '+str(error.args))

        expected_document = {
            "array": {
                "value": ['1', '2', '3'],
            },
            "int_array": {
                "value": [1, 2, 3],
            },
            "integer": {
                "value": 512,
            },
            "text": {
                "value": "Text message",
            },
            "date": {
                "value": "2002-02-02T00:00:00",
            },
            "choices": {
                "value": "2 option",
            },
            "dict": {
                "value": {
                    "first": "value 1",
                    "second": "value 2"
                }
            },
            "converted_dict": {
                "value": [
                    {"key": "first", "value": "value 1"},
                    {"key": "second", "value": "value 2"},
                ]
            },
            "undefined_field": {
                "value": '123.512'
            },
            "undefined_field_with_value": {
                "value": '123.11'
            }
        }
        sleep(1)
        db_document = elasticsearch_dsl.Search().\
            index(strict_instance.Index.name).\
            query("match", _id=strict_instance.meta['id']).\
            execute()[0].to_dict()
        self.assertEqual(expected_document, db_document)

    def test_invalid_data_assertion(self):
        invalid_data = {
            "integer": "512s",
            "int_array": [1, 2, 3, '4s'],
            "date": "-42156742344:sda:231",
            "choices": "4 option",
        }

        strict_instance = self.test_strict_class(**invalid_data)
        warning_instance = self.test_warning_class(**invalid_data)
        disabled_instance = self.test_disabled_class(**invalid_data)

        try:
            strict_instance.clean()
        except ValidationException as error:
            for key in invalid_data.keys():
                if key not in str(error):
                    self.fail(
                        f"{key} key of the Document with invalid data hasn't "
                        f"been validated properly and didn't raise "
                        f"ValidationException.")
        else:
            self.fail("There hasn't been any error validating "
                      "invalid data in the class with strict validation.")

        try:
            warning_instance.clean()
        except Exception as error:
            self.fail(f"There has been an error validating "
                      f"invalid data in the class with 'warning' "
                      f"validation level:{error}")

        for field in invalid_data.keys():
            self.assertTrue('warning' in warning_instance[field]._attrs)

        try:
            disabled_instance.clean()
        except Exception:
            self.fail("There has been an error validating invalid "
                      "data in the class with 'disabled' validation level")

        for field in invalid_data.keys():
            self.assertEqual(getattr(disabled_instance, field),
                             invalid_data[field])

        try:
            warning_instance.to_dict()
            disabled_instance.to_dict()
        except Exception as error:
            self.fail('Не удалось перевести невалидные данные '
                      'в словарь: ' + str(error.args))

    def test_dict_in_dict(self):
        data = {
            "dict": {
                "inner_dict": {
                    "key1": "value1",
                    "key2": "value2",
                }
            }
        }
        try:
            test_object = self.test_strict_class(**data)
            test_object.save()
        except Exception as error:
            self.fail('Не удалось сохранить словарь в словаре: '
                      ''+str(error.args))
        sleep(1.5)
        db_document = elasticsearch_dsl.Search().\
            index(test_object.Index.name).\
            query("match", _id=test_object.meta['id']).execute()[0].to_dict()
        expected_value = {
            "dict": {
                "value": {
                    "inner_dict": {
                        "key1": "value1",
                        "key2": "value2",
                    }
                }
            }
        }
        self.assertEqual(db_document, expected_value)

        # Dict in dict without conversion

        expected_mapping = {
            'test_dict_in_dict_without_conversion': {
                'mappings': {
                    'properties': {
                        'dict': {
                            'properties': {
                                'value': {
                                    'properties': {
                                        'int': {'type': 'integer'},
                                        'text': {
                                            'type': 'text',
                                            'fields': {
                                                "keyword": {
                                                    "type": "keyword",
                                                    "ignore_above": 256
                                                }
                                            }
                                        }}},
                                'content_type': {
                                    'fields': {'keyword': {'ignore_above': 256,
                                                           'type': 'keyword'}},
                                    'type': 'text'
                                },
                                'pretty_name': {
                                    'fields': {'keyword': {'ignore_above': 256,
                                                           'type': 'keyword'}},
                                    'type': 'text'
                                },
                            }
                        }
                    }
                }
            }
        }

        class WithoutConversion(self.test_without_conversion_class):
            pass

        self.assertEqual(expected_mapping,
                         WithoutConversion._index.get_mapping())

        test = WithoutConversion(dict={'text': 5211, 'int': 2512})
        test.save()

        expected_value = {'dict': {'value': {'text': '5211', 'int': 2512}}}
        self.assertEqual(expected_value, test.to_dict())

    def test_defaults(self):
        valid_data = {
            "array": [1, 2, '3'],
            "int_array": [1, 2, 3],
            "integer": 512,
            "text": "Text message",
            "date": "00:00:00 02.02.2002",
            "choices": "2 option",
            "dict": {'first': 'value 1', 'second': 'value 2'}
        }
        document = self.test_strict_class(**valid_data)
        document.save()
        try:
            object_defaults = document.defaults()
        except Exception as error:
            self.fail(error)

        for field in object_defaults:
            default_defaults = self.test_strict_class()._properties[field].\
                __class__().defaults
            self.assertEqual(object_defaults[field], default_defaults)

        defaults = {
            'pretty_name': 'My test field',
            'random_prop': 'random_value'
        }

        class TestDefaults(Document):
            text_field = Text(defaults=defaults)
            int_field = Integer(defaults={'pretty_name': 'My field'})
            dict_field = List(Dict(properties={
                "inner_int": Integer(
                    defaults={'pretty_name': 'My inner int field'}),
                "inner_dict": Dict(properties={
                    "inner_inner_int": Integer(
                        defaults={'pretty_name': 'My inner inner int field'})
                }, defaults={'pretty_name': 'My inner dict field'})
            }, defaults={'pretty_name': 'My dict field'}))

            class Index:
                name = 'test_defaults'

        test_object = TestDefaults(text_field='123')

        document_defaults = test_object.defaults()
        self.assertTrue(defaults.items() <=
                        document_defaults['text_field'].items())

        requested_fields = ['array', 'integer', 'text']
        document_defaults = document.defaults(fields=requested_fields)
        self.assertEqual(requested_fields, list(document_defaults.keys()))

        requested_props = ['pretty_name']
        document_defaults = document.defaults(props=requested_props)
        for prop, value in document_defaults.items():
            self.assertEqual(requested_props, list(value.keys()))

        filter = 'test.*'
        document_defaults = test_object.defaults(filter=filter)
        self.assertEqual(list(document_defaults.keys()), ['text_field'])

        dict_defaults = test_object.defaults(
            fields=['dict_field'], props=['pretty_name', 'type', 'properties'])

        expected_defaults = {
            "dict_field": {
                "pretty_name": "My dict field",
                "type": "object",
                "properties": {
                    "inner_int": {
                        "pretty_name": "My inner int field",
                        "type": "integer",
                        "content_type": "text"
                    },
                    "inner_dict": {
                        "pretty_name": "My inner dict field",
                        "type": "object",
                        "content_type": "text",
                        "properties": {
                            "inner_inner_int": {
                                "pretty_name": "My inner inner int field",
                                "type": "integer",
                                "content_type": "text"
                            }
                        }
                    }
                }
            }
        }

        self.assertEqual(dict_defaults, expected_defaults)
        
    def test_timestamp(self):
        self.test_timestamp_class._index.delete()
        self.test_timestamp_class.init()
        mapping = self.test_timestamp_class._index.get_mapping()
        expected_mapping = {
            'test_timestamp_index': {
                'mappings': {
                    'properties': {
                        'test_field': {
                            'properties': {
                                'value': {
                                    'type': 'text',
                                    'fields': {
                                        'keyword': {
                                            'type': 'keyword',
                                            'ignore_above': 256
                                        }
                                    }
                                },
                                'content_type': {
                                    'fields': {'keyword': {'ignore_above': 256,
                                                           'type': 'keyword'}},
                                    'type': 'text'},
                                'pretty_name': {
                                    'fields': {'keyword': {'ignore_above': 256,
                                                           'type': 'keyword'}},
                                    'type': 'text'},
                            }
                        },
                        'timestamp': {
                            'properties': {
                                'value': {
                                    'type': 'date'
                                },
                                'content_type': {
                                    'fields': {'keyword': {'ignore_above': 256,
                                                           'type': 'keyword'}},
                                    'type': 'text'},
                                'pretty_name': {
                                    'fields': {'keyword': {'ignore_above': 256,
                                                           'type': 'keyword'}},
                                    'type': 'text'},
                            }
                        }
                    }
                }
            }
        }
        self.maxDiff = None
        self.assertEqual(mapping, expected_mapping)

        # Check if it adds a timestamp to the new document
        test_document = self.test_timestamp_class(test_field="123")
        test_document.save()
        self.assertTrue(hasattr(self.test_timestamp_class.
                                get(id=test_document.meta['id']), 'timestamp'))

        # Check if it changes timestamp if it already has a timestamp (should not).
        old_timestamp = test_document.timestamp
        test_document.save()
        self.assertEqual(self.test_timestamp_class.
                         get(id=test_document.meta['id']).timestamp, old_timestamp)

        # Check if it doesn't add timestamp for a document with disabled timestamp
        test_document = self.test_strict_class(text="123")
        test_document.save()
        self.assertFalse(hasattr(self.test_strict_class.
                                 get(id=test_document.meta['id']), 'timestamp'))

    def test_export(self):
        test_json = {
            "test": "test2",
            "test2": {
                "some_test": "something_else"
            }
        }
        test_data = self.test_strict_class(dict=test_json)
        exported_data = test_data.serialize()
        self.assertEqual(exported_data['dict']['value'], test_json)
        exported_data = test_data.serialize(True)
        self.assertEqual(exported_data['_source']['dict']['value'], test_json)

        class TestExport(Document):
            dict = Dict(properties={
                "date": Date(),
                "dict": Dict()
            })

        test_object = TestExport(
            dict={'date': "2020-01-02", 'dict':
                {'field1': 'value1', 'field2': {'field3': 'value3'}}})

        test_object.clean()

        expected_value = {
            "dict": {
                "value": {
                    "date": "2020-01-02T00:00:00",
                    'dict': {'field1': 'value1',
                             'field2': {'field3': 'value3'}}
                }
            }
        }

        self.assertEqual(expected_value, test_object.serialize())

    def test_doc_to_field(self):
        class TestDocToField(Document):
            integer = Integer()
            text = Text()
            dict = ConvertedDict()
            usual_dict = Dict(properties={'test': Integer()})
            doc_field = self.test_timestamp_class.to_field()
        self.maxDiff = None
        doc_field_mapping = TestDocToField._doc_type.\
            mapping.to_dict()['properties']['doc_field']['properties']['value']
        doc_field_mapping.pop('type')
        expected_mapping = {
            "properties": {
                "test_field": {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                        }
                    }
                },
                "timestamp": {"type": "date"}
            }
        }
        self.assertEqual(doc_field_mapping, expected_mapping)

    def test_value_disabled(self):
        mapping = self.test_value_disabled_class._index.\
            get_mapping()[self.test_value_disabled_class.Index.name]\
            ['mappings']['properties']
        expected_mapping = {
            "text": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            "integer": {
                "type": "integer"
            },
            "array": {
                "type": "float"
            },
            "converted_dict": {
                "properties": {
                    "key": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    },
                    "value": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    }
                }
            },
            "usual_dict": {
                "properties": {
                    "key1": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    },
                    "key2": {
                        "type": "integer"
                    },
                    "key3": {
                        "type": "float"
                    },
                    "key4": {
                        "properties": {
                            "key": {
                                "type": "text",
                                "fields": {
                                    "keyword": {
                                        "type": "keyword",
                                        "ignore_above": 256
                                    }
                                }
                            },
                            "value": {
                                "type": "text",
                                "fields": {
                                    "keyword": {
                                        "type": "keyword",
                                        "ignore_above": 256
                                    }
                                }
                            }
                        }

                    },
                    "key5": {
                        "properties": {
                            "inner_key1": {
                                "type": "date"
                            },
                            "inner_key2": {
                                "type": "text",
                                "fields": {
                                    "keyword": {
                                        "type": "keyword",
                                        "ignore_above": 256
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        self.assertEqual(mapping, expected_mapping)

    def test_lists(self):
        # Test whether we can store dicts in lists
        mapping = self.test_lists_class._index.get_mapping()\
            [self.test_lists_class.Index.name]['mappings']['properties']
        list_of_documents_mapping = self.test_value_disabled_class.\
            _index.get_mapping()[self.test_value_disabled_class.Index.name]\
            ['mappings']

        expected_mapping = {
            "array": {
                "properties": {
                    "value": {
                        "properties": {
                            "key1": {
                                "type": "integer"
                            },
                            "key2": {
                                "type": "float"
                            }
                        }
                    },
                    'content_type': {
                        'fields': {'keyword': {'ignore_above': 256,
                                               'type': 'keyword'}},
                        'type': 'text'},
                    'pretty_name': {'fields': {'keyword': {'ignore_above': 256,
                                                           'type': 'keyword'}},
                                    'type': 'text'},
                }
            },
            "list_of_choices": {
                "properties": {
                    "value": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    },
                    'content_type': {
                        'fields': {'keyword': {'ignore_above': 256,
                                               'type': 'keyword'}},
                        'type': 'text'},
                    'pretty_name': {'fields': {'keyword': {'ignore_above': 256,
                                                           'type': 'keyword'}},
                                    'type': 'text'},
                }
            },
            "list_of_dates": {
                "properties": {
                    "value": {
                        "type": "date",
                    },
                    'content_type': {
                        'fields': {'keyword': {'ignore_above': 256,
                                               'type': 'keyword'}},
                        'type': 'text'},
                    'pretty_name': {'fields': {'keyword': {'ignore_above': 256,
                                                           'type': 'keyword'}},
                                    'type': 'text'},
                }
            },
            "list_of_lists": {
                    "properties": {
                        "value": {
                            "type": "integer"
                        },
                        'content_type': {
                            'fields': {'keyword': {'ignore_above': 256,
                                                   'type': 'keyword'}},
                            'type': 'text'},
                        'pretty_name': {
                            'fields': {'keyword': {'ignore_above': 256,
                                                   'type': 'keyword'}},
                            'type': 'text'},
                    }
            },
            "list_of_lists_of_lists": {
                "properties": {
                    "value": {
                        "type": "integer"
                    },
                    'content_type': {
                        'fields': {'keyword': {'ignore_above': 256,
                                               'type': 'keyword'}},
                        'type': 'text'},
                    'pretty_name': {'fields': {'keyword': {'ignore_above': 256,
                                                           'type': 'keyword'}},
                                    'type': 'text'},
                }
            }
        , **{
                "list_of_documents": {
                    "properties": {
                        "value": list_of_documents_mapping,
                        'content_type': {
                            'fields': {'keyword': {'ignore_above': 256,
                                                   'type': 'keyword'}},
                            'type': 'text'},
                        'pretty_name': {
                            'fields': {'keyword': {'ignore_above': 256,
                                                   'type': 'keyword'}},
                            'type': 'text'},
                    }
                }
            },
        }
        self.maxDiff = None
        self.assertEqual(mapping, expected_mapping)

        document = self.test_lists_class(array=[{"key1": 123, "key2": 123.52},
                                                {"key1": 421, "key2": 52.4}])
        document.save()

        expected_document = {
            "array": {
                "value": [
                    {"key1": 123, "key2": 123.52},
                    {"key1": 421, "key2": 52.4}
                ]
            }
        }
        self.assertEqual(expected_document, document.to_dict())

        # Test whether we can store choices in the lists

        test_valid_object = self.test_lists_class(
            list_of_choices=['1 option', '1 option', '2 option'])
        try:
            test_valid_object.clean()
        except ValidationException as error:
            self.fail("Haven't managed to validate a "
                      "valid object: "+str(error.args))

        test_invalid_object = self.test_lists_class(
            list_of_choices=['1 option', '3 option', '2 option'])
        self.assertRaises(ValidationException, test_invalid_object.clean)

        test_valid_object = self.test_lists_class(
            list_of_dates=[datetime.now(), datetime.fromtimestamp(41251)])
        try:
            test_valid_object.clean()
        except ValidationException as error:
            self.fail("Haven't managed to validate "
                      "a valid object: "+str(error.args))

        test_invalid_object = self.test_lists_class(
            list_of_choices=['1 option', datetime.now()])
        self.assertRaises(ValidationException, test_invalid_object.clean)

        # Checking if the List of documents works fine
        element1 = {
            "integer": 241,
            "array": [512.22, 111.21]
        }
        element2 = {
            "integer": 521,
            "array": [112.22, 111.21]
        }

        test_object = self.test_lists_class(list_of_documents=[element1,
                                                               element2])

        test_object.save()

        expected_document = {
            "list_of_documents": {
                "value": [
                    element1,
                    element2
                ]
            }
        }

        self.assertEqual(expected_document, test_object.to_dict())

        test_document1 = self.test_value_disabled_class()
        test_document2 = self.test_value_disabled_class()

        test_document1['integer'], test_document1['array'] = \
            element1['integer'], element1['array']
        test_document2['integer'], test_document2['array'] = \
            element2['integer'], element2['array']

        test_object = self.test_lists_class(
            list_of_documents=[test_document1, test_document2])

        test_object.save()

        self.assertEqual(expected_document, test_object.to_dict())

        # Check if List of Lists works fine

        element = [[1, 2, 3], [3, 4, 5]]
        expected_document = {
            "list_of_lists": {
                "value": element
            }
        }
        test_document = self.test_lists_class(list_of_lists=element)
        test_document.save()

        self.assertEqual(expected_document, test_document.to_dict())

        # Check if List of Lists of Lists works fine

        element = [[[1, 2, 3], [2, 3, 4]], [[3, 4, 5], [5, 6]]]
        expected_document = {
            "list_of_lists_of_lists": {
                "value": element
            }
        }
        test_document = self.test_lists_class(list_of_lists_of_lists=element)
        test_document.save()

        self.assertEqual(expected_document, test_document.to_dict())

    def test_date(self):
        class TestDate(Document):
            date_field = Date(date_format="%H:%M:%S %d.%m.%Y")

        test_time = "22:54:59 01.01.1092"
        try:
            test_object = TestDate(date_field=test_time)
            test_object.clean()
            export = test_object.serialize()
        except Exception as error:
            self.fail(str(error.args))

        self.assertEqual("1092-01-01T22:54:59", export['date_field']['value'])

        class TestDate(Document):
            date_field = Date(date_format="%Y-%m-%d")

        try:
            test_object = TestDate(date_field="1092-01-01")
            test_object.clean()
            export = test_object.serialize()
        except Exception as error:
            self.fail(str(error.args))

        self.assertEqual("1092-01-01T00:00:00", export['date_field']['value'])

    def test_validate_dict(self):
        class TestValidateDict(Document):
            dict = Dict(properties={
                "key1": Integer(attrs_enabled=False),
                "key2": Dict(properties={
                    "key3": Choices(['1 option', '2 option'],
                                    attrs_enabled=False)
                }, attrs_enabled=False)
            })

        test_object = TestValidateDict(
            dict={'key1': 23, 'key2': {'key3': '1 option'}})
        try:
            test_object.clean()
        except ValidationException:
            self.fail("Haven't managed to validate a valid Dict object.")

        test_object = TestValidateDict(
            dict={'key1': "2test", 'key2': {'key3': '1 option'}})
        self.assertRaises(ValidationException, test_object.clean)

        try:
            test_object = TestValidateDict(dict={'key1': 123, 'key2': 42})
        except TypeError:
            pass
        else:
            self.fail('There was no error trying to assign non-dict value '
                      'to the Dict field.')

        test_object = TestValidateDict(
            dict={'key1': 23, 'key2': {'key3': '3 option'}})
        self.assertRaises(ValidationException, test_object.clean)

    def test_dict_assertion(self):
        class TestDictAssertion(Document):
            dict = Dict(properties={
                "key1": Integer(attrs_enabled=False),
                "key2": Float(attrs_enabled=False)
            })

            dict2 = Dict(properties={
                "key1": Integer(attrs_enabled=False),
                "key2": Float(attrs_enabled=False)
            }, attrs_enabled=False)

        test_object = TestDictAssertion(dict={'key1': 125})
        test_object2 = TestDictAssertion(dict={'key2': 125.4})
        test_object3 = TestDictAssertion(dict={})

        try:
            test_object.clean()
            test_object2.clean()
            test_object3.clean()
        except Exception as error:
            self.fail("Haven't managed to validate "
                      "the valid object. "+str(error.args))

        test_object = TestDictAssertion(dict2={'key1': 125})
        test_object2 = TestDictAssertion(dict2={'key2': 125.4})
        test_object3 = TestDictAssertion(dict2={})

        try:
            test_object.clean()
            test_object2.clean()
            test_object3.clean()
        except Exception as error:
            self.fail("Haven't managed to validate the "
                      "valid object. "+str(error.args))

    def test_serialize_dict(self):
        class TestSerializeDict(Document):
            dict = Dict(properties={
                "date": Date(attrs_enabled=False),
                "dict": ConvertedDict(attrs_enabled=False)
            })

        test_object = TestSerializeDict(dict={'date': '10.01.2011',
                                              'dict': {'key1': 'value1'}})
        serialized_data = test_object.to_dict()
        self.assertEqual(type(serialized_data['dict']['value']['date']), str)
        self.assertEqual(len(serialized_data['dict']['value']['dict']), 1)
        self.assertEqual(
            list(serialized_data['dict']['value']['dict'][0].keys()),
            ['key', 'value'])

    def test_assertion_with_attrs_disabled(self):
        class TestClass(Document):
            field1 = Integer(attrs_enabled=False)
            field2 = Dict(properties={
                "inner_field1": Text(attrs_enabled=False),
                "inner_field2": Dict(attrs_enabled=False)
            }, attrs_enabled=False)
        test_data = {
            "field1": 123,
            "field2": {
                "inner_field1": "test",
                "inner_field2": {
                    "key1": "value1"
                }
            }
        }
        test_object = TestClass(**test_data)
        self.assertEqual(test_data, test_object.to_dict())

        test_object = TestClass()
        for key in test_data.keys():
            setattr(test_object, key, test_data[key])
        self.assertEqual(test_data, test_object.to_dict())

    def test_assign_undeclared_field(self):
        class TestAssignUndeclaredField(Document):
            some_field = Integer()

        test_object = TestAssignUndeclaredField(another_field=123.55)
        self.assertTrue('another_field' not in test_object)

        test_object = TestAssignUndeclaredField()
        test_object.another_field = 123.55
        self.assertEqual(type(test_object['another_field']), Text)
        self.assertEqual(test_object['another_field']['value'], 123.55)
        self.assertEqual(test_object.another_field, 123.55)

    def test_bulk(self):
        doc_objects = [self.test_strict_class(
            integer=i, date=datetime.fromtimestamp(i),
            dict={'key1': 'val1', 'key2': {'key3': 'val3'}}) for i in range(5)]

        try:
            result = self.test_strict_class.bulk_save(doc_objects)
        except Exception as error:
            self.fail(f"Haven't managed to perform a bulk request: {error.args}")
        self.assertEqual(result, (len(doc_objects), []))
        self.assertEqual(result[1], [])

        for doc_object in doc_objects:
            db_document = self.test_strict_class.get(id=doc_object.meta['id'])
            self.assertEqual(db_document.to_dict(), doc_object.to_dict())
            self.assertEqual(db_document, doc_object)

        doc_objects = list([self.test_strict_class(
            integer=i, date=datetime.fromtimestamp(i),
            dict={'key1': 'val1', 'key2': {'key3': 'val3'}}) for i in range(5)])
        doc_objects[1] = None
        doc_objects[4] = "Some test"

        try:
            result = self.test_strict_class.bulk_save(doc_objects)
        except Exception as error:
            self.fail(f"Haven't managed to perform "
                      f"a bulk request: {error.args}")
        self.assertEqual(result[0], len(doc_objects)-2)
        self.assertEqual(len(result[1]), 0)

        for doc_object in doc_objects:
            if not isinstance(doc_object, Document):
                continue
            db_document = self.test_strict_class.get(id=doc_object.meta['id'])
            self.assertEqual(db_document.to_dict(), doc_object.to_dict())
            self.assertEqual(db_document, doc_object)

        doc_objects = list([self.test_timestamp_class(
            test_field="123",
            dict={'key1': 'val1', 'key2': {'key3': 'val3'}}) for i in range(2)])

        try:
            result = self.test_timestamp_class.bulk_save(doc_objects)
        except Exception as error:
            self.fail(f"Haven't managed to perform a "
                      f"bulk request: {error.args}")

        for doc_object in doc_objects:
            db_document = self.test_timestamp_class.\
                get(id=doc_object.meta['id'])
            self.assertEqual(db_document.timestamp, doc_object.timestamp)
            self.assertEqual(db_document, doc_object)


    def test_list_of_lists(self):
        class TestListOfLists(Document):
            list_of_lists = List(List(Integer))

        try:
            test_object = TestListOfLists(list_of_lists=[[1, 2, 3], [2, 3, 4]])
            test_object.clean()
            test_object = TestListOfLists(list_of_lists=[[17, 2, 3]])
            test_object.clean()
        except Exception as error:
            self.fail("Haven't managed to validate a valid "
                      "list of lists: "+str(error.args))

        test_object = TestListOfLists(list_of_lists=[[1, 2, '4.211'],
                                                     [24, 1, 4]])
        self.assertRaises(ValidationException, test_object.clean)

        test_object = TestListOfLists(list_of_lists=[[1, 2, 4],
                                                     [24, '1.66', 4]])
        self.assertRaises(ValidationException, test_object.clean)

        test_object = TestListOfLists(list_of_lists=[[24, '1.66', 4]])
        self.assertRaises(ValidationException, test_object.clean)

    def test_inner_document(self):
        class TestInnerDocument(InnerDocument):
            int_field = Integer()
            list_field = List(Integer)
            dict_field = Dict(properties={
                'inner_int': Integer()
            })

        inner_doc = TestInnerDocument()

        expected_properties = ['int_field', 'list_field', 'dict_field']
        for expected_property in expected_properties:
            self.assertTrue(expected_property in inner_doc._properties)

        self.assertEqual(len(inner_doc._properties), len(expected_properties))

        expected_mapping = {
            "properties": {
                "int_field": {
                    "type": "integer"
                },
                "list_field": {
                    "type": "integer"
                },
                "dict_field": {
                    "properties": {
                        "inner_int": {
                            "type": "integer"
                        }
                    }
                }
            },
        }

        self.assertEqual({
            'properties': {
                'value': expected_mapping,
                'content_type': {'fields': {'keyword': {'ignore_above': 256,
                                                        'type': 'keyword'}},
                                 'type': 'text'},
                'pretty_name': {'fields': {'keyword': {'ignore_above': 256,
                                                       'type': 'keyword'}},
                                'type': 'text'},
            },
        }, inner_doc.mapping())

        inner_doc = TestInnerDocument(attrs_enabled=False)
        self.assertEqual(expected_mapping, inner_doc.mapping())

    # TODO: implement a test which checks if attrs recieved from ES are added
    # to the fields.