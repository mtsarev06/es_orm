import os
import unittest
from time import sleep

import elasticsearch_dsl

from src.es_orm import Document, connect
from src.es_orm.fields import (Date, Text, List, Integer, Choices,
                               Dict, ConvertedDict)


class DBInteractionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        es_host = os.environ.get('ES_HOST', None)
        if not es_host:
            raise EnvironmentError('Please provide ES_HOST environment '
                                   'variable to start unittesting.')
        connect(es_host)

        class TestStrictClass(Document):
            array = List()
            integer = Integer()
            text = Text()
            date = Date()
            choices = Choices(['1 option', '2 option', '3 option'])
            dict = ConvertedDict()

            class Index:
                name = 'test_db_interaction_index'

        cls.test_class = TestStrictClass
        if elasticsearch_dsl.Index(cls.test_class.Index.name).exists():
            elasticsearch_dsl.Index(cls.test_class.Index.name).delete()
        elasticsearch_dsl.Index(cls.test_class.Index.name).create()

    @classmethod
    def tearDownClass(cls) -> None:
        elasticsearch_dsl.Index(cls.test_class.Index.name).delete()

    def test_init(self):
        test_data = {
            "array": [1, 2, 3],
            "integer": 512,
            "text": "Text message",
            "date": "00:00:00 02.02.02",
            "choices": "2 option",
            "dict": {
                "inner_dict": {
                    "inner_key": "inner_value"
                },
                "key": "value"
            }
        }
        try:
            self.test_class.init()
            new_document = self.test_class(**test_data)
            new_document.save()
        except Exception as error:
            self.fail("Couldn't manage to initialize index "
                      "in the DB: "+str(error.args))

        expected_mapping = {
            'test_db_interaction_index': {
                'mappings': {
                    'properties': {
                        'array': {
                            'properties': {
                                'value': {
                                    'type': 'text', 'fields': {
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
                        'choices': {
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
                        'date': {
                            'properties': {
                                'value': {'type': 'date'},
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
                        'dict': {
                            'properties': {
                                'value': {
                                    'properties': {
                                        'inner_dict': {
                                            'properties': {
                                                'key': {
                                                    'type': 'text',
                                                    'fields': {
                                                        'keyword': {
                                                            'type': 'keyword',
                                                            'ignore_above': 256
                                                        }
                                                    }
                                                },
                                                'value': {
                                                    'type': 'text',
                                                    'fields': {
                                                        'keyword': {
                                                            'type': 'keyword',
                                                            'ignore_above': 256
                                                        }
                                                    }
                                                }
                                            }
                                        },
                                        'key': {
                                            'type': 'text',
                                            "fields": {
                                                "keyword": {
                                                    "type": "keyword",
                                                    "ignore_above": 256
                                                }
                                            }
                                        },
                                        'value': {
                                            'type': 'text',
                                            "fields": {
                                                "keyword": {
                                                    "type": "keyword",
                                                    "ignore_above": 256
                                                }
                                            }
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
                        'integer': {'properties': {
                            'value': {'type': 'integer'},
                            'content_type': {
                                'fields': {'keyword': {'ignore_above': 256,
                                                       'type': 'keyword'}},
                                'type': 'text'},
                            'pretty_name': {
                                'fields': {'keyword': {'ignore_above': 256,
                                                       'type': 'keyword'}},
                                'type': 'text'},
                        }},
                        'text': {
                            'properties': {
                                'value': {
                                    'type': 'text',
                                    'fields': {
                                        'keyword': {
                                            'type': 'keyword',
                                            'ignore_above': 256}
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
                        }}}}}
        self.assertEqual(expected_mapping, new_document._index.get_mapping())

    def test_save_and_retrieve_data(self):
        test_data = {
                "array": [1, 2, 3],
                "integer": 512,
                "text": "Text message",
                "date": "00:00:00 02.02.02",
                "choices": "2 option",
            }
        new_document = self.test_class(**test_data)

        try:
            new_document.save()
            sleep(1)
        except Exception as error:
            self.fail(str(error.args))

        db_document = self.test_class.get(id=new_document.meta.id)
        db_document.full_clean()
        self.assertEqual(new_document, db_document)

    def test_update_document(self):
        test_data = {
            "array": [1, 2, 3],
            "integer": 512,
            "text": "Text message",
            "date": "00:00:00 02.02.02",
            "choices": "2 option",
        }
        new_document = self.test_class(**test_data)

        new_document.save()

        db_document = self.test_class.get(id=new_document.meta.id)

        new_data = {
            "array": [2, 3, 4],
            "integer": 999,
            "text": "Text message 2",
            "date": "10:10:10 02.02.02",
            "choices": "3 option",
        }

        db_document.set(**new_data)
        new_document.set(**new_data)

        new_document.clean()
        db_document.save()
        sleep(2)
        db_document = self.test_class.get(id=new_document.meta.id)

        self.assertEqual(db_document, new_document)
        self.assertEqual(db_document.serialize(), new_document.serialize())
