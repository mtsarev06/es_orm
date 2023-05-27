from unittest import TestCase

from src.es_orm import fields


class FieldMappingTest(TestCase):
    def test_mapping_with_attrs_disabled(self):
        test_object = fields.Field(attrs_enabled=False)
        self.assertEqual(test_object.mapping(),
                         {"type": "object"})

        test_object = fields.Dict(properties={
            "text": fields.Text(),
            "int": fields.Integer(),
            "inner_dict": fields.Dict(properties={
                "int": fields.Integer()
            })
        }, attrs_enabled=False)
        self.assertEqual(test_object.mapping(),
                         {
                             "properties": {
                                 "text": {
                                     "type": "text",
                                     "fields": {
                                         "keyword": {
                                             "type": "keyword",
                                             "ignore_above": 256
                                         }
                                     }
                                 },
                                 "int": {
                                     "type": "integer"
                                 },
                                 "inner_dict": {
                                     "properties": {
                                         "int": {
                                             "type": "integer"
                                         }
                                     }
                                 }
                             }
                         })

    def test_mapping_of_attributes(self):
        test_object = fields.Integer()
        self.assertEqual(test_object.mapping(),
                         {
                             "properties": {
                                 "value": {
                                     "type": "integer"
                                 },
                                 "pretty_name": {
                                     "type": "text",
                                     "fields": {
                                         "keyword": {
                                             "type": "keyword",
                                             "ignore_above": 256
                                         }
                                     }
                                 },
                                 "content_type": {
                                     "type": "text",
                                     "fields": {
                                         "keyword": {
                                             "type": "keyword",
                                             "ignore_above": 256
                                         }
                                     }
                                 }
                             }
                         })