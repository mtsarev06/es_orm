"""
This package gives an ability to work with Elastisearch
with high customizability.

Features:
    Special data structure: each field value is a dictionary,
    containing at least one key: "value".
    This data structure doesn't affect interaction with
    elasticsearch_dsl objects. Thus:
        my_document = MyIndex(my_field="field_value")
        OR
        my_document.my_field = "field_value"
    will turn into:
        "my_field": {
            "value": "field_value",
            "pretty_name": "Human-readable representation of the field name",
            "content_type": "text"
        }

    Validation: you can set different validation levels to fields.
        STRICT: set mapping in elasticsearch index to a chosen value type.
            No other types are allowed in the field.
        WARNING: set mapping in elasticsearch index to 'Text', thus making
            it possible to put value of any type there. Also adds an extra
            field called "warning", which is added if there is any error
            during validation.
        DISABLED: set mapping in elasticsearch index to 'Text'.
            Omits any validation errors.

    Author: Mikhail Tsarev
"""
import json
from typing import Union
from copy import deepcopy
from datetime import datetime

from elasticsearch import helpers, Elasticsearch, client
from elasticsearch.exceptions import NotFoundError
from elasticsearch_dsl import (mapping, Field as DslField,
                               Document as DslDocument, connections)
from elasticsearch_dsl.utils import DOC_META_FIELDS
from elasticsearch_dsl.document import IndexMeta

from .fields import Field, Dict, DocumentDict, Date
from .exceptions import InitializationRequired, ValidationException


class AdvancedIndexMeta(IndexMeta):
    """
    Metaclass which expands the default elasticsearch_dsl metaclass.
    The crucial thing it does is making it possible to use es_orm Fields
    instead of the elasticsearch_dsl ones (also preventing of using them).
    """
    def __new__(cls, name, bases, attrs):
        orm_fields = {}
        new_attrs = {}
        for base in bases[::-1]:
            if hasattr(base, '_fields_dict'):
                attrs.update(base._fields_dict)
        for attr in attrs:
            if isinstance(attrs[attr], DslField):
                raise TypeError(f"Please don't use elasticsearch_dsl fields"
                                f"(you use it on the {attr} field). "
                                f"Use es_orm.fields classes instead.")
            elif isinstance(attrs[attr], Field):
                orm_fields.update({attr: attrs[attr]})
            else:
                new_attrs[attr] = attrs[attr]
        result = super().__new__(cls, name, bases, new_attrs)
        if result.timestamp_enabled():
            orm_fields['timestamp'] = Date()
        for field_name, field_object in orm_fields.items():
            field_object.__set_name__(result, field_name)

        inner_attrs_enabled = True
        if hasattr(result, 'Index') and hasattr(result.Index, 'attrs_enabled') \
                and not result.Index.attrs_enabled:
            inner_attrs_enabled = False

        result._fields_dict = Dict(properties=orm_fields,
                                   inner_attrs_enabled=inner_attrs_enabled,
                                   attrs_enabled=False)
        doc_mapping = result._doc_type.mapping
        doc_mapping.properties = mapping.Properties()
        doc_mapping._update_from_dict(result._fields_dict.mapping())

        for field_object in orm_fields.values():
            if field_object._analyzer:
                result._index.analyzer(field_object._analyzer)
        return result


class Document(DslDocument, metaclass=AdvancedIndexMeta):
    """
    The main class used to create models of Elasticsearch documents.
    Inherits from the default elasticsearch_dsl Document, thus making it
    possible to use all the functionality it implements and some more.
    """
    def __init__(self, json_data: Union[str, dict] = None, meta: dict = None,
                 **kwargs):
        self._fields_dict = deepcopy(self._fields_dict)

        if json_data:
            if not isinstance(json_data, dict):
                json_data = json.loads(json_data)
            if json_data:
                kwargs.update(json_data)

        self._fields_dict.set(kwargs)
        super().__init__(meta=meta)

    def __getattr__(self, item):
        if item not in self._fields_dict:
            if item in dir(self):
                return object.__getattribute__(self, item)
            raise AttributeError(f"There is no field {item} "
                                 f"in the Document of type {type(self)}.")
        return getattr(self._fields_dict, item)

    def __setattr__(self, key, value):
        if key.startswith('_') and key in dir(self):
            return object.__setattr__(self, key, value)
        setattr(self._fields_dict, key, value)

    def __getitem__(self, item):
        return self._fields_dict[item]

    def __setitem__(self, key, value):
        self._fields_dict[key] = value

    def __iter__(self):
        return self._fields_dict.__iter__()

    def __dir__(self):
        return object.__dir__(self)

    def clean(self):
        """Validates all the fields of the document."""
        try:
            return self._fields_dict.clean()
        except InitializationRequired:
            self.init(self._get_index())
            return self._fields_dict.clean()

    def full_clean(self):
        return self.clean()

    def exists(self):
        """Checks whether current document already exists in the database."""
        if 'id' not in self.meta or not self.meta['id']:
            return False
        try:
            return bool(self.search().from_dict({
                "size": 0,
                "query": {
                    "match": {
                        "_id": self.meta['id']
                    }
                }
            }).index(self._get_index()).count())
        except NotFoundError:
            return False

    def set(self, value: dict = None, include_undefined: bool = False,
            **kwargs):
        """
        Sets values of fields of the document to the passed ones.
        It's important to mention that values of fields that weren't passed
        are set to None.

        Parameters
        ----------
        value: dict
            Dictionary with values to set to the document fields.
        include_undefined: bool
            If False, only fields defined in the model are set.
            If True, creates Text() fields for every undefined key.
        """
        return self._fields_dict.set(value, include_undefined, **kwargs)

    def update(self, value: dict = None, include_undefined: bool = False,
               **kwargs):
        """
        Updates values of fields of the document to the passed ones.

        Parameters
        ----------
        value: dict
            Dictionary with values to set to the document fields.
        include_undefined: bool
            If False, only fields defined in the model are set.
            If True, creates Text() fields for every undefined key.
        """
        return self._fields_dict.update(value, include_undefined, **kwargs)

    def save(self, validate: bool = True, *args, **kwargs):
        """
        Saves the current document to the database.

        Parameters
        ----------
        validate: bool
            Whether the method should perform validation before save.

        Returns
        -------
        str
            State of the document (created, updated).
        """
        if validate:
            try:
                self.clean()
            except InitializationRequired:
                self.init(index=self._get_index())
            else:
                validate = False
        if self.timestamp_enabled() and not self.timestamp:
            self.timestamp = datetime.now()

        return super().save(validate=validate, *args, **kwargs)

    def serialize(self, include_meta: bool = False):
        """
        Converts the document into dictionary which can be put
        into Elasticsearch.

        Parameters
        ----------
        include_meta: bool
            If True, extra meta information about the document (such as index)
            also added to the dictionary.

        Returns
        -------
        dict
            Document object dictionary representation.

        """
        result = self._fields_dict.serialize()
        if not include_meta:
            return result

        meta = {"_" + k: self.meta[k] for k in DOC_META_FIELDS if
                k in self.meta}

        index = self._get_index(required=False)
        if index is not None:
            meta["_index"] = index

        meta["_source"] = result
        return meta

    def to_dict(self, include_meta: bool = False, skip_empty: bool = True):
        return self.serialize(include_meta)

    @classmethod
    def bulk_save(cls, docs: list, forced: bool = False,
                  stats_only: bool = False,
                  using: Union[str, Elasticsearch] = "default"):
        """
        Performs bulk API request to save multiple objects of the model
        at once. Significantly improves saving speed to the db.

        Parameters
        ----------
        docs: list[es_orm.Document]
            List of Document objects to save in the DB.
        forced: bool
            Whether passed data should be tried to save to the DB even though
            it's not an instance of the current Model.
        stats_only: bool
            Whether the method should return just numbers representing amount
            of successful savings and failures or
            also should return a list of responses of failure requests.
        using: Union[str, Elasticsearch]
            Connection to use for saving data.

        Returns
        -------
        tuple[successful: int, failed: list]
            2 values: amount of successful saves and error list of
            unsuccessful actions.
        """
        es_client = connections.get_connection(using)
        bulk_data = []
        bulk_docs = []
        for doc in docs:
            doc_representation = str(doc)[:20] + "..." \
                if len(str(doc)) > 20 else str(doc)
            if not isinstance(doc, cls) and not forced:
                print(f"WARNING: {doc_representation} hasn't been saved, "
                      f"cause it's not an instance of the Document type.")
                continue
            try:
                doc.clean()
            except InitializationRequired:
                doc.init(index=doc._get_index())
                try:
                    doc.clean()
                except ValidationException as error:
                    print(f"WARNING: {doc_representation} hasn't been saved, "
                          f"cause it's invalid: {str(error)}")
                    continue
            except ValidationException as error:
                print(f"WARNING: {doc_representation} hasn't been saved, "
                      f"cause it's invalid: {str(error)}")
                continue

            if doc.timestamp_enabled() and not doc.timestamp:
                doc.timestamp = datetime.now()

            bulk_data.append(doc.to_dict(True))
            bulk_docs.append(doc)

        success, failed = 0, 0
        errors = []

        i = 0
        for result, item in helpers.streaming_bulk(es_client, bulk_data):
            # go through request-response pairs and detect failures
            if not result:
                if not stats_only:
                    errors.append(item)
                failed += 1
            else:
                success += 1
                bulk_docs[i].meta['id'] = item['index']['_id']
                bulk_docs[i].meta['index'] = item['index']['_index']
            i += 1
        return success, failed if stats_only else errors

    @classmethod
    def to_field(cls,  **kwargs):
        """
        Transforms Documents into DocumentDict objects,
        so that they can be used as fields.

        Returns
        -------
        es_orm.fields.DocumentDict
            Dictionary representing the Document.
        """
        return DocumentDict(cls, **kwargs)

    @classmethod
    def timestamp_enabled(cls):
        return hasattr(cls, 'Index') and \
               hasattr(cls.Index, 'timestamp_enabled') and \
               cls.Index.timestamp_enabled

    def _from_dict(self, data):
        return self._fields_dict.deserialize(data, auto_set=True)


def connect(hosts: Union[str, list], **kwargs) -> client.Elasticsearch:
    """
    Allows to connect to elasticsearch DB without using elasticsearch_dsl.

    Parameters
    ----------
    hosts: list
        List of URLs to a server. If one is unreachable,
        it will try with the next one.

    """
    return connections.create_connection(hosts=hosts, **kwargs)
