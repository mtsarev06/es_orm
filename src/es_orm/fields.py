import codecs
from copy import deepcopy
from typing import Union, Any
from datetime import datetime

from dateutil import parser
from elasticsearch_dsl.analysis import AnalysisBase

from .exceptions import ValidationException, InitializationRequired
from .utils import ConfigClass


class Field:
    """
    The main class which represents fields in a document.
    Implements validation, serialization, creating of mapping, working
    with SRC attributes.

    Attributes
    ----------
    ValidationLevel: type
        Stores all the possible values validation level can be set to.
    EMPTY_VALUES: list
        List of all the possible empty values, which shouldn't be put into
        the database. Fields with such values return None as a value instead.
    _default_value: object
        If this set and there is not value set to the field, it's returned
        as a value instead.
    _value_type: type
        Type which value of the field should have in python representation.
    _es_type: str
        Type of the field in Elasticsearch mapping.
    _required: bool
        If True, there will be a ValidationException during cleaning if
        the field value is not set.
    _validation_level: int
        One of the values defined in Field.ValidationLevels, which defines
        the level of strictness to the saving value.
    _attrs_enabled: bool
        If True, the SRC attributes (such as value, pretty_name, content_type)
        are added to the documents. Otherwise, uses direct values (in this
        case it doesn't differ from the elasticsearch_dsl Field).
    _extra_mapping: dict
        Some extra mapping which needs to be put into the index mapping.
    _analyzer: AnalysisBase
        Analyzer created using elasticsearch_dsl methods which has to be
        added to the field.
    _attrs: dict
        SRC attributes of the field (such as pretty_name, content_type etc.)
    _default_attrs: dict
        Default attributes of the field. They are not added to the document,
        but used by some services.
    """
    class ValidationLevel(ConfigClass):
        STRICT = 0
        WARNING = 1
        DISABLED = 2

    EMPTY_VALUES = [None, [], (), {}, set(), ""]

    value = None
    _default_value = None

    _value_type = object
    _es_type = "object"

    _required = False
    _validation_level = None
    _attrs_enabled = True
    _extra_mapping = {}
    _analyzer = None

    _attrs = {}
    _default_attrs = {
        "pretty_name": "Поле",
        "content_type": "text"
    }

    def __init__(self,
                 default: Any = None,
                 default_attrs: dict = None,
                 required: bool = False,
                 validation_level: Union[str, int] = ValidationLevel.STRICT,
                 attrs_enabled: bool = True,
                 extra_mapping: dict = None,
                 analyzer: AnalysisBase = None):
        """
        Parameters
        ----------
        default: Any
            Default value to be set if nothing provided.
        default_attrs: dict
            Default values of SRC attributes (like pretty_name, content_type)
        required: bool
            If True, there will be ValidationException during cleaning
            if no value provided.
        validation_level: Union[str, int]
            Strictness of validation of the field. It's better to use
            ValidationLevel constants in here.
        attrs_enabled: bool
            Whether SRC attributes should be used in this field.
        extra_mapping: dict
            Some extra mapping, which is being put alongside the 'type' key.
        analyzer: AnalysisBase
            Elasticsearch_dsl analyzer, which is being used in the field.
        """
        if not isinstance(validation_level, int):
            validation_level = str(validation_level)
            validation_level = getattr(self.ValidationLevel,
                                       validation_level, None)

        if validation_level not in self.ValidationLevel:
            raise ValueError(f'Please provide a valid validation level for '
                             f'the field {self.__class__} (available: '
                             f'{", ".join(self.ValidationLevel.keys())})')

        self._default_attrs = self._default_attrs.copy()
        if default_attrs:
            if not isinstance(default_attrs, dict):
                raise TypeError(f"Default attributes must be an instance of "
                                f"a dict type, but got {type(default_attrs)} "
                                f"instead.")
            self._default_attrs.update(default_attrs or {})

        self._attrs = {}
        self._extra_mapping = dict(self._extra_mapping)
        self._required = bool(required)
        self._validation_level = validation_level
        self._attrs_enabled = bool(attrs_enabled)
        self._default_value = default

        if analyzer:
            if not hasattr(analyzer, 'get_analysis_definition'):
                raise TypeError("Incorrect type of the analyzer.")
            self._analyzer = analyzer
            self._extra_mapping.update(analyzer=self._analyzer.to_dict())

        if extra_mapping and isinstance(extra_mapping, dict):
            self._extra_mapping.update(extra_mapping)

    def __getitem__(self, item):
        if item == "value":
            return self.value
        if item in self._attrs:
            return self._attrs[item]
        if item in self._default_attrs:
            return self._default_attrs[item]
        raise KeyError(f'There is no "{item}" attribute in the field {self}.')

    def __setitem__(self, key, value):
        if key == "value":
            self.value = value
        elif key in self._attrs or key in self._default_attrs:
            self.set_attrs({key: value})
        else:
            raise KeyError(f'There is no "{key}" attribute in the field {self}.')

    def __set_name__(self, owner, name):
        self._default_attrs['pretty_name'] = name

    def __eq__(self, other):
        if type(other) is not type(self):
            return False
        attrs_to_check = [name for name, obj in vars(Field).items()
                          if not callable(obj) and not name.startswith('__')]
        for attr in attrs_to_check:
            if getattr(self, attr) != getattr(other, attr):
                return False
        return True

    def __repr__(self):
        repr_dict = {
            "value": self.value,
            **self._attrs
        }
        result = ""
        for key, value in repr_dict.items():
            show_value = value
            if isinstance(value, str):
                show_value = f'"{value}"'
            result += f'"{key}": {show_value}, '
        return f'{self.__class__.__name__}(' \
               f'{{{result[:-2]}}}' \
               f')'

    def clean(self):
        """
        Main method used to validate the field. Used by users.
        It performs basic checks and implements ValidationLevel functionality,
        though the main validation is performed in another method (_clean).

        Raises
        -------
        es_orm.exceptions.ValidationException
            If the validation level is set to STRICT and some of fields
            can't properly validate it's values.
        """
        if self.value in Field.EMPTY_VALUES:
            if self._required:
                raise ValidationException('В данном поле ожидается значение, '
                                          'но оно осталось пустым.')
            self.value = None
            return
        try:
            self._clean()
        except ValidationException as error:
            if self._validation_level == self.ValidationLevel.STRICT:
                raise error
            elif self._validation_level == self.ValidationLevel.WARNING:
                self._attrs['warning'] = str(error)

    def _clean(self):
        """
        The main method validating the value of the field.
        Also converts the value to the proper Python form.

        Raises
        -------
        es_orm.exceptions.ValidationException
            If the validation level is set to STRICT and some of fields
            can't properly validate it's values.
        """
        data = self.value
        if not isinstance(data, self._value_type):
            try:
                data = self._deserialize(data)
            except Exception as error:
                raise ValidationException(f"Не удалось перобразовать "
                                          f"значение поля типа "
                                          f"{type(self.value)} к "
                                          f"{self._value_type} ({error}).")
        self.value = data

    def deserialize(self, data: object, auto_set: bool = False):
        """
        Used by users to convert a value usually received from Elasticsearch
        to the proper form which can be set to the es_orm Fields.
        In fact it uses other method to deserialize value (_deserialize),
        but in it's turn it prepares data to get deserialized (removes
        unnecessary attrs before deserializing the immediate value, auto sets
        values to the field if necessary).

        Parameters
        ----------
        data
            Data to deserialize
        auto_set: bool
            Whether the method has to automatically set the deserialized value
            to the current Field object.

        Returns
        -------
        object
            Corresponding deserialized object.
        """
        data_to_deserialize = data
        if self._attrs_enabled:
            if not isinstance(data, dict) or 'value' not in data:
                data = {"value": data}
            data_to_deserialize = data['value']
        deserialized_data = self._deserialize(data_to_deserialize)
        if auto_set:
            self.set(deserialized_data)
        return deserialized_data

    def _deserialize(self, data):
        """
        The main method used to deserialize the value usually received
        from Elasticsearch database to the proper form which can be set
        to the es_orm Fields.

        Parameters
        ----------
        data
            Data (without attributes of any other things) which should be
            deserialized.

        Returns
        -------
        object
            Deserialized value.
        """
        if self._value_type is not object:
            return self._value_type(data)
        return str(data)

    def mapping(self):
        """
        Returns a dictionary representing the mapping of the field.
        It takes into account all the information associated with ORM
        (like attributes). In fact, the simple default mapping is
        defined in another method (_mapping), but this just serves as a
        wrapper over it.

        Returns
        -------
        dict
            Mapping for the current field.
        """
        result = {**self._mapping(), **self._extra_mapping}
        if self._attrs_enabled:
            result = {'properties': {'value': result}}
            for key in {**self._default_attrs, **self._attrs}:
                result['properties'].update({
                    key: {
                        'type': 'text',
                        'fields': {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    }
                })
        return result

    def _mapping(self):
        """
        Return the simple mapping (without attributes or so on) of the
        field, as if it was a default field of Elasticsearch.

        Returns
        -------
        dict
            Simple mapping for the current field.
        """
        return {'type': self._es_type}

    def set(self, value=None):
        """
        Sets a value to the field.

        Parameters
        ----------
        value
            Object to set as a value to the current field.

        Returns
        -------
        object
            Value that has been set to the field.
        """
        new_value = value
        if self._attrs_enabled:
            if not isinstance(value, dict) or 'value' not in value:
                new_value = value
            else:
                new_value = value.pop('value')
                self._attrs = value
        self.value = new_value
        return self

    def update(self, value=None):
        """
        Updates a value of the field. In fact, it behaves just like the set()
        method, but updates SRC attributes instead of setting in case of
        passing full-packed dictionary.

        Parameters
        ----------
        value
            Object to set as a value to the current field

        Returns
        -------
        object
            Value that has been set to the field.
        """
        if self._attrs_enabled:
            if isinstance(value, dict) and 'value' in value:
                value = {**self._attrs, **value}
        return self.set(value=value)

    def set_attrs(self, attrs: dict = None, include_undefined: bool = False,
                  **kwargs):
        """
        Sets SRC attributes for the current field (like pretty_name,
        content_type etc.) Every attribute created as a Text() field with
        keyword subfield.

        Parameters
        ----------
        attrs: dict
            Attributes to set.
        include_undefined: bool
            Whether to add new attributes not defined in class or not.
        kwargs
            Also attributes but passed in another way.

        """
        if attrs and not isinstance(attrs, dict):
            raise TypeError(f"Attributes must be declared using dictionaries, "
                            f"but got {type(attrs)} instead.")
        if not attrs:
            attrs = {}
        attrs.update(kwargs)
        attrs.pop('value', None)
        if include_undefined:
            self._attrs = attrs
        else:
            result = {}
            for attr_name in [*self._attrs.keys(), *self._default_attrs.keys()]:
                if attr_name in attrs:
                    result.update({attr_name: attrs[attr_name]})
            self._attrs = result

    def update_attrs(self, attrs: dict = None, include_undefined: bool = False,
                     **kwargs):
        """
        Updates SRC attributes for the current field (like pretty_name,
        content_type etc.).

        Parameters
        ----------
        attrs: dict
            Attributes to set.
        include_undefined: bool
            Whether to add new attributes not defined in class or not.
        kwargs
            Also attributes but passed in another way.

        """
        if not attrs:
            attrs = {}
        return self.set_attrs({**self._attrs, **attrs},
                              include_undefined=include_undefined, **kwargs)

    def serialize(self, include_all_attrs: bool = False):
        """
        Serializes the value of the current field. In fact, it's a wrapper
        over another method (_serialize) which adds an ability to implement
        SRC ORM functionality (such as attributes).

        Parameters
        ----------
        include_all_attrs: bool
            Whether the method should also add the default attributes
            to the resulting dict if not explicitly set by user.

        Returns
        -------
        dict
            Serialized value of the field.
        """
        value = self._serialize()
        if value in Field.EMPTY_VALUES:
            if self._default_value in Field.EMPTY_VALUES:
                return None
            value = self._default_value
        if self._attrs_enabled:
            if include_all_attrs:
                return {'value': value, **self._default_attrs, **self._attrs}
            else:
                return {'value': value, **self._attrs}
        return value

    def _serialize(self):
        """
        Serializes the field value so that it can be put into Elasticsearch.

        Returns
        -------
        object
            Serialized value ready to be put into Elasticsearch.
        """
        return self.value

    def to_dict(self, include_all_attrs: bool = False):
        return self.serialize(include_all_attrs)


class Dict(Field):
    """
    Implements a dict functionality as well as an ability to work with
    SRC attributes of the dict properties.
    To access a value of the field, you can use attribute getters:
        > Dict().some_integer_field
        123
    Or you can access the direct property object by using square brackes:
        > Dict()['some_integer_field']
        Integer({'value': 123, 'pretty_name': 'Some integer field'})

    Attributes
    ----------
    _properties: dict
        Dictionary of Field objects representing properties
        of the current Dict.
    _inner_attrs_enabled: bool
        If False, attributes of the Field objects are disabled by default.
    __is_properties_defined: bool
        If False, the properties of the Dict object wasn't defined during
        initialization, thus they will be added dynamically during setting.
    """
    _value_type = dict
    _es_type = "object"

    _properties = {}
    _inner_attrs_enabled = False
    __is_properties_defined = True

    def __init__(self, properties: dict = None,
                 inner_attrs_enabled: bool = False, **kwargs):
        self._properties = {}
        self._inner_attrs_enabled = bool(inner_attrs_enabled)

        if properties not in Field.EMPTY_VALUES:
            if not isinstance(properties, dict):
                raise TypeError(f"Properties of the Dict object must be "
                                f"a dict of Field objects, but got "
                                f"{type(properties)} instead.")
            self._properties = {name: properties[name] for name in properties
                                if isinstance(properties[name], Field)}
            if not self._inner_attrs_enabled:
                for property_object in self._properties.values():
                    property_object._attrs_enabled = False

        if not self._properties:
            self.__is_properties_defined = False
        super().__init__(**kwargs)

    def __getattr__(self, item):
        if item not in self._properties:
            if item == 'value' or (item.startswith('_') and item in dir(self)):
                return super().__getattribute__(item)
            raise AttributeError(f"There is no property {item} "
                                 f"in the dict.")
        if isinstance(self._properties[item], type(self)):
            return self._properties[item]
        return self._properties[item].value

    def __setattr__(self, key, value):
        if key == "value" or (key.startswith('_') and key in dir(self)):
            return super().__setattr__(key, value)
        if self._properties and key in self._properties:
            return self._properties[key].set(value)
        self.__add_undefined_property(key, value)

    def __getitem__(self, item):
        return self._properties[item]

    def __setitem__(self, key, value):
        return self._properties[key].set(value)

    def __iter__(self):
        for property_name in self._properties.keys():
            yield property_name

    def __repr__(self):
        repr_dict = {
            **self._attrs
        }
        result = '"value": {...}, '
        for key, value in repr_dict.items():
            show_value = value
            if isinstance(value, str):
                show_value = f'"{value}"'
            result += f'"{key}": {show_value}, '
        return f'{self.__class__.__name__}(' \
               f'{{{result[:-2]}}}' \
               f')'

    @property
    def value(self):
        result = {}
        for property_name, property_object in self._properties.items():
            if property_object.value not in Field.EMPTY_VALUES:
                result.update({property_name: property_object.serialize()})
        return result

    @value.setter
    def value(self, value):
        if value and not isinstance(value, dict):
            raise TypeError(f"Value must be an instance of dict type, but "
                            f"got {type(value)}")
        new_value = {property_name: None for property_name in self._properties}
        new_value.update(value or {})
        self.update(new_value, include_undefined=True)

    def _mapping(self):
        result = {}
        for property_name, property_object in self._properties.items():
            result[property_name] = property_object.mapping()
        return {'properties': result}

    def set(self, value: dict = None, include_undefined: bool = False,
            **kwargs):
        if not self.__is_properties_defined:
            include_undefined = True
        if value and not isinstance(value, dict):
            raise TypeError(f"Value must be an instance of dict type, but "
                            f"got {type(value)}")
        if value and not include_undefined:
            new_value = value
            if self._attrs_enabled and 'value' in value:
                new_value = value['value']
            for property_name in list(new_value.keys()):
                if property_name not in self._properties:
                    new_value.pop(property_name)
        super().set(value)
        self.update(include_undefined=include_undefined, **kwargs)

    def update(self, value: dict = None, include_undefined: bool = False,
               **kwargs):
        """
        Updates the values of the dictionary.

        Parameters
        ----------
        value: dict
            New values of the dictionary properties.
        include_undefined: bool
            If False, only defined properties wiil be set.
            If True, undefined properties will be created as Text() objects.
        kwargs
            Also new values, but passed in another way.

        """
        if not value:
            value = {}
        if not isinstance(value, dict):
            raise TypeError(f"Update method can only work with dictionaries, "
                            f"but got {type(value)} instead.")
        value.update(kwargs)

        if not self.__is_properties_defined:
            include_undefined = True

        for property_name, property_value in value.items():
            if property_name in self._properties:
                self._properties[property_name].set(property_value)
            elif include_undefined:
                self.__add_undefined_property(property_name, property_value)

    def clear(self):
        """ Remove all items from D. """
        for property_name, property_object in self._properties.items():
            property_object.set(None)

    def get(self, key: str, *args):
        """
        Retrieves a value by the key from the dictionary.

        Parameters
        ----------
        key: str
            Key to retrieve a value.
        args
            Default value to return if the key doesn't exist.

        Returns
        -------
        object
            Value or the default value (if provided and key doesn't exist)
        """
        if key not in self._properties:
            if len(args):
                return args[0]
            raise KeyError(f"There is no key {key} in the dictionary "
                           f"object {self}.")
        return self._properties[key].serialize()

    def items(self):
        """Returns generator through the (key, value) items of the dict."""
        for property_name, property_object in self._properties.items():
            yield property_name, property_object.serialize()

    def keys(self):
        """Returns generator through the keys of the dictionary."""
        return self._properties.keys()

    def pop(self, key: str, *args):
        """
        Fetches the value by the key from the dictionary.

        Parameters
        ----------
        key: str
            Key to fetch the value.
        args
            Default value to return if the value with the passed key
            doesn't exist.

        Returns
        -------
        object
            Fetched value or the default value (if provided and key
            doesn't exist)
        """
        result = self._properties.get(key, *args)
        if result is None and not args:
            raise KeyError(f'There is no key {key} in the Dict object {self}.')
        if isinstance(result, Field):
            value = result.value
            self._properties[key].set(None)
            return value
        return result

    def values(self):
        """Returns generator through the values of the dictionary."""
        for property_object in self._properties.values():
            yield property_object.value

    def _clean(self):
        error_message = ""
        for property_name, property_object in self._properties.items():
            try:
                property_object.clean()
            except ValidationException as error:
                error_message += f"Поле {property_name} не было " \
                                 f"успешно валидировано: {error}. "
        if error_message:
            raise ValidationException(error_message)

    def _deserialize(self, data: dict):
        if not data:
            return None
        for key, value in data.items():
            if key not in self._properties:
                continue
            data[key] = self._properties[key].deserialize(value)
        return data

    def __add_undefined_property(self, key: str, value=None):
        """
        Adds a property which hasn't been defined during Dict initialization.

        Parameters
        ----------
        key: str
            Key by which the value will be available.
        value
            Value to set to the newly created property.
        """
        if isinstance(value, dict) and 'value' not in value:
            new_property = type(self)(attrs_enabled=self._inner_attrs_enabled)
            new_property.set(value, include_undefined=True)
        else:
            new_property = Text(attrs_enabled=self._inner_attrs_enabled)
            new_property.set(value)
        self._properties[key] = new_property


class Date(Field):
    """
    Field for storing dates.
    During cleaning, it tries to convert stored value to the datetime object.
    By default, it can only perceive ISO format of strings, but
    you can adjust it if necessary.

    Attributes
    ----------
    _date_format: str
        Format of date which is put into the Field object. It doesn't affect
        the format of date in Elasticsearch.
    """
    _value_type = datetime
    _es_type = "date"

    _date_format = None

    def __init__(self, date_format: str = None, *args, **kwargs):
        if date_format:
            datetime.now().strftime(date_format)
            self._date_format = date_format
        super().__init__(*args, **kwargs)

    def _serialize(self):
        if not isinstance(self.value, datetime):
            return str(self.value)
        return self.value.isoformat()

    def _deserialize(self, data):
        if isinstance(data, int):
            return datetime.fromtimestamp(data)
        if self._date_format:
            data = datetime.strptime(str(data), self._date_format)
        else:
            data = parser.parse(str(data))
        return data


class Choices(Field):
    """
    Implements an ability to restrict allowed for storing values to some
    limited set.

    Attributes
    ----------
    _field_class: Field
        Field class used to create mapping in Elasticsearch.
    _choices: list
        List of values which are allowed to put into the database.
    """
    _value_type = object
    _es_type = "text"

    _field_class = None
    _choices = []

    def __init__(self, choices: Union[list, tuple, set],
                 field_class: Field = None, *args, **kwargs):
        if not isinstance(choices, (list, tuple, set)):
            raise TypeError("Choices must be an instance of list,"
                            "tuple or set type.")
        if not field_class:
            field_class = Text
        if isinstance(field_class, type):
            field_class = field_class()

        self._field_class = field_class
        self._choices = choices
        super().__init__(*args, **kwargs)

    def _clean(self):
        if self.value not in self._choices:
            raise ValidationException('Значение поля не находится в списке '
                                      'допустимых.')

    def _mapping(self):
        return self._field_class._mapping()


class List(Field):
    """
    Used for storing lists of values.
    In the essence of mapping in Elasticsearch there is no such thing
    as lists, as you can store either a value or an array of values, but
    we can check all the necessary things ourselves.

    Attributes
    ----------
    _element_object: Field
        Object used for validating elements and creating mapping for an index.
    """
    _value_type = list
    _es_type = None

    _element_object: Field = None

    def __init__(self, element_object: Union[Field, type] = None, *args,
                 **kwargs):
        if not element_object:
            element_object = Text
        if isinstance(element_object, type):
            element_object = element_object()
        if not isinstance(element_object, Field):
            raise TypeError("Elements of the List class must be an instance "
                            "of the Field class.")
        self._element_object = element_object
        self._element_object._validation_level = Field.ValidationLevel.STRICT
        self._es_type = element_object._es_type
        super().__init__(*args, **kwargs)

    def _clean(self):
        if not isinstance(self.value, list):
            raise ValidationException("Значение поля не является списком.")
        element_object = deepcopy(self._element_object)
        for i, element in enumerate(self.value):
            try:
                element_object.set(element)
            except Exception as error:
                raise ValidationException(f"Не удалось проверить элемент "
                                          f"{element} списка: {error}.")
            try:
                element_object.clean()
                self.value[i] = element_object.value
            except ValidationException as error:
                raise ValidationException(f"Не удалось валидировать элемент "
                                          f"{element} списка: {error}.")

    def _mapping(self):
        return self._element_object._mapping()

    def _serialize(self):
        if not isinstance(self.value, list):
            return str(self.value)

        element_object = deepcopy(self._element_object)
        result = []
        for element in self.value:
            element_object.set(element)
            result.append(element_object._serialize())

        return result


class ConvertedDict(Field):
    """
    In case when there is no ability to assess the amount of properties in
    dictionary (i.e. when you can't define properties of a dictionary in
    advance), you'd better use this class.
    It's because if the amount of properties continuously raises, it leads
    to the thing called "Mapping explosion" (better google it, if you don't
    know what it is).

    Attributes
    ----------
    _properties: dict
        The mapping to be put into Elasticsearch.
    __depth: int
        The depth of dictionary which currently can be put into Elasticsearch.
    """
    _value_type = dict
    _es_type = "object"

    _properties = {}
    __depth = 0

    def __init__(self, *args, **kwargs):
        self.__update_mapping(1)
        super().__init__(*args, **kwargs)

    def _clean(self):
        super()._clean()
        if not isinstance(self.value, dict):
            raise ValidationException('Значение поля не является словарем.')
        is_mapping_changed = self.__update_mapping()
        if is_mapping_changed:
            raise InitializationRequired()

    def _serialize(self):
        if isinstance(self.value, dict):
            return self.__convert_dict(self.value)
        return {'value': str(self.value)}

    def deserialize(self, data: object, auto_set: bool = False):
        result = super().deserialize(data, auto_set)
        if auto_set:
            self.__update_mapping()
        return result

    def _deserialize(self, data: list):
        return self.__deconvert_dict(data)

    def _mapping(self):
        return self._properties.mapping()

    def __convert_dict(self, data: dict):
        """
        Converts a dict to the form in that doesn't leads
        to the mapping explosion. Recursive.

        Parameters
        ----------
        data: dict
            Dictionary to convert.

        Returns
        -------
        list
            Converted dictionary.
        """
        new_data = []
        for key, value in data.items():
            if isinstance(value, dict):
                value = self.__convert_dict(value)
                new_data.append({'key': key, 'inner_dict': value})
            else:
                new_data.append({'key': key, 'value': value})
        return new_data

    def __deconvert_dict(self, data: list) -> dict:
        """
        Deconverts a dict from the form which doesn't cause mapping explosion.
        Recursive.

        Parameters
        ----------
        data: list
            Dict that was converted using __convert_dict before.

        Returns
        ----------
        dict
            Deconverted dictionary.
        """
        if not data or not isinstance(data, list):
            return data if not isinstance(data, dict) or \
                           'value' not in data else data['value']
        result = {}
        for pair in data:
            if ('value' in pair and 'inner_dict' in pair) or \
                    ('value' not in pair and 'inner_dict' not in pair) \
                    or 'key' not in pair:
                raise ValueError('Incorrect structure of converted dict.')
            if 'value' in pair:
                result[pair['key']] = pair['value']
            elif 'inner_dict' in pair:
                result[pair['key']] = self.__deconvert_dict(pair['inner_dict'])
        return result

    def __determine_depth(self, data: dict) -> int:
        """
        Returns the depth of the passed dictionary.
        """
        if isinstance(data, dict) and data:
            return 1 + (max(map(self.__determine_depth, data.values())) if data else 0)
        return 0

    def __update_mapping(self, depth: int = None):
        """
        Updates index mapping in accordance with the depth of current dict.

        Parameters
        ----------
        depth: int
            The depth of new dictionary to make a mapping for. If not passed,
            the depth of current value is calculated.

        Returns
        ----------
        bool
            True, if the mapping was updated, and False otherwise.
        """
        current_depth = depth
        if not depth:
            current_depth = self.__determine_depth(self.value)
        if self.__depth >= current_depth:
            return False

        new_mapping = {
            "key": Text(),
            "value": Text(),
        }
        for i in range(current_depth-1):
            new_mapping = {
                "key": Text(),
                "value": Text(),
                "inner_dict": Dict(properties=new_mapping,
                                   attrs_enabled=False),
            }
        self._properties = Dict(properties=new_mapping, attrs_enabled=False)
        self.__depth = current_depth
        return True


class InnerDocument(Field):
    """
    Used to define dictionaries as classes.
    The definition of these classes doesn't differ from definition
    of Document classes, apart from defining the Index class inside.
    """
    @classmethod
    def to_field(cls, **kwargs):
        dict_properties = {}
        for attr in dir(cls):
            if isinstance(getattr(cls, attr), Field):
                dict_properties[attr] = getattr(cls, attr)
        return Dict(properties=dict_properties, **kwargs)

    def __new__(cls, *args, **kwargs):
        return cls.to_field(**kwargs)


class DocumentDict(Dict):
    """
    Converts Document object to the Dict object.

    Attributes
    ----------
    _document_class: Document
        Document class to convert into the Dict object.
    """
    _document_class = None

    def __init__(self, document_class=None, properties: dict = None, **kwargs):
        if properties and not isinstance(properties, dict):
            raise TypeError('Properties must be an instance of the dict type.')
        combined_properties = {}
        if document_class:
            combined_properties.update(deepcopy(document_class._fields_dict))
        combined_properties.update(properties or {})
        self._document_class = document_class
        super().__init__(properties=combined_properties, **kwargs)

    def set(self, value: dict = None, include_undefined: bool = False,
            **kwargs):
        if isinstance(value, self._document_class):
            value = value.serialize()
        return super().set(value, include_undefined, **kwargs)


class Bytes(Field):
    """
    Used for storing bytes in Elasticsearch.
    Under the hood it converts input bytes into a string using ASCII encoding,
    omitting all unencodable bytes and leaving them in \xFF representation.
    """
    _value_type = bytes
    _es_type = "text"

    def _serialize(self):
        data = self.value
        if not isinstance(data, bytes):
            data = str(data)
        if isinstance(data, str):
            data = data.encode("ascii", "backslashreplace")
        return data.decode("ascii", "backslashreplace")

    def _deserialize(self, data):
        return codecs.escape_decode(data)[0]


class ForeignKey(Field):
    """
    Represents ForeignKey, which doesn't actually exist in Elasticsearch.
    Actually it saves a plain text in DB, which is interpreted as ID of
    another document with child class of the Document type.

    Attributes
    ----------
    _foreign_doc_type: Document
        Document which is considered as Foreign key.
    _ref_property: str
        Name of a property which could be used in Document object to retrieve
        an associated document object of the _foreign_doc_type type.
    __foreign_key_object: Document
        Document object associated with another foreign document,
        in another index.
    """
    _value_type = str
    _es_type = "text"

    _foreign_doc_type = None
    _ref_property = None
    __foreign_key_object = None

    def __init__(self, foreign_doc_type, ref_property: str = None,
                 *args, **kwargs):
        from . import Document

        if not issubclass(foreign_doc_type, Document):
            raise ValueError(f"Foreign key has to point on a Document object.")

        self._foreign_doc_type = foreign_doc_type
        self._ref_property = str(ref_property)
        super().__init__(*args, **kwargs)

    def __set_name__(self, owner, name):
        super().__set_name__(owner, name)

        if self._ref_property:
            @property
            def ref_document_property(class_self):
                return class_self[name].ref_document
            setattr(owner, self._ref_property, ref_document_property)

    @property
    def ref_document(self):
        """
        Returns associated foreign document with the current ID set in value.
        """
        foreign_key_id = self.value
        if not foreign_key_id:
            raise ValueError(
                f"You have to set foreign key id before accessing it.")
        if not hasattr(self, '__foreign_key_object') or \
                not self.__foreign_key_object or  \
                self.__foreign_key_object.meta['id'] != foreign_key_id:
            self.__foreign_key_object = self._foreign_doc_type(
                meta={'id': foreign_key_id})
            if self.__foreign_key_object.exists():
                self.__foreign_key_object = self._foreign_doc_type().get(
                    id=foreign_key_id)
        return self.__foreign_key_object


class Keyword(Field):
    _value_type = str
    _es_type = "keyword"

    _extra_mapping = {"ignore_above": 256}


class Text(Field):
    _value_type = str
    _es_type = "text"

    def _mapping(self):
        return {
            'type': self._es_type,
            "fields": {
                "keyword": {"type": "keyword", "ignore_above": 256}
            }
        }


class Integer(Field):
    _value_type = int
    _es_type = "integer"


class Long(Field):
    _value_type = int
    _es_type = "long"


class Short(Field):
    _value_type = int
    _es_type = "short"


class Boolean(Field):
    _value_type = bool
    _es_type = "boolean"


class Float(Field):
    _value_type = float
    _es_type = "float"


class Double(Field):
    _value_type = float
    _es_type = "double"


class Wildcard(Field):
    _value_type = str
    _es_type = "wildcard"
