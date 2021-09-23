import operator
import six

import pandas as pd
from pyspark.sql.types import StructField, StructType

from ghanat.schema.fields import Field


def _compile_field(field, name, serializer_cls):
    getter = field.as_getter(name, serializer_cls)
    if getter is None:
        getter = serializer_cls.default_getter(field.attr or name)

    to_value = None
    if field._is_to_value_overridden():
        to_value = field.to_value
    # Set the field name to a supplied label; defaults to the attribute name.
    name = field.label or name
    spark_type = field._spark_type
    fields_with_schema = {
        "type": StructField(name, spark_type),
        "catalog": (
            name,
            getter,
            to_value,
            field.required,
            field.getter_takes_serializer,
        ),
    }
    return fields_with_schema


class SerializerBase(Field):
    _field_transformation_map = {}
    _field_load_map = {}


class SerializerMeta(type):
    @staticmethod
    def _get_fields(direct_fields, serializer_cls, field_state):
        field_map = {}
        # Get all the fields from base classes.
        for cls in serializer_cls.__mro__[::-1]:
            if issubclass(cls, SerializerBase):
                field_map.update(getattr(cls, field_state))
        field_map.update(direct_fields)
        return field_map

    @staticmethod
    def _compile_fields_with_schema(field_map, serializer_cls):
        fields = []
        schema = []
        for name, field in field_map.items():
            compiled_field_schema = _compile_field(field, name, serializer_cls)
            fields.append(compiled_field_schema["catalog"])
            schema.append(compiled_field_schema["type"])
        return dict(fields=fields, schema=StructType(schema))

    @staticmethod
    def _empty_schema_generator(field_map):
        empty_schema = {}
        for name, field in field_map.items():
            empty_schema[field.label or name] = pd.Series([], dtype=field._dtype)
        return pd.DataFrame(empty_schema)

    @staticmethod
    def filter_fields(spec_fields, fields_name):
        return { field_name: spec_fields[field_name] for field_name in fields_name }

    def __new__(cls, name, bases, attrs):
        # Fields declared directly on the class.
        direct_fields = {}
        # Take all the Fields from the attributes.
        for attr_name, field in attrs.items():
            if isinstance(field, Field):
                direct_fields[attr_name] = field

        for k in direct_fields.keys():
            del attrs[k]

        real_cls = super(SerializerMeta, cls).__new__(cls, name, bases, attrs)
        meta = attrs.get('Meta')
        transformation_fieldname = list(getattr(meta, '__transformation__', direct_fields.keys()))
        load_fieldname = list(getattr(meta, '__load__', direct_fields.keys()))

        transformation_fields = real_cls.filter_fields(direct_fields, transformation_fieldname)
        load_fields = real_cls.filter_fields(direct_fields, load_fieldname)

        field_transformation_map = cls._get_fields(transformation_fields, real_cls, '_field_transformation_map')
        field_load_map = cls._get_fields(load_fields, real_cls, '_field_load_map')

        compiled_transformation_fields = cls._compile_fields_with_schema(field_transformation_map, real_cls)
        compiled_load_fields = cls._compile_fields_with_schema(field_load_map, real_cls)


        real_cls._empty_dataframe = cls._empty_schema_generator(field_transformation_map)
        real_cls._prep_func = attrs.get("prep_func")
        real_cls._aggr_func = attrs.get("aggr_func")
        real_cls._field_transformation_map = field_transformation_map
        real_cls._field_load_map = field_load_map

        real_cls._compiled_transformation_fields = compiled_transformation_fields["fields"]
        real_cls._transformation_fields_schema = compiled_transformation_fields["schema"]

        real_cls._compiled_load_fields = compiled_load_fields["fields"]
        real_cls.load_fields_schema = compiled_load_fields["schema"]
        return real_cls



class DataSchema(SerializerBase, metaclass=SerializerMeta,):
    default_getter = operator.attrgetter
    __spark_partition_keys__ = []
    __group_key__ = None

    def __init__(self, instance=None, **kwargs):
        super(DataSchema, self).__init__(**kwargs)
        self.instance = instance
        self._data = None
        self._obj = None
        if not isinstance(instance, pd.DataFrame):
            self.spark_df = True
            # TODO: handle __group_key__ if it's null
            if not self.__group_key__:
                raise ValueError("group key must not be None")
        else:
            self.spark_df = False

    def to_value(self, instance):
        fields = self._compiled_transformation_fields
        prep_func = self._prep_func
        aggr_func = self._aggr_func
        empty_dataframe = self._empty_dataframe

        def _serialize(instance):
            if prep_func is not None:
                instance = prep_func(instance)

            if aggr_func is not None:
                instance = aggr_func(instance)
            # TODO return empty dataframe with column names
            if instance.empty:
                return empty_dataframe
            df = {}
            for (
                name,
                getter,
                to_value,
                required,
                pass_self,
            ) in fields:
                if pass_self:
                    result = getter(instance)
                else:
                    try:
                        result = getter(instance)
                    except (KeyError, AttributeError):
                        if required:
                            raise
                        else:
                            continue
                    if required or result is not None:
                        if to_value:
                            result = to_value(result)
                if isinstance(result, pd.DataFrame):
                    df.update(result.to_dict("series"))
                    instance.update(result.to_dict("series"))
                else:
                    df[name] = result
                    instance[name] = result
            dataframe = pd.concat(df.values(), keys=df.keys(), axis=1)
            return dataframe

        if self.spark_df:
            fn_df = instance.groupBy(self.__group_key__).applyInPandas(
                _serialize, schema=self._transformation_fields_schema
            )
            return fn_df

        fn_df = _serialize(instance)
        return fn_df

    @property
    def dumps(self):
        """Get the serialized data from the :class:`Serializer`."""
        # Cache the data for next time .data is called.
        if self._data is None:
            self._data = self.to_value(self.instance)
        return self._data

    def to_hive(self):
        pass

    def object_builder(self):
        return self

    @property
    def df(self):
        return self._data


    @property
    def loads(self):
        """Get the serialized data from the :class:`Serializer`.

        The data will be cached for future accesses.
        """
        # Cache the data for next time .data is called.
        if self._data is None:
            self._data = self._loads(self.instance)
        return self.object_builder()
