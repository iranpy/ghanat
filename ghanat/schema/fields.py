import types

import pandas as pd
from pyspark.sql.types import (
    StringType,
    IntegerType,
    LongType,
    FloatType,
    BooleanType,
    TimestampType,
    DateType,
)


def _coalesce(column, value1, value2):
    return column.replace(value1, value2)


class Field:
    getter_takes_serializer = False
    _dtype = None
    _spark_type = None
    _load_pass = False

    def __init__(self, attr=None, call=False, label=None, required=True, **kwargs):
        self.attr = attr
        self.call = call
        self.label = label
        self.required = required
        self.coalesce = kwargs.get("coalesce")
        self.extra = kwargs

    def to_value(self, col):
        return col.astype(self._dtype)

    def loads(self, col):
        return col

    to_value._base = True

    def _is_to_value_overridden(self):
        to_value = self.to_value
        # If to_value isn't a method, it must have been overridden.
        if not isinstance(to_value, types.MethodType):
            return True
        if isinstance(to_value, types.MethodType) and self._load_pass:
            return True
        return not getattr(to_value, "_base", False)

    def as_getter(self, serializer_field_name, serializer_cls):
        return None


class StrField(Field):
    """A :class:` String Field` that converts the column to a string."""

    _dtype = "object"
    _spark_type = StringType()

    def to_value(self, col):
        if self.coalesce:
            return _coalesce(col.astype("str"), *self.coalesce)
        return col.astype("str")


class LongField(Field):
    """A :class:`Long Field` that converts the column to an long."""

    _dtype = "int64"
    _spark_type = LongType()

    def to_value(self, col):
        return col.astype("int64")


class IntField(Field):
    """A :class:`Integer Field` that converts the column to an integer."""

    _dtype = "int32"
    _spark_type = IntegerType()

    def to_value(self, col):
        return col.astype("int32")


class FloatField(Field):
    """A :class:`Float Field` that converts the value to a float."""

    _dtype = "float64"
    _spark_type = FloatType()


class BoolField(Field):
    """A :class:`Bool Field` that converts the value to a boolean."""

    _dtype = "bool"
    _spark_type = BooleanType()


class DatetimeField(Field):
    _dtype = "datetime64[ns]"
    _spark_type = TimestampType()

    def to_value(self, value):
        return pd.to_datetime(value)

    def loads(self, value):
        return pd.to_datetime(value)


class DateField(Field):
    _dtype = "datetime64[ns]"
    _spark_type = DateType()

    def to_value(self, value):
        # return value
        return pd.to_datetime(value).dt.date

    def loads(self, value):
        return pd.to_datetime(value).dt.date


class MethodField(Field):
    """A :class:`Method Field` that calls a method on the :class:`Serializer`.

    This is useful if a :class:`Field` needs to serialize a value that may come
    from multiple attributes on an object. For example: ::

        class SampleSerializer(Serializer):
            foo = IntField()
            bar = StrField()
            plus = MethodField('do_plus', return_type=IntType(), label='is_holiday')

            @staticmethod
            def do_plus(df_obj):
                return df_obj.foo - df_obj.bar

        df =
            +----+-----+
            |  foo| bar|
            +----+-----+
            |  5 |scrat|
            |  3 |spark|
            +----+-----+

        SampleSerializer(df).dataframe
        +----+---+------+
        |  foo| bar|plus|
        +----+---+------+
        |  5 |scrat| 10 |
        |  3 |spark|  6 |
        +----+---+------+

    :param str method: The method on the serializer to call. Defaults to
        ``'get_<field name>'``.
    """

    _dtype = "string"
    _spark_type = StringType()

    getter_takes_serializer = True
    _load_pass = True

    def __init__(self, method, return_type, **kwargs):
        super(MethodField, self).__init__(**kwargs)
        self.method = method
        self._spark_type = return_type

    def as_getter(self, serializer_field_name, serializer_cls):
        method_name = self.method
        return getattr(serializer_cls, method_name)

    def loads(self, value):
        return value.astype("str")
