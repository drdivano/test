# -*- coding: utf-8 -*-

"""ORM Schema.

$Id: schema.py 1002 2012-11-18 18:38:17Z anovgorodov $
"""

from datetime import datetime, date, time
from zope.interface import implements, implementsOnly, Interface
from zope.schema.fieldproperty import FieldProperty
from zope.schema._bootstrapinterfaces import RequiredMissing
import zope.schema
import zope.schema.interfaces
from PyDecNumber import DecNumber

import pyramid.schema
from pyramid.schema import fixedpoint
from rx.ormlite2 import lob

from rx.ormlite2.schema.interfaces import IORMField
from rx.ormlite2.schema.interfaces import IMultilevelChoice
from rx.ormlite2.schema.interfaces import IDatetime, IDate, ITime
from rx.ormlite2.schema.interfaces import IReference, IChoice
from rx.ormlite2.utils import parseDate, parseTime, parseDateTime


__all__ = "Bool Int Float FixedPoint LOB TextLine Text Email Password "\
          "Datetime Date Time Choice Bytes List Reference" \
          "".split()

# Interfaces -------------------------------------------------------------------

class IBoundTable(Interface):
    table_name = zope.schema.ASCIILine()


# Fields -----------------------------------------------------------------------

class ORMField(zope.schema.Field):
    u"""Базовый класс значения поля."""
    
    implements(IORMField)
    
    db_column = FieldProperty(IORMField['db_column'])
    
    _typeFactory = lambda self, value: value
    _emptyStringToNone = True
    
    def _toType(self, value):
        if value == u'' and self._emptyStringToNone:
            value = None
        if value is not None:
            value = self._typeFactory(value)
        return value
    
#    def validate(self, value):
#        try:
#            super(ORMField, self).validate(value)
#        except RequiredMissing:
#            parts = []
#            if getattr(self, 'interface', None) is not None:
#                parts.append(self.interface.__module__)
#                parts.append(self.interface.getName())
#            if getattr(self, '__name__', None) is not None:
#                parts.append(self.__name__)
#            raise RequiredMissing(u'.'.join(parts))

    def __init__(self, db_column=None, primary_key=False, **kw):
        super(ORMField, self).__init__(**kw)
        self.db_column = db_column
        self.primary_key = primary_key

    def fromUnicode(self, value):
        u"""Преобразует входное значение из unicode-строки
        во внутренний формат.
        """
        value = self._toType(value)
        self.validate(value)
        return value

    def toUnicode(self, value):
        u"""Преобразует входное значение из внутреннего формата
        в unicode-строку.
        """
        value = self._toType(value)
        self.validate(value)
        if value is None:
            return u''
        return unicode(value)
    
    def fromDbType(self, value):
        u"""Преобразует входное значение из формата поля записи БД
        во внутренний формат.
        """
        value = self._toType(value)
        # Делаем валидацию только в debug-режиме для ускорения загрузки из БД
        if __debug__:
            try:
                self.validate(value)
            except RequiredMissing:
                value = self.default
        return value

    def toDbType(self, value):
        u"""Преобразует входное значение из внутреннего формата
        в формат поля записи БД.
        """
        value = self._toType(value)
        self.validate(value)
        return value

    def null(self):
        return None


class Bool(ORMField, zope.schema.Bool):
    u"""Булевское значение."""
    missing_value = None

    def __init__(self, db_column=None, default=False, **kw):
        super(Bool, self).__init__(db_column=db_column, default=default, **kw)
    
    def _typeFactory(self, value):
        # Значения None, '0', '', 'N' интерпретируем как False, остальные - True
        if value in (None, '0', '', 'N'):
            return False
        try:
            value = int(value)
        except (TypeError, ValueError):
            pass
        return bool(value)

    def _toType(self, value):
        if value in ('0', '', 'N'):
            return False
        return super(Bool, self)._toType(value)
    
    def toUnicode(self, value):
        value = super(Bool, self).toUnicode(value)
        if value == unicode(True):
            return u'1'
        return u'0'

    def toDbType(self, value):
        value = self._toType(value)
        if value is not None:
            value = int(value)
        self.validate(value)
        return value


class Int(ORMField, zope.schema.Int):
    u"""Целое числовое значение."""
    _typeFactory = int


class Float(ORMField, zope.schema.Float):
    u"""Числовое значение с плавающей точкой."""
    _typeFactory = float


class FixedPoint(ORMField, fixedpoint.FixedPoint):
    u"""Числовое значение с фиксированной точкой."""
    _typeFactory = lambda self, value: DecNumber(value, self.precision)

    def _toType(self, value):
        # DecNumber не поддерживает unicode для входных данных
        if isinstance(value, unicode):
            value = str(value)
        return super(FixedPoint, self)._toType(value)


class LOB(ORMField, lob.LOB):

    def fromDbType(self, value):
        if value is not None:
            return self._get_lobio_class()(value)
        return self._get_lobio_class()()

    def toDbType(self, value):
        if value is not None:
            return value.getSqlData()

#    def fromDbType(self, oid):
#        return self._get_lobio_class()(oid)

#    def toDbType(self, io):
#        return io.oid

    def null(self):
        return self._get_lobio_class()()


class _EmptyTextValue(object):
    u"""Класс для представления отсутствующего текстового значения
    в сравнениях на равенство.
    """
    
    def __eq__(self, other):
        return (other == u'' or other is None)
    def __ne__(self, other):
        return (not self.__eq__(other))


class TextLine(ORMField, zope.schema.TextLine):
    u"""Текстовое значение"""
    _typeFactory = unicode
    _emptyStringToNone = False
    missing_value = _EmptyTextValue()

    def _toType(self, value):
        if isinstance(value, str):
            try: value = unicode(value)
            except:
                raise
        return super(TextLine, self)._toType(value)


class Text(ORMField, zope.schema.Text):
    u"""Многострочное текстовое значение"""
    _typeFactory = unicode
    _emptyStringToNone = False
    missing_value = _EmptyTextValue()

    def _toType(self, value):
        if isinstance(value, str):
            value = unicode(value)
        return super(Text, self)._toType(value)
    
    def fromDbType(self, value):
        # if the value is a LOB, read in it's value
        if hasattr(value, 'read'):
            value = value.read()
        return super(Text, self).fromDbType(value)


class Email(TextLine, pyramid.schema.Email):
    u"""Поле ввода/отображения email"""
    _emptyStringToNone = True


#class Password(ORMField, zope.schema.Password): pass
class Password(TextLine, zope.schema.Password): pass


class Datetime(ORMField, zope.schema.Datetime):
    u"""Значение даты и времени"""
    implements(IDatetime)
    _type = (datetime, date)

    def __init__(self, db_column=None, format='%d.%m.%Y %H:%M', **kw):
        super(Datetime, self).__init__(db_column=db_column, **kw)
        self.format = format
    
    def _typeFactory(self, value):
        if isinstance(value, basestring):
            value = parseDateTime(value)
        if isinstance(value, datetime):
            # datetime in SQL database has percision up to second
            value = datetime.combine(value.date(), value.time().replace(microsecond=0))
            #value.replace(microsecond=0)
        elif isinstance(value, date):
            value = datetime.combine(value, time())
        else:
            value = datetime(*value)
        return value

    def toUnicode(self, value):
        value = self._toType(value)
        self.validate(value)
        if value is None:
            return u''
        return unicode(value.strftime(self.format))


class Date(ORMField, zope.schema.Date):
    u"""Значение даты"""
    implements(IDate)

    def __init__(self, db_column=None, format='%d.%m.%Y', **kw):
        super(Date, self).__init__(db_column=db_column, **kw)
        self.format = format
    
    def _typeFactory(self, value):
        if isinstance(value, basestring):
            value = parseDate(value)
        if isinstance(value, datetime):
            value = value.date()
        elif not isinstance(value, date):
            value = date(*value)
        return value

    def toUnicode(self, value):
        value = self._toType(value)
        self.validate(value)
        if value is None:
            return u''
        return unicode(value.strftime(self.format))


class Time(ORMField, zope.schema.Time):
    u"""Значение времени"""
    implements(ITime)

    def __init__(self, db_column=None, format='%H:%M', **kw):
        super(Time, self).__init__(db_column=db_column, **kw)
        self.format = format
    
    def _typeFactory(self, value):
        if isinstance(value, basestring):
            value = parseTime(value)
        if isinstance(value, datetime):
            value = value.time()
        elif not isinstance(value, time):
            value = time(*value)
        return value

    def toUnicode(self, value):
        value = self._toType(value)
        self.validate(value)
        if value is None:
            return u''
        return unicode(value.strftime(self.format))

#    def fromUnicode(self, value):
#        return parseTime(value)
#
#    def fromDbType(self, value):
#        if not isinstance(value, time):
#            return parseTime(value)
#            
#        return value


class Choice(ORMField, zope.schema.Choice):
    u"""Элемент, значение которого ограничено vocabulary.
        При обращении производится автозагрузка (lazy loading)
        значения с помощью дескриптора.
    """
    implements(IChoice)
    

class Bytes(ORMField, zope.schema.Bytes):
    fromDbType = Text.fromDbType

class List(ORMField, zope.schema.List):
    def __init__(self, separator=',', **kw):
        super(List, self).__init__(**kw)
        self.separator = separator

    def fromDbType(self, value):
        if zope.schema.interfaces.IFromUnicode.providedBy(self.value_type):
            convert = self.value_type.fromUnicode
        else:
            convert = lambda e: e
            
        return [ convert(elem) for elem in value.split(self.separator) ]

    def toDbType(self, value):
        if IORMField.providedBy(self.value_type):
            convert = self.value_type.toUnicode
        else:
            convert = unicode
        return self.separator.join([ convert(elem) for elem in value])

    def null(self):
        return []


class Reference(Choice):
    """Загрузка значения элемента происходит при выполнении load()
       объекта-контейнера.
    """
    implementsOnly(IReference, IORMField)
