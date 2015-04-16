# -*- coding: utf-8 -*-

"""ORM-related Interfaces.

$Id$
"""
# pylint: disable-msg=W0232,E0213,C0301,E0211,R0903

import zope.schema
import zope.schema.interfaces
import z3c.schema.email
from pyramid.schema.interfaces import IEmail, IMultilevelChoice


class IORMField(zope.schema.interfaces.IField,
        zope.schema.interfaces.IFromUnicode):
    u"""Поле схемы, связанное с колонкой таблицы БД."""
    
    db_column = zope.schema.ASCIILine(title=u'Имя колонки таблицы БД',
        required=True)

    primary_key = zope.schema.Bool(title=u'Поле является первичным ключом',
        required=False, default=False)

    def toUnicode(value):
        u"""Преобразует входное значение из внутреннего формата
        в unicode-строку.
        """
    
    def fromDbType(value):
        u"""Преобразует входное значение из формата поля записи БД
        во внутренний формат.
        """

    def toDbType(value):
        u"""Преобразует входное значение из внутреннего формата
        в формат поля записи БД.
        """


class IDatetime(zope.schema.interfaces.IDatetime):
    u"""Поле ввода/отображения даты и времени"""

    format = zope.schema.ASCIILine(title=u'Формат представления даты и времени'
        ' в виде текста', required=True, default='%d.%m.%Y %H:%M')


class IDate(zope.schema.interfaces.IDate):
    u"""Поле ввода/отображения даты"""

    format = zope.schema.ASCIILine(title=u'Формат представления даты'
        ' в виде текста', required=True, default='%d.%m.%Y')


class ITime(zope.schema.interfaces.ITime):
    u"""Поле ввода/отображения времени"""

    format = zope.schema.ASCIILine(title=u'Формат представления времени'
        ' в виде текста', required=True, default='%H:%M')


class IChoice(zope.schema.interfaces.IChoice):
    u"""Choice с поддержкой автозагрузки и контроля целостности через ORM"""
    

class IReference(zope.schema.interfaces.IChoice):
    u""" В некоторых случаях применение Choice нелогично. Например,
         объекты могут быть жестко связаны друг с другом, или связанный
         объект может принадлежать к бесконечному множеству. """
    
