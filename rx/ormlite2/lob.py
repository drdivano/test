# -*- coding: utf-8 -*-
from zope.interface import implements
from zope.schema.interfaces import IField
from zope.schema._bootstrapfields import Field
from zope.component import getUtility
from PyDecNumber import DecNumber


class ILOB(IField):
    u"""Бинарный объект"""

    def read(n):
        """Read n bytes from the LOB and return them as a string"""

    def write(s):
        """Write a string to the LOB"""

    def open():
        u"""Открывает бинарный объект для записи (если применимо)"""

    def close():
        u"""Закрывает бинарный объект для чтения и записи"""

    def truncate(size):
        """Truncate the LOB to at most size bytes."""

    def unlink():
        """Delete LOB"""
    
    def getSqlData():
        """Return LOB data to be used in SQL statement"""


class LOB(Field):
    implements(ILOB)

    def _get_lobio_class(self):
        from interfaces import IDatabaseAdapter
        return getUtility(IDatabaseAdapter).LOBIO

    _type = property(_get_lobio_class)
