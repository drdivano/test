# -*- coding: utf-8 -*-

"""Big Vocabularies

$Id$
"""

from __future__ import with_statement
import threading
from zope.interface import implements
from zope.schema.interfaces import ITokenizedTerm
from rx.ormlite2 import dbop, PersistenceError
from pyramid.vocabulary.interfaces import IBigVocabulary
from rx.ormlite2.interfaces import IRecord
from rx.ormlite2.txnlocal import getTxnLocalData
from pyramid.vocabulary import _fmtToken

class BigRDBVocabulary(object):
    implements(IBigVocabulary)

    objectC = None
    _seq = 0

    def __init__(self, *args, **kwargs):
        assert self.objectC is not None
        assert IRecord.implementedBy(self.objectC)
        with threading.Lock():
            BigRDBVocabulary._seq += 1
            self.id = BigRDBVocabulary._seq
        super(BigRDBVocabulary, self).__init__(*args, **kwargs)
        self.objectC() # make sure __class_init__ has been called


    @property
    def _data(self):
        return getTxnLocalData().bigVocabCache.setdefault(self.id, {}) #id(self), {})
        # В некоторых случаях создание/удаление множества одинаковых объектов
        # может приводить к дублирующимся id(self)


    def __normalize_key(self, key):
        if type(key) not in (list, tuple):
            key = (key,)
        assert len(key) == len(self.objectC.p_keys)
        return key

    def __iter__(self):
        raise NotImplementedError, "BigVocabulary does not support iteration" 

    def __contains__(self, value):
        try:
            term = ITokenizedTerm(value)
            token = term.token
            value = term.value
        except TypeError:
            token = value
            value = None
        try:
            ob = self[token]
            return True
        except KeyError:
            return False


    def __getitem__(self, key):
        assert key is not None, "Object primary key cannot be None"
        token = _fmtToken(key)
        try:
            return self._data[token]
        except KeyError:
            pass
        key = self.__normalize_key(key)
        params = dict(zip(self.objectC.p_keys, key))
        try:
            ob = self.objectC.load(**params)
        except PersistenceError:
            raise KeyError(key)

        self._data[token] = ob
        return ob


    def get(self, token, default=None):
        u"""Возвращает объект, соответствующий указанному токену."""
        try:
            return self[token]
        except LookupError:
            return default


    def getTerm(self, value):
        u"""Возвращает терм для указанного объекта"""
        try:
            term = ITokenizedTerm(value)
        except TypeError:
            raise LookupError(value)
        if value in self:
            return term
        raise LookupError(value)


    def getTermByToken(self, token):
        u"""Возвращает терм, соответствующий указанному токену."""
        if token not in self:
            raise LookupError(token)
        return ITokenizedTerm(self[token])

