# -*- coding: utf-8 -*-

"""DB Vocabularies.

$Id$
"""

from zope.interface import implements

from rx.ormlite2.exc import PersistenceError
from rx.ormlite2.dbop import dbquery
from rx.ormlite2.vocabulary.interfaces import IRDBVocabulary
from pyramid.vocabulary.base import ObjectVocabulary


class RDBVocabulary(ObjectVocabulary):
    u"""Словарь объектов, загружаемых из БД"""

    implements(IRDBVocabulary)

    select = None   # Кастомизированный запрос к БД

    def _load(self):
        if self.select is not None:
            self._v_skipObjectTypeCheckOnCreation = False
            params = {}
            return self.objectC.load_many(dbquery(self.select), **params)
        self._v_skipObjectTypeCheckOnCreation = True
        return self.objectC.load_all()

    def _reload(self):
        self.by_token = self._mappingFactory()
        values = self._load()
        self._reindex_by_token(values)

    def __init__(self, *args, **kwargs):
        assert self.objectC is not None
        values = self._load()
        super(RDBVocabulary, self).__init__(values, *args, **kwargs)

    def _dict2token(self, token):
        if isinstance(token, dict):
            token = tuple([ token[p] for p in self.objectC.p_keys ])
        return token

    def __getitem__(self, token):
        u"""Возвращает объект, соответствующий указанному токену."""
        try:
            return super(RDBVocabulary, self).__getitem__(
                self._dict2token(token))
        except LookupError:
            raise PersistenceError(
                'Object `%s(%s)` is not found' % (self.objectC.__name__, token))

    def getTerm(self, value):
        u"""Возвращает терм для указанного объекта"""
        try:
            return super(RDBVocabulary, self).getTerm(value)
        except LookupError:
            raise PersistenceError('Object `%s` is not found' % value)

    def getTermByToken(self, token):
        u"""Возвращает терм, соответствующий указанному токену."""
        try:
            return super(RDBVocabulary, self).getTermByToken(
                self._dict2token(token))
        except LookupError:
            raise PersistenceError(
                'Object `%s(%s)` is not found' % (self.objectC.__name__, token))
