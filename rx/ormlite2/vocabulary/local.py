# -*- coding: utf-8 -*-

"""Transaction-local vocabulary factory.

$Id: local.py 861 2010-09-30 13:16:05Z vmozhaev $
"""
from rx.ormlite2.txnlocal import getTxnLocalData
from pyramid.vocabulary.factory import CacheableFactory


class _TransactionLocalMapping(object):
    u"""Прокси-объект для реализации подмножества методов dict'а"""
    
    _cache_name = '_vocabulary_cache'

    def __init__(self, name=None):
        if name is None:
            import random
            name = random.random()
        cache = getTxnLocalData().localData.setdefault(self._cache_name, {})
        if name in cache:
            cache[name].clear()
        self.id = name

    @property
    def _dict(self):
        cache = getTxnLocalData().localData.setdefault(self._cache_name, {})
        return cache.setdefault(self.id, {})
    
    def __getitem__(self, key):
        return self._dict[key]

    def __setitem__(self, key, item):
        self._dict[key] = item

    def __contains__(self, key):
        return (key in self._dict)


class TransactionLocalFactory(CacheableFactory):
    u"""Создает и инициализирует локальный для транзакции кешируемый словарь.

    Словарь создается только один раз для текущеей транзакции, и далее используется
    сохраненная копия.
    """

    _mappingFactory = _TransactionLocalMapping

    def __init__(self, vocabClass, name=None, **kw):
        super(CacheableFactory, self).__init__(vocabClass)
        self.cache = self._mappingFactory(name=name)


def clearCache(name=None):
    u"""Сервисная утилита для очистки локального для транзакции кеша словарей.
    
    Если задан параметр `name`, то будет очищен только кеш заданного словаря.
    Иначе будет очищен кеш всех словарей.
    """
    cache = getTxnLocalData().localData.get('_vocabulary_cache')
    if cache is not None:
        if name is not None:
            cache.pop(name, None)
        else:
            cache.clear()
    