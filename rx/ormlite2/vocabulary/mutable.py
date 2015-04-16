# -*- coding: utf-8 -*-

"""Mutable DB Vocabularies

$Id$
"""

import log
from zope.interface import implements, directlyProvides
from zope.schema.interfaces import ITokenizedTerm, IVocabularyFactory
from zope.deprecation import deprecate

import persistent
from persistent.mapping import PersistentMapping
from persistent.list import PersistentList
import transaction

from pyramid.vocabulary import _fmtToken
from pyramid.vocabulary.mutable import MutableObjectVocabulary
from pyramid.vocabulary.base import SubsetPreindexedVocabulary
from pyramid.vocabulary.indexer import IndexVocabulary, VocabularyIndexer
from pyramid.vocabulary.interfaces import IPersistentVocabulary
from pyramid.vocabulary.interfaces import IVocabulary, ICacheable
from pyramid.vocabulary.interfaces import IContextVocabulary, IContextual
from pyramid.vocabulary.interfaces import IVocabularyIndexer
from pyramid.vocabulary.interfaces import IVocabularyIndexerFactory
from pyramid.registry.interfaces import IRegisterableVocabulary,\
    IRegisterableVocabularyIndexer

from rx.ormlite2.vocabulary.rdb import RDBVocabulary
from rx.ormlite2.cache import ELEM_ADDED, ELEM_DELETED, ATTR_MODIFIED
from rx.ormlite2.cache import getTxnLocalData, saveChanges, con
from rx.ormlite2.interfaces import IMutableElement
from rx.ormlite2.vocabulary.resolve import PersistentMappingConflictErrorResolve,\
    PersistentListConflictErrorResolve


def _getCacheRoot():
    if not getTxnLocalData().registeredHook:
        t = transaction.get()
        t.addBeforeCommitHook(saveChanges)
        getTxnLocalData().registeredHook = True
    cacheRoot = con().root()['vocabCache']
    return cacheRoot


class PersistentVocabularyFactory(object):
    u"""Создает и инициализирует persistent-словарь."""

    implements(IVocabularyFactory, ICacheable)

    def __init__(self, vocabClass, **kw):
        if not IVocabulary.implementedBy(vocabClass):
            raise TypeError(
                "The class doesn't implement `IVocabulary` interface.")
        self.vocabClass = vocabClass
        if IContextVocabulary.implementedBy(vocabClass):
            directlyProvides(self, IContextual)

    def _getCacheKey(self, context):
        cacheKey = self.vocabClass.regName \
          if IRegisterableVocabulary.providedBy(self.vocabClass) \
          else '%s.%s' % (self.vocabClass.__module__, self.vocabClass.__name__)
        if context is not None and IContextVocabulary.implementedBy(self.vocabClass):
            try:
                token = ITokenizedTerm(context).token
            except TypeError:
                token = str(context)
            cacheKey += '|%s.%s|%s' % (context.__class__.__module__,
                context.__class__.__name__, token)
        return cacheKey

    def __call__(self, context, *interfaces):
        cacheRoot = _getCacheRoot()
        cacheKey = self._getCacheKey(context)
        try:
            v = cacheRoot[cacheKey]
            return v
        except KeyError:
            args = []
            if IContextVocabulary.implementedBy(self.vocabClass):
                args.append(context)
            v = cacheRoot[cacheKey] = self.vocabClass(*args)
            log.debug('Cache key %r created for %r' % (cacheKey, self.vocabClass))
            return v


class PersistentVocabularyMapping(PersistentMappingConflictErrorResolve): pass
class PersistentVocabularyAfterChangeList(PersistentListConflictErrorResolve): pass

class PersistentVocabulary(RDBVocabulary, MutableObjectVocabulary, persistent.Persistent):

    implements(IPersistentVocabulary)

    _mappingFactory = PersistentVocabularyMapping
    factory = PersistentVocabularyFactory

    def __init__(self, *args, **kwargs):
        assert self.objectC is not None
        assert IMutableElement.implementedBy(self.objectC)
        super(PersistentVocabulary, self).__init__(*args, **kwargs)
        self.p_afterChange = PersistentVocabularyAfterChangeList()

    def _load(self):
        values = super(PersistentVocabulary, self)._load()
        if not IMutableElement.implementedBy(self.objectC):
            values = [ IMutableElement(v) for v in values ]
        for v in values:
            v.p_subscribe(self)
        return values

    def add(self, value):
        u"""Добавляет объект в словарь."""
        if value is None:
            raise TypeError(value)
        if not isinstance(value, self.objectC):
            raise TypeError(value)
        if self.objectI is not None and not self.objectI.providedBy(value):
            raise TypeError(value)
        value = IMutableElement(value)
        term = ITokenizedTerm(value)
        token = _fmtToken(term.token)
        if token in self.by_token:
            del self.by_token[token]
        self.by_token[token] = value
        txnData = getTxnLocalData()
        txnData.added_objects.append(value)
        value.p_subscribe(self)
        log.debug('Element %r added to %r' % (token, self))
        for ob in self.p_afterChange:
            ob.p_onChange(ELEM_ADDED, (value,), None, None)

    @deprecate('`append` method is deprecated - use `add`')
    def append(self, value):
        self.add(value)

    def __delitem__(self, token):
        u"""Удаляет объект, соответствующий указанному токену."""
        token = _fmtToken(self._dict2token(token))
        # Удаление записи из БД при удалении объекта из вокаба может нарушить
        # ссылочную целостность в ситуации, когда на объект ссылается другой
        # mutable-объект, и ссылка в нем удаляется только в конце транзакции.
        # Поэтому фактическое удаление записи происходит тоже в конце транзакции
        #self.by_token[key].delete()
        value = self.by_token[token]
        value.p_unsubscribe(self)
        del self.by_token[token]
        # Внимание! Мы сохраняем ссылку на удаляемый объект.
        # Фактическое его удаление произойдет после удаления txnLocalData
        getTxnLocalData().deleted_objects.add(value)
        log.debug('Element %r deleted from %r' % (token, self))

        for ob in self.p_afterChange:
            ob.p_onChange(ELEM_DELETED, (value,), None, None)

    def p_onChange(self, event, obs, attr, value):
        for ob in self.p_afterChange:
            ob.p_onChange(event, obs, attr, value)

    def p_subscribe(self, ob):
        self.p_afterChange.append(ob)

    def p_unsubscribe(self, ob):
        self.p_afterChange.remove(ob)


def _checkCacheWrapper(func):
    u"""Вспомогательный механизм проверки состояния кеша перед вызовом метода"""
    def wrappedFunc(self, *args, **kwargs):
        self._checkCache()
        return func(self, *args, **kwargs)
    return wrappedFunc


class BasePersistentSubsetVocabularyMapping(PersistentMappingConflictErrorResolve): pass

class _BasePersistentSubsetVocabulary(object):
    u"""Бызовый класс с общим для словарей кодом."""

    implements(IPersistentVocabulary)

    _mappingFactory = BasePersistentSubsetVocabularyMapping
    factory = PersistentVocabularyFactory

    _SAFE_FOR_ELEMENT_MODIFICATIONS = False

    def p_onChange(self, event, obs, attr, value):

        def propagateEvent(event, obs, attr=None, value=None):
            for ob in self.p_afterChange:
                ob.p_onChange(event, obs, attr, value)

        if event == ELEM_ADDED:
            obs_to_add = [ ob for ob in obs if self.filter(ob) and ob not in self ]
            if obs_to_add:
                tokens_to_add = [ ITokenizedTerm(ob).token for ob in obs_to_add ]
                self._reindex_by_token(obs_to_add)
                log.debug('Element %s added to %r' % (
                    ', '.join(repr(t) for t in tokens_to_add), self))
                propagateEvent(ELEM_ADDED, obs_to_add)
        elif event == ELEM_DELETED:
            obs_to_del = [ ob for ob in obs if ob in self ]
            if obs_to_del:
                tokens_to_del = [ ITokenizedTerm(ob).token for ob in obs_to_del ]
                for token in tokens_to_del:
                    self.by_token.pop(token, None)
                log.debug('Element %s deleted from %r' % (
                    ', '.join(repr(t) for t in tokens_to_del), self))
                propagateEvent(ELEM_DELETED, obs_to_del)
        elif event == ATTR_MODIFIED \
          and not self._SAFE_FOR_ELEMENT_MODIFICATIONS:
            obs_to_add = []
            obs_to_del = []
            obs_passed = []
            for ob in obs:
                isGood = self.filter(ob)
                isIn = (ob in self)
                if isGood and not isIn:
                    obs_to_add.append(ob)
                elif not isGood and isIn:
                    obs_to_del.append(ob)
                elif isGood and isIn:
                    obs_passed.append(ob)
            if obs_to_add:
                tokens_to_add = [ ITokenizedTerm(ob).token for ob in obs_to_add ]
                self._reindex_by_token(obs_to_add)
                log.debug('Element %s added to %r' % (
                    ', '.join(repr(t) for t in tokens_to_add), self))
                propagateEvent(ELEM_ADDED, obs_to_add)
            if obs_to_del:
                tokens_to_del = [ ITokenizedTerm(ob).token for ob in obs_to_del ]
                for token in tokens_to_del:
                    self.by_token.pop(token, None)
                log.debug('Element %s deleted from %r' % (
                    ', '.join(repr(t) for t in tokens_to_del), self))
                propagateEvent(ELEM_DELETED, obs_to_del)
            if obs_passed:
                propagateEvent(ATTR_MODIFIED, obs_passed, attr, value)
        else:
            propagateEvent(event, obs, attr, value)

    def p_subscribe(self, ob):
        self.p_afterChange.append(ob)

    def p_unsubscribe(self, ob):
        self.p_afterChange.remove(ob)


class PersistentSubsetVocabularyAfterChangeList(PersistentListConflictErrorResolve): pass

class PersistentSubsetVocabulary(_BasePersistentSubsetVocabulary,
        SubsetPreindexedVocabulary, persistent.Persistent):
    u"""Кеширующий словарь-фильтр для родительского PersistentVocabulary.

    При изменении родительского словаря избирательно модифицирует данные кеша.
    """

    def __init__(self, *args, **kwargs):
        super(PersistentSubsetVocabulary, self).__init__(*args, **kwargs)
        superVocab = self._super()
        assert IPersistentVocabulary.providedBy(superVocab)
        self.p_afterChange = PersistentSubsetVocabularyAfterChangeList()
        superVocab.p_subscribe(self)


class PersistentVocabularyIndexerFactory(object):
    u"""Создает и инициализирует persistent-индекс словаря."""

    implements(IVocabularyIndexerFactory, ICacheable)

    def __init__(self, indexerClass, **kw):
        if not IVocabularyIndexer.implementedBy(indexerClass):
            raise TypeError(
                "The class doesn't implement `IVocabularyIndexer` interface.")
        self.indexerClass = indexerClass

    def _getCacheKey(self, context):
        cacheKey = self.indexerClass.regName \
          if IRegisterableVocabularyIndexer.providedBy(self.indexerClass) \
          else '%s.%s' % (self.indexerClass.__module__, self.indexerClass.__name__)
        if context is not None:
            try:
                token = ITokenizedTerm(context).token
            except TypeError:
                token = str(context)
            cacheKey += '|%s.%s|%s' % (context.__class__.__module__,
                context.__class__.__name__, token)
        return cacheKey

    def __call__(self, context, *interfaces):
        cacheRoot = _getCacheRoot()
        cacheKey = self._getCacheKey(context)
        try:
            v = cacheRoot[cacheKey]
            return v
        except KeyError:
            v = cacheRoot[cacheKey] = self.indexerClass()
            log.debug('Cache key %r created for %r' % (cacheKey, self.indexerClass))
            return v



class PersistentVocabularyIndexerListFactory(PersistentListConflictErrorResolve): pass
class PersistentVocabularyIndexerAfterChangeList(PersistentListConflictErrorResolve): pass


#class PersistentVocabularyIndexerMappingFactory(PersistentMappingConflictErrorResolve): pass
class PersistentVocabularyIndexerMapping(PersistentMapping):

    def __init__(self, *args, **kw):
        PersistentMapping.__init__(self, *args, **kw)
        self._by_rev_index = {}

    def __repr__(self):
        r = '<%s.%s at 0x%X>' % (
            self.__class__.__module__,
            self.__class__.__name__,
            id(self))
        return r

    def _p_resolveConflict(self, old, store, new):

        from rx.ormlite2.vocabulary.resolve import resolveDict
        try:
            resolved_rev_idx = resolveDict(
                old['_by_rev_index'], store['_by_rev_index'],
                new['_by_rev_index'], throw_change_conflict_exception=True)
        except:
            log.debug('Error during conflict resolution for %r' % self,
                          exc_info=True)
            raise

        resolved_data = {}
        for key, value in resolved_rev_idx.iteritems():
            resolved_data.setdefault(value, set()).add(key)

        resolved = new.copy()
        resolved['_by_rev_index'] = resolved_rev_idx
        resolved['data'] = resolved_data

        return resolved


class PersistentVocabularyIndexer(VocabularyIndexer, persistent.Persistent):

    _mappingFactory = PersistentVocabularyIndexerMapping
    #_listFactory = PersistentVocabularyIndexerListFactory
    factory = PersistentVocabularyIndexerFactory

    _SAFE_FOR_ELEMENT_MODIFICATIONS = False

    def __init__(self, *args, **kwargs):
        super(PersistentVocabularyIndexer, self).__init__(*args, **kwargs)
        vocab = self._super()
        assert IPersistentVocabulary.providedBy(vocab)
        self.p_afterChange = PersistentVocabularyIndexerAfterChangeList()
        vocab.p_subscribe(self)

    def _reindex(self):
        super(PersistentVocabularyIndexer, self)._reindex()
        by_index = getattr(self, '_by_index')
        #by_rev_index = self._mappingFactory()
        #setattr(self, '_by_rev_index', by_rev_index)
        by_rev_index = getattr(by_index, '_by_rev_index')
        for idx, obs in by_index.iteritems():
            for ob in obs:
                by_rev_index[ob] = idx
        by_index._p_changed = 1

    def p_onChange(self, event, obs, attr, value):

        def propagateEvent(event, obs, attr=None, value=None):
            for ob in self.p_afterChange:
                ob.p_onChange(event, obs, attr, value)

        by_index = getattr(self, '_by_index')
        #by_rev_index = getattr(self, '_by_rev_index')
        by_rev_index = getattr(by_index, '_by_rev_index')
        #listFactory = self._listFactory
        #mappingFactory = self._mappingFactory
        if event == ELEM_ADDED:
            obs_to_add = [ ob for ob in obs if ob not in by_rev_index ]
            if obs_to_add:
                tokens_to_add = [ ITokenizedTerm(ob).token for ob in obs_to_add ]
                for ob in obs_to_add:
                    idx = self.objectIndex(ob)
                    #by_index.setdefault(idx, listFactory()).append(ob)
                    ##by_index.setdefault(idx, mappingFactory())[ob] = 1
                    by_index.setdefault(idx, set()).add(ob)
                    by_rev_index[ob] = idx
                    by_index._p_changed = 1
                log.debug('Element %s added to %r' % (
                    ', '.join(repr(t) for t in tokens_to_add), self))
                propagateEvent(ELEM_ADDED, obs_to_add)
        elif event == ELEM_DELETED:
            obs_to_del = [ ob for ob in obs if ob in by_rev_index ]
            if obs_to_del:
                tokens_to_del = [ ITokenizedTerm(ob).token for ob in obs_to_del ]
                for ob in obs_to_del:
                    idx = by_rev_index[ob]
                    del by_rev_index[ob]
                    #by_index.get(idx, []).remove(ob)
                    ##del by_index[idx][ob]
                    by_index[idx].remove(ob)
                    by_index._p_changed = 1
                log.debug('Element %s deleted from %r' % (
                    ', '.join(repr(t) for t in tokens_to_del), self))
                propagateEvent(ELEM_DELETED, obs_to_del)
        elif event == ATTR_MODIFIED \
          and not self._SAFE_FOR_ELEMENT_MODIFICATIONS:
            obs_to_add = []
            obs_to_mod = []
            for ob in obs:
                if ob in by_rev_index:
                    obs_to_mod.append(ob)
                else:
                    obs_to_add.append(ob)
            if obs_to_mod:
                for ob in obs_to_mod:
                    idx = by_rev_index[ob]
                    new_idx = self.objectIndex(ob)
                    if new_idx  != idx:
                        token = ITokenizedTerm(ob).token
                        #by_index.get(idx, []).remove(ob)
                        ##del by_index[idx][ob]
                        by_index[idx].remove(ob)
                        #by_index.setdefault(new_idx, listFactory()).append(ob)
                        ##by_index.setdefault(new_idx, mappingFactory())[ob] = 1
                        by_index.setdefault(new_idx, set()).add(ob)
                        by_rev_index[ob] = new_idx
                        by_index._p_changed = 1
                        log.debug("Element %r changed it's index in %r" % (
                            token, self))
                propagateEvent(ATTR_MODIFIED, obs_to_mod, attr, value)
            if obs_to_add:
                tokens_to_add = [ ITokenizedTerm(ob).token for ob in obs_to_add ]
                for ob in obs_to_add:
                    idx = self.objectIndex(ob)
                    #by_index.setdefault(idx, listFactory()).append(ob)
                    ##by_index.setdefault(idx, mappingFactory())[ob] = 1
                    by_index.setdefault(idx, set()).add(ob)
                    by_rev_index[ob] = idx
                    by_index._p_changed = 1
                log.debug('Element %s added to %r' % (
                    ', '.join(repr(t) for t in tokens_to_add), self))
                propagateEvent(ELEM_ADDED, obs_to_add)
        else:
            propagateEvent(event, obs, attr, value)

    def p_subscribe(self, ob):
        self.p_afterChange.append(ob)

    def p_unsubscribe(self, ob):
        self.p_afterChange.remove(ob)


class PersistentIndexVocabularyAfterChangeList(PersistentListConflictErrorResolve): pass

class PersistentIndexVocabulary(_BasePersistentSubsetVocabulary,
        IndexVocabulary, persistent.Persistent):
    u"""Кеширующий словарь-индекс для родительского PersistentVocabulary.

    При изменении родительского словаря избирательно модифицирует данные кеша.
    """

    # TODO: PersistentIndexVocabulary is not conflict-safe due to context dependence

    def __init__(self, *args, **kwargs):
        super(PersistentIndexVocabulary, self).__init__(*args, **kwargs)
        indexer = self._indexer()
        self.p_afterChange = PersistentIndexVocabularyAfterChangeList()
        indexer.p_subscribe(self)

    def filter(self, ob):
        indexer = self._indexer()
        return (ob in indexer(self.context))

