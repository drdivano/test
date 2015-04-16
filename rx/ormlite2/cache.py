# -*- coding: utf-8 -*-

"""Cache implementations.

$Id: cache.py 944 2012-03-14 12:12:13Z vmozhaev $
"""

from zope.interface import implements
from zope import component
import log

import transaction
import thread

from ZODB import DB
from persistent import Persistent
from tempstorage import TemporaryStorage

from pyramid.app.server.interfaces import IEngine
from rx.ormlite2 import dbop
from rx.ormlite2.interfaces import IActiveRecord
from rx.ormlite2.interfaces import IMutableElement
from rx.ormlite2.record import ActiveRecord, ObjectRef
from rx.ormlite2.schema.interfaces import IChoice
from rx.ormlite2.txnlocal import txnLocalDataSynch, getTxnLocalData
from rx.ormlite2.vocabulary.resolve import PersistentMappingConflictErrorResolve
from rx.ormlite2.vocabulary.resolve import PersistentListConflictErrorResolve


ATTR_MODIFIED = 1
ELEM_ADDED = 2
ELEM_DELETED = 3
ELEM_MODIFIED = 4

global cons, storage, db


class CacheStorage(TemporaryStorage.TemporaryStorage):
    
    def sortKey(self):
        sort_key = super(CacheStorage, self).sortKey()
        return '1:%s' % sort_key # Always first!


def registerTransactionManagerSynch(threadN=None):
    transaction.manager.registerSynch(txnLocalDataSynch)


def init(db_storage=None, cache_size=None, cache_size_bytes=None):
    global cons, storage, db
    cons = {}
    
    if 'db' in globals() and db is not None:
        db.close()
        db = None
    
    if db_storage is None:
        storage = CacheStorage('ormlite-cache')
    else:
        storage = db_storage
    
    kw = dict()
    if cache_size is not None:
        kw.update(cache_size=cache_size)
    if cache_size_bytes is not None:
        kw.update(cache_size_bytes=cache_size_bytes)
    
    db = DB(storage, **kw)
    initZODB()

    component.getUtility(IEngine).subscribe('start_thread', registerTransactionManagerSynch)


class VocabCachePersistentMapping(PersistentMappingConflictErrorResolve): pass


def initZODB():
    global db
    conn = db.open()

    dbroot = conn.root()
    dbroot['vocabCache'] = VocabCachePersistentMapping()
    transaction.commit()


def con():
    t_id = thread.get_ident()
    try:
        c = cons[t_id]
        return c
    except KeyError:
        c = cons[t_id] = db.open()
        return c
    except NameError:
        raise RuntimeError('Module is not initialized. You must call init() first.')


def closeCon():
    t_id = thread.get_ident()
    cons[t_id].close()
    del cons[t_id]


def saveChanges():
    dbop.defer_constraints()
    from zope.component import getAllUtilitiesRegisteredFor
    from rx.ormlite2.interfaces import IObjectChangeLogger

    loggers = getAllUtilitiesRegisteredFor(IObjectChangeLogger)

    edited = []
    for elem in con()._registered_objects:
        # Fix for python >2.5
        try:
            hash(elem)
        except TypeError:
            continue
        else:
            if elem in getTxnLocalData().deleted_objects:
                continue
            edited.append(elem)

    for logger in loggers:
        logger.log(getTxnLocalData().added_objects,
                   edited,
                   getTxnLocalData().deleted_objects)


    for ob in getTxnLocalData().added_objects + con()._registered_objects:
        # проверка на интерфейс IMutableElement нужна, т.к. в
        # _registered_objects могут быть vocabulary
        # (из-за добавления/удаления элементов из них), и прочие объекты
        if IMutableElement.providedBy(ob):
            #assert ob not in getTxnLocalData().deleted_objects
            if ob not in getTxnLocalData().deleted_objects:
                IActiveRecord(ob).save()
    for ob in getTxnLocalData().deleted_objects:
        if IMutableElement.providedBy(ob):
            IActiveRecord(ob).delete()

    log.debug('saveChanges hook completed successfully')


class MutableObjectRef(Persistent, ObjectRef):
    def set(self, value):
        super(MutableObjectRef, self).set(value)
        if hasattr(self.owner, 'p_afterChange'):
            [ ob.p_onChange(ATTR_MODIFIED, (self.owner,), self.name, self.key)
              for ob in self.owner.p_afterChange ]
        
    def __setattr__(self, name, value):
        super(MutableObjectRef, self).__setattr__(name, value)

        if name == 'key' and hasattr(self.owner, 'p_afterChange'):
            [ ob.p_onChange(ATTR_MODIFIED, (self.owner,), self.name, value)
              for ob in self.owner.p_afterChange ]


class MutableElement(Persistent, ActiveRecord): #, Term):
    implements(IMutableElement)

    @classmethod
    def _object_ref(self, *args, **kw):
        return MutableObjectRef(*args, **kw)

    @property
    def _p_new(self):
        return not self._p_jar

## мы не можем вынести методы в отдельный класс ChangeNotify,
## т.к. persistent.Persistent определяет свой __setattr__, и не вызывает
## __setattr__ дальше по цепочке

    def p_subscribe(self, ob):
        self.p_afterChange.append(ob)

    def p_unsubscribe(self, ob):
        self.p_afterChange.remove(ob)

    def copy(self):
        clone = super(MutableElement, self).copy()
        clone.p_version = 0
        #clone.p_afterChange = []
        clone.p_afterChange = PersistentListConflictErrorResolve()
        return clone

    def __setattr__(self, name, value):
        if name.startswith('p_') or name.startswith('_p_') or name.startswith('_v_'):
            return super(MutableElement, self).__setattr__(name, value)

        if not hasattr(self, 'p_version'):
            self.p_version = 0
            #self.p_afterChange = []
            self.p_afterChange = PersistentListConflictErrorResolve()

        super(MutableElement, self).__setattr__(name, value)
        super(MutableElement, self).__setattr__('p_version', self.p_version + 1)

        if hasattr(self, 'p_afterChange'):
            [ ob.p_onChange(ATTR_MODIFIED, (self,), name, value)
              for ob in self.p_afterChange ]

    def _p_resolveConflict(self, old, saved, new):
        #return newState
        resolved = new.copy()
        resolved_names = []

        # Merge model fields
        for name in self.p_attr_seq:
            if not IChoice.providedBy(self.p_fields[name]):
                old_v = old[name]
                saved_v = saved[name]
                new_v = new[name]
                if saved_v != old_v and new_v == old_v:
                    resolved[name] = saved_v
                    resolved_names.append(name)

        # Merge model choice fields
        if 'p_choice_tokens' in old:
            resolved['p_choice_tokens'] = resolved['p_choice_tokens'].copy()
            for name in old['p_choice_tokens']:
                old_v = old['p_choice_tokens'][name]
                saved_v = saved['p_choice_tokens'][name]
                new_v = new['p_choice_tokens'][name]
                if saved_v != old_v and new_v == old_v:
                    resolved['p_choice_tokens'][name] = saved_v
                    resolved_names.append(name)

        log.debug('Conflict resolved for %r, values of %s merged' % \
                      (self, ', '.join(repr(n) for n in resolved_names)))
        return resolved

