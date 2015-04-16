# -*- coding: utf-8 -*-

"""Cache implementations.

$Id: resolve.py 941 2012-03-07 14:53:58Z vmozhaev $
"""

import log
from persistent.mapping import PersistentMapping
from ZODB.POSException import ConflictError


def resolveDict(old, store, new,
        throw_add_conflict_exception=True,
        throw_change_conflict_exception=False):
    u"""Resolves dictionary conflicts"""

    resolved = new.copy()

    old_keys = set(old)
    store_keys = set(store)
    new_keys = set(new)

    deleted = old_keys - store_keys
    added = store_keys - old_keys
    saved = store_keys & old_keys

    for key in deleted:
        if key in new_keys:
            del resolved[key]

    for key in added:
        if key in new_keys:
            if throw_add_conflict_exception:
                log.debug('Conflict resolver raised an add dictionary key '
                    '"%s" conflict error' % key)
                raise ConflictError('Add dictionary key conflict error')
        resolved[key] = store[key]

    for key in saved:
        if key in new_keys:
            if store[key] != old[key]:
                if new[key] != old[key]:
                    if throw_change_conflict_exception:
                        log.debug('Conflict resolver raised a change '
                            'dictionary value for key "%s" conflict error' % key)
                        raise ConflictError('Change dictionary value conflict error')
                else:
                    resolved[key] = store[key]

    return resolved


class PersistentMappingConflictErrorResolve(PersistentMapping):
     
    def __repr__(self):
        r = '<%s.%s at 0x%X>' % (
            self.__class__.__module__,
            self.__class__.__name__,
            id(self))
        return r
    
    def _p_resolveConflict(self, old, store, new):

        try:
            resolved_data = resolveDict(old['data'], store['data'], new['data'])
        except:
            log.debug('Error during conflict resolution for %r' % self,
                          exc_info=True)
            raise
            
        resolved = new.copy()
        resolved['data'] = resolved_data
        
        if log.getLogger().level <= log.DEBUG:
            deleted = set(new['data']) - set(resolved_data)
            added = set(resolved_data) - set(new['data'])
            changed = set(key for key in resolved_data \
                          if key in new['data'] and key in resolved_data \
                          and resolved_data[key] != new['data'][key])
            parts = ['Conflict resolved for %r' % self]
            if deleted:
                parts.append('key %s deleted' % \
                             ', '.join(repr(k) for k in deleted))
            if added:
                parts.append('key %s added' % \
                             ', '.join(repr(k) for k in added))
            if changed:
                parts.append('key %s changed' % \
                             ', '.join(repr(k) for k in changed))
            log.debug(', '.join(parts))

        return resolved


class PersistentMappingWithAddAndRemove(PersistentMapping):
    u"""Mapping with minimum list/set emulation"""

    def append(self, key, value=1): self[key] = value
    def add(self, key, value=1): self[key] = value
    def remove(self, key): del self[key]


class PersistentListConflictErrorResolve(PersistentMappingWithAddAndRemove):

    def __repr__(self):
        r = '<%s.%s at 0x%X>' % (
            self.__class__.__module__,
            self.__class__.__name__,
            id(self))
        return r

    def _p_resolveConflict(self, old, store, new):

        try:
            resolved_data = resolveDict(old['data'], store['data'], new['data'],
                throw_add_conflict_exception=False)
        except:
            log.debug('Error during conflict resolution for %r' % self,
                          exc_info=True)
            raise
        resolved = new.copy()
        resolved['data'] = resolved_data
        
        if log.getLogger().level <= log.DEBUG:
            deleted = set(new['data']) - set(resolved_data)
            added = set(resolved_data) - set(new['data'])
            changed = set(key for key in resolved_data \
                          if key in new['data'] and key in resolved_data \
                          and resolved_data[key] != new['data'][key])
            parts = ['Conflict resolved for %r' % self]
            if deleted:
                parts.append('key %s deleted' % \
                             ', '.join(repr(k) for k in deleted))
            if added:
                parts.append('key %s added' % \
                             ', '.join(repr(k) for k in added))
            if changed:
                parts.append('key %s changed' % \
                             ', '.join(repr(k) for k in changed))
            log.debug(', '.join(parts))

        return resolved

