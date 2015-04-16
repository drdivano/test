# -*- coding: utf-8 -*-

"""Vocabulary-related Adapters (specifically to ITitledTokenizedTerm)

$Id$
"""

from zope.interface import implements
from zope.component import adapts
from zope.schema.interfaces import ITitledTokenizedTerm

from pyramid.vocabulary import _fmtToken
from pyramid.registry import makeAdapterRegisterable
from rx.ormlite2.interfaces import IRecord


class Record2TermAdapter(object):
    u"""Универсальный базовый адаптер"""

    implements(ITitledTokenizedTerm)
    adapts(IRecord)

    @property
    def token(self):
        t = [ getattr(self.value, k) for k in self.value.p_keys ]
        return _fmtToken(t)


    @property
    def title(self):
        t = '%s (%s)' % (self.value.__class__.__name__, self.token)
        return t

    def __init__(self, ob):
        assert IRecord.providedBy(ob)
        self.value = ob
    
    makeAdapterRegisterable()
