# -*- coding: utf-8 -*-
'''
$Id: $
'''

from zope.interface import Interface
from zope.component import getAdapter, provideAdapter, ComponentLookupError
from pyramid.ormlite.interfaces import IRecord
from pyramid.ormlite.schema import IORMField, IChoice
from rx.utils import enumerate_fields

PRIMITIVE_TYPES = (type(None), bool, int, long, float, str, unicode, list, tuple, dict)

class IPrimitive(Interface):
    pass

def record_to_primitive(ob):
    d = {'class': ob.__class__.__name__}
    for name, field in enumerate_fields(ob):
        if IChoice.providedBy(field):
            d[name] = ob.p_choice_tokens[name]
        elif IORMField.providedBy(field):
            d[name] = as_primitive(field.get(ob))
    return d

provideAdapter(lambda ob: record_to_primitive(ob),
               [IRecord], IPrimitive)


def as_primitive(ob):
    if isinstance(ob, dict):
        d = {}
        for k, v in ob.items():
            d[k] = as_primitive(v)
        return d
    elif isinstance(ob, (tuple, list)):
        return [as_primitive(v) for v in ob]
    elif isinstance(ob, PRIMITIVE_TYPES):
        return ob
    else:
        try:
            return getAdapter(ob, IPrimitive)
        except ComponentLookupError:
            raise ValueError(ob)
