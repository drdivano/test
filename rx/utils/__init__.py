# -*- coding: utf-8 -*-
import zope.interface
from zope.schema import getFieldsInOrder
from zope.schema.interfaces import IField

# на больших объемах работает медленно (~ 200 us)
def enumerate_fields(ob, exclude=[], include=[]):
    for iface in zope.interface.providedBy(ob):
        for elem in enumerate_iface_fields(iface, exclude, include):
            yield elem

def enumerate_iface_fields(iface, exclude=[], include=[]):
    for name, field in getFieldsInOrder(iface):
        if name in exclude:
            continue
        if include and name not in include:
            continue
        yield name, field
