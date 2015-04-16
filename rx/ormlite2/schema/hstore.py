# -*- coding: utf-8 -*-

"""
$Id: $

See http://www.postgresql.org/docs/8.4/static/hstore.html
"""

import zope.schema as zs
from rx.ormlite2.schema.schema import ORMField


class HStoreDict(ORMField, zs.Dict):
    key_type = zs.TextLine()
    key_separator = ':'

    def _toType(self, value):
        return value

    def fromUnicode(self, s):
        value = dict([ line.strip().split(self.key_separator, 1)
                       for line in s.strip().split('\n') ])
        self.validate(value)
        return value
    
    def toUnicode(self, value):
        return '\n'.join([('%s%s%s' % (item[0], self.key_separator, item[1]))
                           for item in value.items()])

    def constraint(self, value):
        if not super(HStoreDict, self).constraint(value):
            return False
        for k, v in value.items():
            if self.key_separator in k or self.key_separator in v:
                return False
        return True
