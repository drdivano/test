# -*- coding: utf-8 -*-

from datetime import datetime
import unittest
import testoob
from zope.interface import Interface, implements
from zope.schema import Int, TextLine
#from pyramid.ormlite.schema import Int, TextLine
from rx.utils import enumerate_iface_fields, enumerate_fields
from rx.utils.dt import parse_iso_datetime
from rx.utils.json import as_primitive, record_to_primitive

class Test(unittest.TestCase):
    def test_enumerate_fields(self):
        class IOb(Interface):
            k = Int()
            v = TextLine()

        class Ob(object):
            implements(IOb)

        fields = list(enumerate_iface_fields(IOb))
        self.assertEqual(len(fields), 2)
        self.assertEqual(fields[0][0], 'k')
        self.assertTrue(isinstance(fields[0][1], Int))
        self.assertEqual(len(fields[0]), 2)

        self.assertEqual(fields[1][0], 'v')
        self.assertTrue(isinstance(fields[1][1], TextLine))

        ob = Ob()
        fields = list(enumerate_fields(ob))
        self.assertEqual(len(fields), 2)
        self.assertEqual(len(fields[0]), 2)


    def test_parse_iso_datetime(self):
        dt = parse_iso_datetime('2010-12-30T23:50:51')
        self.assertEqual(dt, datetime(2010, 12, 30, 23, 50, 51))
        
    def test_json_primitive(self):
        self.assertEqual(as_primitive(123), 123)
        self.assertEqual(as_primitive('123'), '123')
        self.assertEqual(as_primitive(None), None)
        self.assertEqual(as_primitive([1, 2, 3]), [1, 2, 3])
        self.assertEqual(as_primitive({1: 2}), {1: 2})

        class Ob: pass
        ob = Ob()
        self.assertRaises(ValueError, as_primitive, ob)
        self.assertRaises(ValueError, as_primitive, [ob])

        from zope.interface import Interface, implements
        from pyramid.ormlite import ActiveRecord
        from pyramid.ormlite.schema import Int, TextLine
        
        class IOb(Interface):
            k = Int(db_column='k')
            v = TextLine(db_column='v')
            
        class Ob2(ActiveRecord):
            implements(IOb)

        ob2 = Ob2(k=123, v='sss')

        self.assertEqual(as_primitive(ob2), {'class': 'Ob2', 'k': 123, 'v': 'sss'})
        self.assertEqual(as_primitive([ob2]), [{'class': 'Ob2', 'k': 123, 'v': 'sss'}])

            
if __name__ == "__main__":
    testoob.main()
