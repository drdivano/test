# -*- coding: utf-8 -*-

"""ORM Schema Tests.

$Id$
"""

import unittest
import testoob
from datetime import date, time, datetime

import zope.schema.interfaces
from zope.schema._bootstrapinterfaces import RequiredMissing
from zope.schema._bootstrapinterfaces import ConstraintNotSatisfied
from z3c.schema.email.interfaces import NotValidRFC822MailAdress
from PyDecNumber import DecNumber

import pyramid.schema.interfaces
from rx.ormlite2.schema.interfaces import IORMField
from rx.ormlite2.schema import *
from rx.ormlite2.schema.schema import ORMField


class TestSchemaFields(unittest.TestCase):
    """Tests of ORM schema fields"""
    
    def testORMField(self):
        """Test base field `ORMField`"""
        #'`db_column` parameter should be mandatory')
        self.assertRaises(RequiredMissing, ORMField)
        ob = ORMField('column_name', required=False)
        self.assertTrue(IORMField.providedBy(ob))
        self.assertEqual(u'abc', ob.fromUnicode(u'abc'))
        self.assertEqual(u'abc', ob.toUnicode(u'abc'))
        self.assertEqual(u'abc', ob.toUnicode('abc'))
        self.assertEqual(u'123', ob.toUnicode(123))
        self.assertEqual(u'', ob.toUnicode(None))
        self.assertEqual(u'abc', ob.fromDbType(u'abc'))
        self.assertEqual(None, ob.fromDbType(None))
        self.assertEqual(u'abc', ob.toDbType(u'abc'))
        ob.required = True
        self.assertEqual(None, ob.fromDbType(None))
        ob.default = 'abc'
        self.assertEqual('abc', ob.fromDbType(None))
        #self.assertRaises(RequiredMissing, ob.fromDbType, None)

    def testBool(self):
        """Test field `Bool`"""
        ob = Bool('column_name', required=False)
        self.assertTrue(IORMField.providedBy(ob))
        self.assertTrue(zope.schema.interfaces.IBool.providedBy(ob))
        
        self.assertEqual(True, ob.fromUnicode('1'))
        self.assertEqual(True, ob.fromUnicode('abc'))
        self.assertEqual(False, ob.fromUnicode('0'))
        self.assertEqual(False, ob.fromUnicode(''))
        self.assertEqual(False, ob.fromUnicode('N'))
        self.assertEqual(None, ob.fromUnicode(None))
        
        self.assertEqual(u'1', ob.toUnicode(True))
        self.assertEqual(u'1', ob.toUnicode(1))
        self.assertEqual(u'1', ob.toUnicode('1'))
        self.assertEqual(u'1', ob.toUnicode('abc'))
        self.assertEqual(u'0', ob.toUnicode(False))
        self.assertEqual(u'0', ob.toUnicode(0))
        self.assertEqual(u'0', ob.toUnicode('0'))
        self.assertEqual(u'0', ob.toUnicode(''))
        self.assertEqual(u'0', ob.toUnicode(None))

        self.assertEqual(True, ob.fromDbType(True))
        self.assertEqual(True, ob.fromDbType(1))
        self.assertEqual(True, ob.fromDbType('1'))
        self.assertEqual(True, ob.fromDbType('abc'))
        self.assertEqual(False, ob.fromDbType(False))
        self.assertEqual(False, ob.fromDbType(0))
        self.assertEqual(False, ob.fromDbType('0'))
        self.assertEqual(False, ob.fromDbType(''))
        self.assertEqual(False, ob.fromDbType('N'))
        self.assertEqual(None, ob.fromDbType(None))
        
        self.assertEqual(1, ob.toDbType(True))
        self.assertEqual(0, ob.toDbType(False))
        self.assertEqual(None, ob.toDbType(None))

        ob.required = True
        self.assertEqual(False, ob.fromUnicode('0'))
        self.assertEqual(False, ob.fromUnicode(False))
        self.assertRaises(RequiredMissing, ob.toDbType, None)

        self.assertEqual(False, ob.fromDbType(False))
        self.assertEqual(False, ob.fromDbType(None))
        ob.default = True
        self.assertEqual(True, ob.fromDbType(None))
        self.assertEqual(False, ob.fromDbType(False))
        
    def testInt(self):
        """Test field `Int`"""
        ob = Int('column_name', required=False)
        self.assertTrue(IORMField.providedBy(ob))
        self.assertTrue(zope.schema.interfaces.IInt.providedBy(ob))

        self.assertEqual(123, ob.fromUnicode(u'123'))
        self.assertEqual(None, ob.fromUnicode(u''))
        self.assertEqual(None, ob.fromUnicode(None))

        self.assertEqual(u'123', ob.toUnicode(u'123'))
        self.assertEqual(u'123', ob.toUnicode(123))
        self.assertEqual(u'123', ob.toUnicode(123.45))
        self.assertEqual(u'', ob.toUnicode(u''))
        self.assertEqual(u'', ob.toUnicode(None))

        self.assertEqual(123, ob.fromDbType(u'123'))
        self.assertEqual(123, ob.fromDbType(123))
        self.assertEqual(123, ob.fromDbType(123.45))
        self.assertEqual(None, ob.fromDbType(u''))
        self.assertEqual(None, ob.fromDbType(None))

        self.assertEqual(123, ob.toDbType(123))
        self.assertEqual(123, ob.toDbType(123.45))
        self.assertEqual(None, ob.toDbType(None))
        
        ob.required = True
        self.assertRaises(RequiredMissing, ob.fromUnicode, u'')
        self.assertRaises(RequiredMissing, ob.fromUnicode, None)
        self.assertRaises(RequiredMissing, ob.toUnicode, u'')
        self.assertRaises(RequiredMissing, ob.toDbType, u'')
        #self.assertRaises(RequiredMissing, ob.fromDbType, u'')
        #self.assertRaises(RequiredMissing, ob.fromDbType, None)

        self.assertEqual(None, ob.fromDbType(u''))
        self.assertEqual(None, ob.fromDbType(None))
        ob.default = 0
        self.assertEqual(0, ob.fromDbType(u''))
        self.assertEqual(0, ob.fromDbType(None))

    def testFixedPoint(self):
        """Test field `FixedPoint`"""
        ob = FixedPoint('column_name', required=False)
        self.assertTrue(IORMField.providedBy(ob))
        self.assertTrue(pyramid.schema.interfaces.IFixedPoint.providedBy(ob))

        self.assertEqual('123.00', str(ob.fromUnicode(u'123')))
        self.assertEqual('123.46', str(ob.fromUnicode(u'123.45678')))
        self.assertEqual(None, ob.fromUnicode(u''))
        self.assertEqual(None, ob.fromUnicode(None))

        self.assertEqual(u'123.00', ob.toUnicode(u'123'))
        self.assertEqual(u'123.00', ob.toUnicode(123))
        self.assertEqual(u'123.46', ob.toUnicode(u'123.45678'))
        self.assertEqual(u'123.46', ob.toUnicode(123.45678))
        self.assertEqual(u'123.46', ob.toUnicode(DecNumber(123.45678, 5)))
        self.assertEqual(u'', ob.toUnicode(u''))
        self.assertEqual(u'', ob.toUnicode(None))

        self.assertEqual('123.00', str(ob.fromDbType(u'123')))
        self.assertEqual('123.00', str(ob.fromDbType(123)))
        self.assertEqual('123.46', str(ob.fromDbType(u'123.45678')))
        self.assertEqual('123.46', str(ob.fromDbType(123.45678)))
        self.assertEqual('123.46', str(ob.fromDbType(DecNumber(123.45678, 5))))
        self.assertEqual(None, ob.fromDbType(u''))
        self.assertEqual(None, ob.fromDbType(None))

        self.assertEqual('123.00', str(ob.toDbType(u'123')))
        self.assertEqual('123.00', str(ob.toDbType(123)))
        self.assertEqual('123.46', str(ob.toDbType(u'123.45678')))
        self.assertEqual('123.46', str(ob.toDbType(123.45678)))
        self.assertEqual('123.46', str(ob.toDbType(DecNumber(123.45678, 5))))
        self.assertEqual(None, ob.toDbType(u''))
        self.assertEqual(None, ob.toDbType(None))
        
        ob.precision = 3
        self.assertEqual('123.000', str(ob.fromUnicode(u'123')))
        self.assertEqual('123.457', str(ob.fromUnicode(u'123.45678')))

        ob.required = True
        self.assertRaises(RequiredMissing, ob.fromUnicode, u'')
        self.assertRaises(RequiredMissing, ob.fromUnicode, None)
        self.assertRaises(RequiredMissing, ob.toUnicode, u'')
        self.assertRaises(RequiredMissing, ob.toDbType, u'')
        #self.assertRaises(RequiredMissing, ob.fromDbType, u'')
        #self.assertRaises(RequiredMissing, ob.fromDbType, None)

        self.assertEqual(None, ob.fromDbType(u''))
        self.assertEqual(None, ob.fromDbType(None))
        ob.default = DecNumber(0)
        self.assertEqual(0, ob.fromDbType(u''))
        self.assertEqual(0, ob.fromDbType(None))

    def testTextLine(self):
        """Test field `TextLine`"""
        ob = TextLine('column_name', required=False)
        self.assertTrue(IORMField.providedBy(ob))
        self.assertTrue(zope.schema.interfaces.ITextLine.providedBy(ob))

        self.assertEqual(u'abc', ob.fromUnicode(u'abc'))
        self.assertEqual(u'abc', ob.fromUnicode('abc'))
        self.assertTrue(isinstance(ob.fromUnicode('abc'), unicode))
        self.assertEqual(u'', ob.fromUnicode(u''))
        self.assertEqual(None, ob.fromUnicode(None))
        self.assertRaises(ConstraintNotSatisfied, ob.fromUnicode, u'abc\nd')

        self.assertEqual(u'abc', ob.toUnicode(u'abc'))
        self.assertEqual(u'abc', ob.toUnicode('abc'))
        self.assertTrue(isinstance(ob.toUnicode('abc'), unicode))
        self.assertEqual(u'', ob.toUnicode(u''))
        self.assertEqual(u'', ob.toUnicode(None))
        self.assertRaises(ConstraintNotSatisfied, ob.toUnicode, u'abc\nd')

        self.assertEqual(u'abc', ob.fromDbType(u'abc'))
        self.assertEqual(u'abc', ob.fromDbType('abc'))
        self.assertTrue(isinstance(ob.fromDbType('abc'), unicode))
        self.assertEqual(u'123', ob.fromDbType(123))
        self.assertEqual(u'', ob.fromDbType(u''))
        self.assertEqual(None, ob.fromDbType(None))
        self.assertRaises(ConstraintNotSatisfied, ob.fromDbType, u'abc\nd')

        self.assertEqual(u'abc', ob.toDbType(u'abc'))
        self.assertEqual(u'abc', ob.toDbType('abc'))
        self.assertTrue(isinstance(ob.toDbType('abc'), unicode))
        self.assertEqual(u'', ob.toDbType(u''))
        self.assertEqual(None, ob.toDbType(None))
        self.assertRaises(ConstraintNotSatisfied, ob.toDbType, u'abc\nd')
        
        ob.required = True
        self.assertRaises(RequiredMissing, ob.fromUnicode, u'')
        self.assertRaises(RequiredMissing, ob.fromUnicode, None)
        self.assertRaises(RequiredMissing, ob.toUnicode, u'')
        self.assertRaises(RequiredMissing, ob.toDbType, u'')
        #self.assertRaises(RequiredMissing, ob.fromDbType, '')
        #self.assertRaises(RequiredMissing, ob.fromDbType, u'')
        #self.assertRaises(RequiredMissing, ob.fromDbType, None)

        self.assertEqual(None, ob.fromDbType(u''))
        self.assertEqual(None, ob.fromDbType(None))
        ob.default = u''
        self.assertEqual(u'', ob.fromDbType(u''))
        self.assertEqual(u'', ob.fromDbType(None))

    def testText(self):
        """Test field `Text`"""
        ob = Text('column_name', required=False)
        self.assertTrue(IORMField.providedBy(ob))
        self.assertTrue(zope.schema.interfaces.IText.providedBy(ob))

        self.assertEqual(u'abc', ob.fromUnicode(u'abc'))
        self.assertEqual(u'abc', ob.fromUnicode('abc'))
        self.assertTrue(isinstance(ob.fromUnicode('abc'), unicode))
        self.assertEqual(u'', ob.fromUnicode(u''))
        self.assertEqual(None, ob.fromUnicode(None))
        self.assertEqual(u'abc\nd', ob.fromUnicode(u'abc\nd'))

        self.assertEqual(u'abc', ob.toUnicode(u'abc'))
        self.assertEqual(u'abc', ob.toUnicode('abc'))
        self.assertTrue(isinstance(ob.toUnicode('abc'), unicode))
        self.assertEqual(u'', ob.toUnicode(u''))
        self.assertEqual(u'', ob.toUnicode(None))
        self.assertEqual(u'abc\nd', ob.toUnicode(u'abc\nd'))

        self.assertEqual(u'abc', ob.fromDbType(u'abc'))
        self.assertEqual(u'abc', ob.fromDbType('abc'))
        self.assertTrue(isinstance(ob.fromDbType('abc'), unicode))
        self.assertEqual(u'123', ob.fromDbType(123))
        self.assertEqual(u'', ob.fromDbType(u''))
        self.assertEqual(None, ob.fromDbType(None))
        self.assertEqual(u'abc\nd', ob.fromDbType(u'abc\nd'))

        self.assertEqual(u'abc', ob.toDbType(u'abc'))
        self.assertEqual(u'abc', ob.toDbType('abc'))
        self.assertTrue(isinstance(ob.toDbType('abc'), unicode))
        self.assertEqual(u'', ob.toDbType(u''))
        self.assertEqual(None, ob.toDbType(None))
        self.assertEqual(u'abc\nd', ob.toDbType(u'abc\nd'))
        
        ob.required = True
        self.assertRaises(RequiredMissing, ob.fromUnicode, u'')
        self.assertRaises(RequiredMissing, ob.fromUnicode, None)
        self.assertRaises(RequiredMissing, ob.toUnicode, u'')
        self.assertRaises(RequiredMissing, ob.toDbType, u'')
        #self.assertRaises(RequiredMissing, ob.fromDbType, u'')
        #self.assertRaises(RequiredMissing, ob.fromDbType, None)

        self.assertEqual(None, ob.fromDbType(u''))
        self.assertEqual(None, ob.fromDbType(None))
        ob.default = u''
        self.assertEqual(u'', ob.fromDbType(u''))
        self.assertEqual(u'', ob.fromDbType(None))

    def testEmail(self):
        """Test field `Email`"""
        ob = Email('column_name', required=False)
        self.assertTrue(IORMField.providedBy(ob))
        #self.assertTrue(zope.schema.interfaces.ITextLine.providedBy(ob))

        self.assertEqual(u'abc@some.domain', ob.fromUnicode(u'abc@some.domain'))
        self.assertEqual(u'abc@some.domain', ob.fromUnicode('abc@some.domain'))
        self.assertTrue(isinstance(ob.fromUnicode('abc@some.domain'), unicode))
        self.assertEqual(None, ob.fromUnicode(u''))
        self.assertEqual(None, ob.fromUnicode(None))
        self.assertRaises(NotValidRFC822MailAdress, ob.fromUnicode, u'abc@')
        self.assertRaises(ConstraintNotSatisfied, ob.fromUnicode, u'abc@some.domain\n')

        self.assertEqual(u'abc@some.domain', ob.toUnicode(u'abc@some.domain'))
        self.assertEqual(u'abc@some.domain', ob.toUnicode('abc@some.domain'))
        self.assertTrue(isinstance(ob.toUnicode('abc@some.domain'), unicode))
        self.assertEqual(u'', ob.toUnicode(u''))
        self.assertEqual(u'', ob.toUnicode(None))
        self.assertRaises(NotValidRFC822MailAdress, ob.toUnicode, u'abc@')
        self.assertRaises(ConstraintNotSatisfied, ob.toUnicode, u'abc@some.domain\n')

        self.assertEqual(u'abc@some.domain', ob.fromDbType(u'abc@some.domain'))
        self.assertEqual(u'abc@some.domain', ob.fromDbType('abc@some.domain'))
        self.assertTrue(isinstance(ob.fromDbType('abc@some.domain'), unicode))
        self.assertEqual(None, ob.fromDbType(u''))
        self.assertEqual(None, ob.fromDbType(None))
        self.assertRaises(NotValidRFC822MailAdress, ob.fromDbType, u'abc@')
        self.assertRaises(ConstraintNotSatisfied, ob.fromDbType, u'abc@some.domain\n')

        self.assertEqual(u'abc@some.domain', ob.toDbType(u'abc@some.domain'))
        self.assertEqual(u'abc@some.domain', ob.toDbType('abc@some.domain'))
        self.assertTrue(isinstance(ob.toDbType('abc@some.domain'), unicode))
        self.assertEqual(None, ob.toDbType(u''))
        self.assertEqual(None, ob.toDbType(None))
        self.assertRaises(NotValidRFC822MailAdress, ob.toDbType, u'abc@')
        self.assertRaises(ConstraintNotSatisfied, ob.toDbType, u'abc@some.domain\n')
        
        ob.required = True
        self.assertRaises(RequiredMissing, ob.fromUnicode, u'')
        self.assertRaises(RequiredMissing, ob.fromUnicode, None)
        self.assertRaises(RequiredMissing, ob.toUnicode, u'')
        self.assertRaises(RequiredMissing, ob.toDbType, u'')
        #self.assertRaises(RequiredMissing, ob.fromDbType, u'')
        #self.assertRaises(RequiredMissing, ob.fromDbType, None)

        self.assertEqual(None, ob.fromDbType(u''))
        self.assertEqual(None, ob.fromDbType(None))
        ob.default = u''
        self.assertEqual(u'', ob.fromDbType(u''))
        self.assertEqual(u'', ob.fromDbType(None))

    def testDatetime(self):
        """Test field `Datetime`"""
        ob = Datetime('column_name', required=False)
        self.assertTrue(IORMField.providedBy(ob))
        self.assertTrue(zope.schema.interfaces.IDatetime.providedBy(ob))
        
        dt = datetime
        d = date

        self.assertEqual(dt(2009,12,31,23,59,0), ob.fromUnicode(u'2009-12-31 23:59:00'))
        self.assertEqual(dt(2009,12,31,23,59,0), ob.fromUnicode(u'31.12.2009 23:59:00'))
        self.assertEqual(dt(2009,12,31,0,0,0), ob.fromUnicode(u'2009-12-31'))
        self.assertEqual(None, ob.fromUnicode(u''))
        self.assertEqual(None, ob.fromUnicode(None))

        self.assertEqual(u'31.12.2009 23:59', ob.toUnicode(dt(2009,12,31,23,59,59)))
        self.assertEqual(u'31.12.2009 00:00', ob.toUnicode(dt(2009,12,31)))
        self.assertEqual(u'', ob.toUnicode(u''))
        self.assertEqual(u'', ob.toUnicode(None))

        self.assertEqual(dt(2009,12,31,23,59,0), ob.fromDbType(u'2009-12-31 23:59:00'))
        self.assertEqual(dt(2009,12,31,23,59,0), ob.fromDbType(u'31.12.2009 23:59:00'))
        self.assertEqual(dt(2009,12,31,23,59,0), ob.fromDbType(dt(2009,12,31,23,59,0,99)))
        self.assertEqual(dt(2009,12,31,0,0,0), ob.fromDbType(d(2009,12,31)))
        self.assertEqual(None, ob.fromDbType(u''))
        self.assertEqual(None, ob.fromDbType(None))

        self.assertEqual(dt(2009,12,31,23,59,0), ob.toDbType(dt(2009,12,31,23,59,0,99)))
        self.assertEqual(dt(2009,12,31,0,0,0), ob.toDbType(d(2009,12,31)))
        self.assertEqual(None, ob.toDbType(u''))
        self.assertEqual(None, ob.toDbType(None))

        ob.format = '%d|%m|%Y{%H:%M}'
        self.assertEqual(u'31|12|2009{23:59}', ob.toUnicode(dt(2009,12,31,23,59,59)))
        
        ob.required = True
        self.assertRaises(RequiredMissing, ob.fromUnicode, u'')
        self.assertRaises(RequiredMissing, ob.fromUnicode, None)
        self.assertRaises(RequiredMissing, ob.toUnicode, u'')
        self.assertRaises(RequiredMissing, ob.toDbType, u'')
        #self.assertRaises(RequiredMissing, ob.fromDbType, u'')
        #self.assertRaises(RequiredMissing, ob.fromDbType, None)

        self.assertEqual(None, ob.fromDbType(u''))
        self.assertEqual(None, ob.fromDbType(None))
        ob.default = dt(2009,12,31,0,0,0)
        self.assertEqual(dt(2009,12,31,0,0,0), ob.fromDbType(u''))
        self.assertEqual(dt(2009,12,31,0,0,0), ob.fromDbType(None))

    def testDate(self):
        """Test field `Date`"""
        ob = Date('column_name', required=False)
        self.assertTrue(IORMField.providedBy(ob))
        self.assertTrue(zope.schema.interfaces.IDate.providedBy(ob))
        
        dt = datetime
        d = date

        self.assertEqual(d(2009,12,31), ob.fromUnicode(u'2009-12-31'))
        self.assertEqual(d(2009,12,31), ob.fromUnicode(u'31.12.2009'))
        self.assertEqual(d(2009,12,31), ob.fromUnicode(u'31.12.2009 23:59:00'))
        self.assertEqual(None, ob.fromUnicode(u''))
        self.assertEqual(None, ob.fromUnicode(None))

        self.assertEqual(u'31.12.2009', ob.toUnicode(d(2009,12,31)))
        self.assertEqual(u'31.12.2009', ob.toUnicode(dt(2009,12,31,23,59,59)))
        self.assertEqual(u'', ob.toUnicode(u''))
        self.assertEqual(u'', ob.toUnicode(None))

        self.assertEqual(d(2009,12,31), ob.fromDbType(u'2009-12-31'))
        self.assertEqual(d(2009,12,31), ob.fromDbType(u'31.12.2009'))
        self.assertEqual(d(2009,12,31), ob.fromDbType(u'31.12.2009 23:59:00'))
        self.assertEqual(d(2009,12,31), ob.fromDbType(dt(2009,12,31,23,59,0,99)))
        self.assertEqual(None, ob.fromDbType(u''))
        self.assertEqual(None, ob.fromDbType(None))

        self.assertEqual(d(2009,12,31), ob.toDbType(dt(2009,12,31,23,59,0,99)))
        self.assertEqual(None, ob.toDbType(u''))
        self.assertEqual(None, ob.toDbType(None))

        ob.format = '%d|%m|%Y'
        self.assertEqual(u'31|12|2009', ob.toUnicode(d(2009,12,31)))
        
        ob.required = True
        self.assertRaises(RequiredMissing, ob.fromUnicode, u'')
        self.assertRaises(RequiredMissing, ob.fromUnicode, None)
        self.assertRaises(RequiredMissing, ob.toUnicode, u'')
        self.assertRaises(RequiredMissing, ob.toDbType, u'')
        #self.assertRaises(RequiredMissing, ob.fromDbType, u'')
        #self.assertRaises(RequiredMissing, ob.fromDbType, None)

        self.assertEqual(None, ob.fromDbType(u''))
        self.assertEqual(None, ob.fromDbType(None))
        ob.default = d(2009,12,31)
        self.assertEqual(d(2009,12,31), ob.fromDbType(u''))
        self.assertEqual(d(2009,12,31), ob.fromDbType(None))

    def testTime(self):
        """Test field `Time`"""
        ob = Time('column_name', required=False)
        self.assertTrue(IORMField.providedBy(ob))
        self.assertTrue(zope.schema.interfaces.ITime.providedBy(ob))
        
        dt = datetime
        t = time

        self.assertEqual(t(23,59,0), ob.fromUnicode(u'23:59:00'))
        self.assertEqual(t(23,59,0), ob.fromUnicode(u'2009-12-31 23:59:00'))
        self.assertEqual(None, ob.fromUnicode(u''))
        self.assertEqual(None, ob.fromUnicode(None))

        self.assertEqual(u'23:59', ob.toUnicode(dt(2009,12,31,23,59,0)))
        self.assertEqual(u'23:59', ob.toUnicode(t(23,59,0)))
        self.assertEqual(u'', ob.toUnicode(u''))
        self.assertEqual(u'', ob.toUnicode(None))

        self.assertEqual(t(23,59,0), ob.fromDbType(u'23:59:00'))
        self.assertEqual(t(23,59,0), ob.fromDbType(u'2009-12-31 23:59:00'))
        self.assertEqual(t(23,59,0), ob.fromDbType(dt(2009,12,31,23,59,0)))
        self.assertEqual(t(23,59,0), ob.fromDbType(t(23,59,0)))
        self.assertEqual(None, ob.fromDbType(u''))
        self.assertEqual(None, ob.fromDbType(None))

        self.assertEqual(t(23,59,0), ob.toDbType(dt(2009,12,31,23,59,0)))
        self.assertEqual(t(23,59,0), ob.toDbType(t(23,59,0)))
        self.assertEqual(None, ob.toDbType(u''))
        self.assertEqual(None, ob.toDbType(None))

        ob.format = '%H/%M/%S'
        self.assertEqual(u'23/59/00', ob.toUnicode(t(23,59,0)))
        
        ob.required = True
        self.assertRaises(RequiredMissing, ob.fromUnicode, u'')
        self.assertRaises(RequiredMissing, ob.fromUnicode, None)
        self.assertRaises(RequiredMissing, ob.toUnicode, u'')
        self.assertRaises(RequiredMissing, ob.toDbType, u'')
        #self.assertRaises(RequiredMissing, ob.fromDbType, u'')
        #self.assertRaises(RequiredMissing, ob.fromDbType, None)

        self.assertEqual(None, ob.fromDbType(u''))
        self.assertEqual(None, ob.fromDbType(None))
        ob.default = t(23,59,0)
        self.assertEqual(t(23,59,0), ob.fromDbType(u''))
        self.assertEqual(t(23,59,0), ob.fromDbType(None))


if __name__ == '__main__':
    testoob.main()
