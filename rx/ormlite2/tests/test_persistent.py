# -*- coding: utf-8 -*-
"""Persistent and Record tests

$Id: test_persistent.py 930 2012-02-01 08:28:30Z vmozhaev $
"""
from datetime import date
import unittest
import testoob
import transaction

from persistent import Persistent
from zope.interface import implements
from zope.schema.interfaces import IBaseVocabulary
from zope.interface import Interface
from ZODB.POSException import ConflictError

from rx.ormlite2 import schema, PersistenceError
from rx.ormlite2.cache import MutableElement
from rx.ormlite2.dbop import dbquery
from rx.ormlite2.interfaces import IRecord
from rx.ormlite2.record import Record
from pyramid.registry import registerAdapter, unregisterAdapter
from pyramid.vocabulary.simple import SimpleVocabulary
import pyramid.model.interfaces
import rx.ormlite2.interfaces

from pyramid.tests import testlib
import testdata


testVocab = SimpleVocabulary(items=((1,), (2,), (3,)))

class _ITestOb(Interface):
    p1 = schema.TextLine(db_column='c1')
    p2 = schema.TextLine(db_column='c2')
    p3 = schema.TextLine(db_column='c3')
    p4 = schema.TextLine(db_column='c4')
    p5 = schema.Choice(db_column='c5', vocabulary=testVocab)
    p6 = schema.Choice(db_column='c6', vocabulary=testVocab)

class _TestOb(MutableElement):
    implements(_ITestOb)
    p_table_name='__fake_table'


class _TestPersistent(Persistent):
    def __init__(self, id):
        self.id = id
    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return (self.id == other.id)
        return False
    def __ne__(self, other):
        if isinstance(other, self.__class__):
            return (self.id != other.id)
        return True
    def __cmp__(self, other):
        if isinstance(other, self.__class__):
            return cmp(self.id, other.id)
        return cmp(hash(self), hash(other))
    def __hash__(self):
        return hash(self.id)
    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, self.id)


class TestRecord(unittest.TestCase):
    """Tests generic Record object"""

    def test_Record_with_multiple_interfaces(self):

        class I1(Interface):
            attr1 = schema.Int(db_column="attr_1", primary_key=True)
            attr2 = schema.TextLine(db_column="attr_2")
            attr3 = schema.Bool(db_column="attr_3")

        class I2(I1):
            attr3 = schema.Int(db_column="attr_3")
            attr4 = schema.Int(db_column="attr_4")

        class I3(Interface):
            attr4 = schema.Int(db_column="attr_5", primary_key=True)

        class C1(Record):
            implements(I3, I2)

        obj = C1()
        self.assertEqual(4, len(obj.p_fields))
        self.assertEqual(4, len(obj.p_attr2col))
        self.assertEqual(4, len(obj.p_col2attr))
        self.assertEqual(4, len(obj.p_attr_seq))
        self.assertEqual(4, len(obj.p_col_seq))

        self.assertEqual(2, len(obj.p_keys))
        self.assertEqual(['attr1', 'attr4'], sorted(obj.p_keys))
        self.assertEqual(2, len(obj.p_key_fields))
        self.assertEqual(['attr_1', 'attr_5'], sorted(obj.p_key_fields))

        self.assertTrue(obj.p_fields['attr1'].primary_key)
        self.assertFalse(obj.p_fields['attr2'].primary_key)
        self.assertTrue(isinstance(obj.p_fields['attr3'], schema.Int))
        self.assertTrue(obj.p_fields['attr4'].primary_key)
        self.assertEqual('attr_5', obj.p_fields['attr4'].db_column)


    def test_Record_with_default_field_values(self):
        """Tests that Record accepts default values from field declaration"""

        class _ITest(IRecord):
            field1 = schema.Int(db_column='filed_1')
            field2 = schema.Int(db_column='filed_2', default=10)
            field3 = schema.Bool(db_column='filed_3')
            field4 = schema.Bool(db_column='filed_4', default=True)

        class _Test(Record):
            implements(_ITest)

        r = _Test()
        self.assertEqual(None, r.field1)
        self.assertEqual(10, r.field2)
        self.assertEqual(False, r.field3)
        self.assertEqual(True, r.field4)


    def test_Record_copy_method(self):

        class V1(object):
            implements(IBaseVocabulary)
            pass

        class I1(Interface):
            attr1 = schema.Int(db_column="attr_1", primary_key=True)
            attr2 = schema.TextLine(db_column="attr_2")
            attr3 = schema.Choice(db_column="attr_3", vocabulary=V1())

        class C1(Record):
            implements(I1)

        obj = C1(attr1=1, attr2="test attr2")
        obj_copy = obj.copy()
        self.assertEqual(obj.attr1, obj_copy.attr1)
        self.assertEqual(obj.attr2, obj_copy.attr2)
        self.assertEqual(obj.attr3, obj_copy.attr3)
        self.assertFalse(obj_copy.p_choice_tokens is obj.p_choice_tokens)


class TestConflictResolvers(testlib.TestCaseWithThreads):

#    def tearDown(self):
#        super(TestConflictResolvers, self).tearDown()
#        from rx.ormlite2.cache import con
#        cache = lambda: con().root()
#        if '__ob' in cache():
#            del cache()['__ob']
#            transaction.commit()

    def testMutableElement(self):
        from rx.ormlite2.cache import con
        cache = lambda: con().root()

        def _setV(params):
            ob = cache()['__ob']
            for name, value in params.items():
                setattr(ob, name, value)

        def _addSubsriber(sub):
            ob = cache()['__ob']
            if isinstance(sub, basestring):
                sub = cache()[sub]
            ob.p_afterChange.append(sub)

        def _delSubsriber(sub):
            ob = cache()['__ob']
            if isinstance(sub, basestring):
                sub = cache()[sub]
            ob.p_afterChange.remove(sub)

        cache()['__ob'] = _TestOb(p1='a', p2='a', p3='a', p4='a', p5=1, p6=1)
        transaction.commit()

        def d(*args):
            # Parameters dict from the list of values (p1=args[0], p2=args[1], etc)
            _d = dict()
            for i in range(len(args)):
                _d['p%s' % (i+1)] = args[i]
            return _d

        # Test for fields merge
        self._run_conflicting_threads(_setV,
            t1_params=(d('a', 'b', 'a', 'd', 2, 2),),
            t2_params=(d('a', 'a', 'c', 'e', 3, 1),))
        ob = cache()['__ob']
        self.assertEqual('a', ob.p1)
        self.assertEqual('b', ob.p2)
        self.assertEqual('c', ob.p3)
        self.assertEqual('e', ob.p4)
        self.assertEqual(3, ob.p5)
        self.assertEqual(2, ob.p6)

        ob1 = _TestPersistent(1)
        ob2 = _TestPersistent(2)
        ob3 = _TestPersistent(3)
        ob1_eq = _TestPersistent(1)
        self.assertEqual(ob1, ob1)
        self.assertEqual(ob1, ob1_eq)
        self.assertNotEqual(ob1, ob2)

        self.assertFalse(ob1 in ob.p_afterChange)
        self.assertFalse(ob2 in ob.p_afterChange)
        self.assertFalse(ob3 in ob.p_afterChange)

        cache()['__ob3'] = ob3
        transaction.commit()

        # Test for change event subscriber adding - different subscribers
        self._run_conflicting_threads(_addSubsriber, (ob1,), (ob2,))
        ob = cache()['__ob']
        self.assertTrue(ob1 in ob.p_afterChange)
        self.assertTrue(ob2 in ob.p_afterChange)

        # Test for change event subscriber adding - same subscriber
        self._run_conflicting_threads(_addSubsriber, ('__ob3',), ('__ob3',))
        ob = cache()['__ob']
        self.assertTrue(ob1 in ob.p_afterChange)
        self.assertTrue(ob2 in ob.p_afterChange)
        self.assertTrue(ob3 in ob.p_afterChange)
        self.assertEqual([], self.conflict_errors)

        # Test for change event subscriber deleting - different subscribers
        self._run_conflicting_threads(_delSubsriber, (ob1,), (ob2,))
        ob = cache()['__ob']
        self.assertFalse(ob1 in ob.p_afterChange)
        self.assertFalse(ob2 in ob.p_afterChange)

        # Test for change event subscriber deleting - same subscriber
        self._run_conflicting_threads(_delSubsriber, ('__ob3',), ('__ob3',))
        ob = cache()['__ob']
        self.assertFalse(ob3 in ob.p_afterChange)
        self.assertEqual([], self.conflict_errors)

    def testPersistentMapping(self):
        from rx.ormlite2.cache import con
        from rx.ormlite2.vocabulary.resolve import PersistentMappingConflictErrorResolve
        cache = lambda: con().root()
        cache()['__ob'] = PersistentMappingConflictErrorResolve()
        transaction.commit()

        def _addKey(key):
            ob = cache()['__ob']
            ob[key] = 1

        def _delKey(key):
            ob = cache()['__ob']
            del ob[key]

        def _changeKey(key, value):
            ob = cache()['__ob']
            ob[key] = value

        def _addOb(item):
            ob = cache()['__ob']
            if isinstance(item, basestring):
                item = cache()[item]
            ob[item] = 1

        def _changeOb(item, value):
            ob = cache()['__ob']
            if isinstance(item, basestring):
                item = cache()[item]
            ob[item] = value

        ob1 = _TestPersistent(1)
        ob2 = _TestPersistent(2)
        ob3 = _TestPersistent(3)
        cache()['__ob3'] = ob3
        transaction.commit()

        ob = cache()['__ob']
        self.assertFalse('p1' in ob)
        self.assertFalse('p2' in ob)
        self.assertFalse('p3' in ob)
        self.assertFalse(ob1 in ob)
        self.assertFalse(ob2 in ob)
        self.assertFalse(ob3 in ob)

        # Test for different objects adding
        self._run_conflicting_threads(_addKey, ('p1',), ('p2',))
        self.assertEqual([], self.conflict_errors)
        ob = cache()['__ob']
        self.assertTrue('p1' in ob)
        self.assertTrue('p2' in ob)

        # Test for different persistent objects adding
        self._run_conflicting_threads(_addOb, (ob1,), (ob2,))
        self.assertEqual([], self.conflict_errors)
        ob = cache()['__ob']
        self.assertTrue(ob1 in ob)
        self.assertTrue(ob2 in ob)

        # Test for same object adding
        self._run_conflicting_threads(_addKey, ('p3',), ('p3',))
        self.assertEqual([2], self.conflict_errors) # T2 raised a ConflictError
        ob = cache()['__ob']
        self.assertTrue('p1' in ob)
        self.assertTrue('p2' in ob)
        self.assertTrue('p3' in ob)

        # Test for same persistent object adding
        self._run_conflicting_threads(_addOb, ('__ob3',), ('__ob3',))
        self.assertEqual([2], self.conflict_errors) # T2 raised a ConflictError
        ob = cache()['__ob']
        self.assertTrue(ob3 in ob)

        # Test for same object editing
        self._run_conflicting_threads(_changeKey, ('p1', 2), ('p1', 1))
        self.assertEqual([], self.conflict_errors)
        ob = cache()['__ob']
        self.assertEqual(2, ob['p1'])

        # Test for same persistent object editing
        self._run_conflicting_threads(_changeOb, ('__ob3', 2), ('__ob3', 1))
        self.assertEqual([], self.conflict_errors)
        ob = cache()['__ob']
        self.assertEqual(2, ob[ob3])
        self._run_conflicting_threads(_changeOb, ('__ob3', 3), ('__ob3', 4))
        self.assertEqual([], self.conflict_errors)
        ob = cache()['__ob']
        self.assertEqual(4, ob[ob3])

        # Test for different objects deleting
        self._run_conflicting_threads(_delKey, ('p1',), ('p2',))
        self.assertEqual([], self.conflict_errors)
        ob = cache()['__ob']
        self.assertFalse('p1' in ob)
        self.assertFalse('p2' in ob)
        self.assertTrue('p3' in ob)

        # Test for same object deleting
        self._run_conflicting_threads(_delKey, ('p3',), ('p3',))
        self.assertEqual([], self.conflict_errors) # No conflicts if same key deleted
        ob = cache()['__ob']
        self.assertFalse('p3' in ob)

    def testPersistentList(self):
        from rx.ormlite2.cache import con
        from rx.ormlite2.vocabulary.resolve import PersistentListConflictErrorResolve
        cache = lambda: con().root()

        cache()['__ob'] = PersistentListConflictErrorResolve()
        transaction.commit()

        def _addItem(item):
            ob = cache()['__ob']
            if isinstance(item, basestring):
                item = cache()[item]
            ob.append(item)

        def _delItem(item):
            ob = cache()['__ob']
            if isinstance(item, basestring):
                item = cache()[item]
            ob.remove(item)

        ob = cache()['__ob']

        ob1 = _TestPersistent(1)
        ob2 = _TestPersistent(2)
        ob3 = _TestPersistent(3)
        ob1_eq = _TestPersistent(1)
        self.assertEqual(ob1, ob1)
        self.assertEqual(ob1, ob1_eq)
        self.assertNotEqual(ob1, ob2)

        self.assertFalse(ob1 in ob)
        self.assertFalse(ob2 in ob)
        self.assertFalse(ob3 in ob)
        self.assertFalse('foo' in ob)

        cache()['__ob3'] = ob3
        transaction.commit()

        # Test for different objects adding
        self._run_conflicting_threads(_addItem, (ob1,), (ob2,))
        ob = cache()['__ob']
        self.assertTrue(ob1 in ob)
        self.assertTrue(ob2 in ob)
        self.assertEqual(2, len(ob))
        self.assertEqual([], self.conflict_errors)

        # Test for same object adding
        self._run_conflicting_threads(_addItem, ('__ob3',), ('__ob3',))
        ob = cache()['__ob']
        self.assertTrue(ob1 in ob)
        self.assertTrue(ob2 in ob)
        self.assertTrue(ob3 in ob)
        self.assertEqual(3, len(ob)) # Only one copy of 'ob3' should be added
        self.assertEqual([], self.conflict_errors)

        # Test for non-persistent object adding
        self._run_conflicting_threads(_addItem, (123,), (123,))
        ob = cache()['__ob']
        self.assertTrue(123 in ob)
        self.assertEqual(4, len(ob)) # Only one copy of 'ob3' should be added
        self.assertEqual([], self.conflict_errors)

        # Test for different objects deleting
        self._run_conflicting_threads(_delItem, (ob1,), (ob2,))
        ob = cache()['__ob']
        self.assertFalse(ob1 in ob)
        self.assertFalse(ob2 in ob)
        self.assertTrue(ob3 in ob)
        self.assertTrue(123 in ob)
        self.assertEqual(2, len(ob))
        self.assertEqual([], self.conflict_errors)

        # Test for same object deleting
        self._run_conflicting_threads(_delItem, ('__ob3',), ('__ob3',))
        ob = cache()['__ob']
        self.assertFalse(ob3 in ob)
        self.assertTrue(123 in ob)
        self.assertEqual(1, len(ob))
        self.assertEqual([], self.conflict_errors)

        # Test for non-persistent object deleting
        self._run_conflicting_threads(_delItem, (123,), (123,))
        ob = cache()['__ob']
        self.assertFalse(123 in ob)
        self.assertEqual(0, len(ob))
        self.assertEqual([], self.conflict_errors)


    def testBTreeMapping(self):
        from rx.ormlite2.cache import con
        from BTrees.OOBTree import OOBTree
        cache = lambda: con().root()
        cache()['__ob'] = OOBTree()
        transaction.commit()

        def _addKey(key):
            ob = cache()['__ob']
            ob[key] = 1

        def _delKey(key):
            ob = cache()['__ob']
            del ob[key]

        def _changeKey(key, value):
            ob = cache()['__ob']
            ob[key] = value

        def _addOb(item):
            ob = cache()['__ob']
            if isinstance(item, basestring):
                item = cache()[item]
            ob[item] = 1

        def _changeOb(item, value):
            ob = cache()['__ob']
            if isinstance(item, basestring):
                item = cache()[item]
            ob[item] = value

        ob1 = _TestPersistent(1)
        ob2 = _TestPersistent(2)
        ob3 = _TestPersistent(3)
        cache()['__ob3'] = ob3
        transaction.commit()

        ob = cache()['__ob']
        self.assertFalse('p1' in ob)
        self.assertFalse('p2' in ob)
        self.assertFalse('p3' in ob)
        self.assertFalse(ob1 in ob)
        self.assertFalse(ob2 in ob)
        self.assertFalse(ob3 in ob)

        # Test for different objects adding
        self._run_conflicting_threads(_addKey, ('p1',), ('p2',))
        self.assertEqual([], self.conflict_errors)
        ob = cache()['__ob']
        self.assertTrue('p1' in ob)
        self.assertTrue('p2' in ob)

        # Test for different persistent objects adding
        self._run_conflicting_threads(_addOb, (ob1,), (ob2,))
        self.assertEqual([2], self.conflict_errors) # T2 raised a ConflictError
        ob = cache()['__ob']
        self.assertTrue(ob1 in ob)
        self.assertTrue(ob2 not in ob)

        # Test for same object adding
        self._run_conflicting_threads(_addKey, ('p3',), ('p3',))
        self.assertEqual([2], self.conflict_errors) # T2 raised a ConflictError
        ob = cache()['__ob']
        self.assertTrue('p1' in ob)
        self.assertTrue('p2' in ob)
        self.assertTrue('p3' in ob)

        # Test for same persistent object adding
        self._run_conflicting_threads(_addOb, ('__ob3',), ('__ob3',))
        self.assertEqual([2], self.conflict_errors) # T2 raised a ConflictError
        ob = cache()['__ob']
        self.assertTrue(ob3 in ob)

        # Test for same object editing
        self._run_conflicting_threads(_changeKey, ('p1', 2), ('p1', 1))
        self.assertEqual([], self.conflict_errors)
        ob = cache()['__ob']
        self.assertEqual(2, ob['p1'])

        # Test for same persistent object editing
        self._run_conflicting_threads(_changeOb, ('__ob3', 2), ('__ob3', 1))
        self.assertEqual([], self.conflict_errors)
        ob = cache()['__ob']
        self.assertEqual(2, ob[ob3])
        self._run_conflicting_threads(_changeOb, ('__ob3', 3), ('__ob3', 4))
        self.assertEqual([2], self.conflict_errors) # T2 raised a ConflictError
        ob = cache()['__ob']
        self.assertEqual(3, ob[ob3])

        # Test for different objects deleting
        self._run_conflicting_threads(_delKey, ('p1',), ('p2',))
        self.assertEqual([], self.conflict_errors)
        ob = cache()['__ob']
        self.assertFalse('p1' in ob)
        self.assertFalse('p2' in ob)
        self.assertTrue('p3' in ob)

        # Test for same object deleting
        self._run_conflicting_threads(_delKey, ('p3',), ('p3',))
        self.assertEqual([2], self.conflict_errors) # T2 raised a ConflictError
        ob = cache()['__ob']
        self.assertFalse('p3' in ob)


if __name__ == '__main__':
    testoob.main()
