# -*- coding: utf-8 -*-

"""Oracle (cx_Oracle) DB Adapter tests.

$Id: test_ora.py 1001 2012-11-12 13:42:19Z vmozhaev $
"""

import testoob
import transaction

from zope.interface import implements
from zope.component import provideUtility

from rx.ormlite2 import schema
from rx.ormlite2.interfaces import IDatabaseAdapter, IActiveRecord
from rx.ormlite2.record import ActiveRecord

from pyramid.utils import Config as config
from pyramid.tests.testlib import TestCaseWithOraDB, TestCaseWithThreads
from pyramid.tests.testlib import TestCaseWithDBAndVocabs

import testdata
from _test_common import CommonTests
from _test_vocab import VocabTests


try:
    from rx.ormlite2 import cxoracon
except ImportError:
    cxoracon = None
else:
    provideUtility(cxoracon, IDatabaseAdapter)


class _IOb(IActiveRecord):
    recId = schema.Int(db_column="k", primary_key=True, readonly=True)
    data = schema.LOB(db_column="data_lob", required=False)

class _Ob(ActiveRecord):
    implements(_IOb)
    p_table_name = 'ut__orm_lob_test'


class Test(CommonTests, VocabTests, TestCaseWithThreads, TestCaseWithOraDB, TestCaseWithDBAndVocabs):

    def setUp(self):
        super(Test, self).setUp()
        transaction.begin()
        testdata.deleteAllFrom('ut__orm_months',
                               'ut__orm_reports',
                               'ut__orm_users',
                               'ut__orm_units',
                               'ut__orm_lob_test',
                               'ut__orm_test',
                               )
        testdata._initMonthsData()
        testdata._initReportsData()
        testdata._initUnitsData()
        testdata._initOrmUsersData()
        transaction.commit()

    def tearDown(self):
        transaction.begin()
        testdata.deleteAllFrom('ut__orm_months',
                               'ut__orm_reports',
                               'ut__orm_users',
                               'ut__orm_units',
                               'ut__orm_lob_test',
                               'ut__orm_test',
                               )
        transaction.commit()
        super(Test, self).tearDown()

    def testLob(self):

        ob = _Ob(recId=22)
        self.assertTrue(isinstance(ob.data, cxoracon.LOBIO))

        datalen = ob.data.write('xxx zzz')
        self.assertEquals(datalen, 7)
        ob.save()

        ob2 = _Ob.load(recId=22)
        self.assertTrue(isinstance(ob.data, cxoracon.LOBIO))
        self.assertEquals(ob2.data.read(), 'xxx zzz')

        ob2.data.truncate(3)
        ob2 = _Ob.load(recId=22)
        self.assertEquals(ob2.data.read(), 'xxx')
        ob2.save()
        ob2 = _Ob.load(recId=22)

        ob2.data.write('\01' * 5000)
        ob2.save()
        ob2 = _Ob.load(recId=22)
        self.assertEquals(ob2.data.read(), '\01' * 5000)

        # Bug with chunk size in use (the default is 8132)
        ob2.data.write('\01' * 9000)
        ob2.save()
        ob2 = _Ob.load(recId=22)
        self.assertEquals(ob2.data.read(), '\01' * 9000)

        # Test for string storing as blob
        ob2.data.write((u'Я' * 5000).encode('utf8'))  # Byte-strings only!
        ob2.save()
        ob2 = _Ob.load(recId=22)
        self.assertEquals(unicode(ob2.data.read(), 'utf8'), u'Я' * 5000)

    def testLobSubsequentFetching(self):
        # Test for Oracle's subsequent fetch problem
        for id in range(100):  # >= 100 (default array size)
            newOb = _Ob(recId=id)
            newOb.data.write('abc %s' % id)
            newOb.save()

        lst = _Ob.getAll()
        self.assertEqual(100, len(lst))
        lst.sort(key=lambda o: o.recId)

        self.assertEqual('abc 0', lst[0].data.read())
        self.assertEqual('abc 50', lst[50].data.read())
        self.assertEqual('abc 99', lst[99].data.read())

        # Test for LOB pickling
        import pickle

        # valid because already reloaded
        s0 = pickle.dumps(lst[0].data)
        ob0 = pickle.loads(s0)
        self.assertEqual('abc 0', ob0.read())

        # invalid
        s1 = pickle.dumps(lst[1].data)
        ob1 = pickle.loads(s1)
        self.assertEqual('abc 1', ob1.read())

    def testLobWriteIssues(self):
        # First-time created LOBs have no issues
        transaction.begin()
        ob1 = _Ob(recId=1)
        ob1.data.write('test data')
        ob1.save()
        ob2 = _Ob(recId=2)
        ob2.data.write('test data')
        ob2.save()
        transaction.commit()

        # Test for ORA-22292: Cannot open a LOB in read-write mode without
        # a transaction

        # Write in new transaction but using the same object
        transaction.begin()
        ob1.data.truncate()
        ob1.data.write('new test data')
        transaction.commit()

        # Write in new transaction but using a new object
        import cx_Oracle
        transaction.begin()
        ob1 = _Ob.load(recId=1)
        try:
            ob1.data.truncate()
            ob1.data.write('even newer test data')
        except cx_Oracle.DatabaseError, e:
            if str(e).startswith('ORA-22292:'):
                self.fail('Record must be opened for update before LOB write')
            raise
        transaction.commit()

        # Test for ORA-22920: row containing the LOB value is not locked

        transaction.begin()
        # First object has no issues during writing
        ob1 = _Ob.load(recId=1)
        ob1.data.truncate()
        ob1.data.write('new data')
        # But the second had a error!
        ob2 = _Ob.load(recId=2)
        try:
            ob2.data.truncate()
            ob2.data.write('new data')
        except cx_Oracle.DatabaseError, e:
            if str(e).startswith('ORA-22920:'):
                self.fail('Record must be locked for update before LOB write')
            raise
        transaction.commit()

    def testFakeLOBIsNotPickling(self):
        ob = _Ob(recId=1)
        self.assertTrue(isinstance(ob.data, cxoracon.LOBIO))
        self.assertTrue(isinstance(ob.data.ob, cxoracon._FakeLOB))

        import pickle
        p = pickle.dumps(ob)
        ob1 = pickle.loads(p)
        self.assertTrue(isinstance(ob1.data, cxoracon.LOBIO))
        self.assertTrue(ob1.data.ob is None)


# Disable test if Oracle DB Connection is not configured
if config.ORACLE_HOST is None or cxoracon is None:
    import unittest

    class Test(unittest.TestCase):

        def testOra(self):
            testoob.testing.skip('No configuration for Oracle (cx_Oracle) tests')


if __name__ == "__main__":
    testoob.main()
