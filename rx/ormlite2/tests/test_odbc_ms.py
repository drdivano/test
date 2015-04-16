# -*- coding: utf-8 -*-

"""Microsoft SQL Server ODBC (pyodbc) DB Adapter tests.

$Id $
"""

import unittest
import testoob
import transaction

from zope.component import provideUtility
from zope.interface.declarations import implements

from rx.ormlite2.record import ActiveRecord
from rx.ormlite2.interfaces import IDatabaseAdapter, IActiveRecord
from rx.ormlite2 import schema

from pyramid.utils import Config as config
from pyramid.tests.testlib import TestCaseWithOdbcMsDB, TestCaseWithThreads
from pyramid.tests.testlib import TestCaseWithDBAndVocabs

from _test_common import CommonTests
from _test_vocab import VocabTests
import testdata

try:
    from rx.ormlite2 import odbcmscon
except ImportError:
    odbcmscon = None
else:
    provideUtility(odbcmscon, IDatabaseAdapter)



class TestMs(CommonTests, VocabTests, TestCaseWithOdbcMsDB, TestCaseWithThreads,
    TestCaseWithDBAndVocabs):

    def setUp(self):
        super(TestMs, self).setUp()
        transaction.begin()
        testdata.deleteAllFrom('ut__orm_months',
                               'ut__orm_reports',
                               'ut__orm_lob_test',
                               'ut__orm_users',
                               'ut__orm_units',
                               )
        testdata._initMonthsData()
        testdata._initReportsData()
        testdata._initUnitsData()
        testdata._initOrmUsersData()
        testdata._initLOBData()
        transaction.commit()

    def tearDown(self):
        transaction.begin()
        testdata.deleteAllFrom('ut__orm_months',
                               'ut__orm_reports',
                               'ut__orm_lob_test',
                               'ut__orm_users',
                               'ut__orm_units',
                               )
        transaction.commit()
        super(TestMs, self).tearDown()

    def testLOB(self):
        class IOb(IActiveRecord):
            recId = schema.Int(db_column="k",
                               primary_key=True, readonly=True)
            data = schema.LOB(db_column="data_lob", required=False)

        class Ob(ActiveRecord):
            implements(IOb)
            p_table_name = 'ut__orm_lob_test'

        ob = Ob(recId=22)
        self.assertTrue(isinstance(ob.data, odbcmscon.LOBIO))

        datalen = ob.data.write('xxx zzz')
        self.assertEquals(datalen, 7)
        ob.save()

        ob2 = Ob.load(recId=22)
        self.assertTrue(isinstance(ob.data, odbcmscon.LOBIO))
        self.assertTrue(ob2.data._io is None)
        self.assertEquals(ob2.data.read(), 'xxx zzz')
        self.assertTrue(ob2.data._io is not None)

        ob2.data.truncate(3)
        ob2.save()
        ob3 = Ob.load(recId=22)
        self.assertEquals(ob3.data.read(), 'xxx')

        # test for bibary >4K
        ob3.data.write('\x00'*(4*1024+1))
        ob3.save()
        ob4 = Ob.load(recId=22)
        self.assertEqual(4*1024+1, len(ob4.data.read()))

        # test for bibary >400K
        ob4.data.write('\x00'*(400*1024+1))
        ob4.save()
        ob5 = Ob.load(recId=22)
        self.assertEqual(400*1024+1, len(ob5.data.read()))

        # test for bibary >4M
        ob5.data.write('\x00'*(4*1024*1024+1))
        ob5.save()
        ob6 = Ob.load(recId=22)
        self.assertEqual(4*1024*1024+1, len(ob6.data.read()))

    def testDeferConstraints(self):
        testoob.testing.skip('Deferrable Constraints are not supported by MS SQL Server')


if config.MS_ODBC_DSN is None or odbcmscon is None:

    class TestMs(unittest.TestCase):

        def testMs(self):
            testoob.testing.skip('No configuration for MS SQL Server (pyodbc) tests')


if __name__ == "__main__":
    testoob.main()
