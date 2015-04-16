# -*- coding: utf-8 -*-

"""OracleL ODBC (pyodbc) DB Adapter tests.

$Id: _test_odbc_ora.py 882 2011-03-04 14:48:45Z vmozhaev $
"""

import unittest
import testoob
import transaction

from zope.component import provideUtility

from rx.ormlite2.interfaces import IDatabaseAdapter
from rx.ormlite2 import odbcoracon

from pyramid.utils import Config as config
from pyramid.tests.testlib import TestCaseWithOdbcOraDB, TestCaseWithThreads
from pyramid.tests.testlib import TestCaseWithDBAndVocabs

from _test_common import CommonTests
from _test_vocab import VocabTests
import testdata


provideUtility(odbcoracon, IDatabaseAdapter)


class TestOra(CommonTests, VocabTests, TestCaseWithOdbcOraDB, TestCaseWithThreads,
    TestCaseWithDBAndVocabs):

    def setUp(self):
        super(TestOra, self).setUp()
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
        super(TestOra, self).tearDown()


if config.ORA_ODBC_DSN is None:

    class TestOra(unittest.TestCase):

        def testOra(self):
            testoob.testing.skip('No configuration for Oracle (pyodbc) tests')


if __name__ == "__main__":
    testoob.main()
