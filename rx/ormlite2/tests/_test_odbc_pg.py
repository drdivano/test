# -*- coding: utf-8 -*-

"""PostgreSQL ODBC (pyodbc) DB Adapter tests.

$Id: _test_odbc_pg.py 882 2011-03-04 14:48:45Z vmozhaev $
"""

import unittest
import testoob
import transaction

from zope.component import provideUtility

from rx.ormlite2.interfaces import IDatabaseAdapter
from rx.ormlite2 import odbcpgcon
from pyramid.utils import Config as config
from pyramid.tests.testlib import TestCaseWithOdbcPgDB, TestCaseWithThreads
from pyramid.tests.testlib import TestCaseWithDBAndVocabs

from _test_common import CommonTests
from _test_vocab import VocabTests
import testdata


provideUtility(odbcpgcon, IDatabaseAdapter)


class TestPg(CommonTests, VocabTests, TestCaseWithOdbcPgDB, TestCaseWithThreads,
    TestCaseWithDBAndVocabs):

    def setUp(self):
        super(TestPg, self).setUp()
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
        super(TestPg, self).tearDown()


if config.PG_ODBC_DSN is None:

    class TestPg(unittest.TestCase):

        def testPg(self):
            testoob.testing.skip('No configuration for PostgreSQL (pyodbc) tests')


if __name__ == "__main__":
    testoob.main()
