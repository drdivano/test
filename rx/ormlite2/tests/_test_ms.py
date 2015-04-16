# -*- coding: utf-8 -*-

"""MS SQL Server (pymssql) DB Adapter tests.

$Id: _test_ms.py 882 2011-03-04 14:48:45Z vmozhaev $
"""

import testoob
import transaction
import pymssql

from zope.component.globalregistry import provideUtility

from rx.ormlite2.interfaces import IDatabaseAdapter
from pyramid.utils import Config as config
from pyramid.tests.testlib import TestCaseWithMsDB
from pyramid.tests.testlib import TestCaseWithMsDB, TestCaseWithThreads
from pyramid.tests.testlib import TestCaseWithDBAndVocabs

import testdata
from _test_common import CommonTests
from _test_vocab import VocabTests


provideUtility(pymssql, IDatabaseAdapter)


class Test(CommonTests, VocabTests, TestCaseWithMsDB, TestCaseWithThreads,
    TestCaseWithDBAndVocabs):

    def setUp(self):
        super(Test, self).setUp()
        transaction.begin()
        testdata.deleteAllFrom('ut__orm_months',
                               'ut__orm_reports',
                               'ut__orm_users',
                               'ut__orm_units',
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
                               )
        transaction.commit()
        super(Test, self).tearDown()

    def testDeferConstraints(self):
        testoob.testing.skip('Deferrable Constraints are not supported by MS SQL Server')

#    def testLOB(self):
#        testoob.testing.skip('LOBs are not supported by pymssql DB adapter')


# Disable test if MS SQL Server DB Connection is not configured
if config.MSSQL_HOST is None:
    import unittest

    class Test(unittest.TestCase):

        def testMs(self):
            testoob.testing.skip('No configuration for MS SQL Server (pymssql) tests')


if __name__ == "__main__":
    testoob.main()
