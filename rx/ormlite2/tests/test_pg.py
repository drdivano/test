# -*- coding: utf-8 -*-

"""PostgreSQL (psycopg2) DB Adapter tests.

$Id: test_pg.py 994 2012-10-23 21:34:06Z anovgorodov $
"""

import testoob
import transaction

from zope.interface import implements
from zope.component import provideUtility

from rx.ormlite2.record import ActiveRecord
from rx.ormlite2.dbop import dbquery

from rx.ormlite2.interfaces import IActiveRecord, IDatabaseAdapter
from rx.ormlite2 import schema, psycopgcon, dbop

from pyramid.utils import Config as config
from pyramid.tests.testlib import TestCaseWithPgDB, TestCaseWithThreads
from pyramid.tests.testlib import TestCaseWithDBAndVocabs

from _test_common import Ob, CommonTests
from _test_vocab import VocabTests
import testdata


provideUtility(psycopgcon, IDatabaseAdapter)


class Test(CommonTests, VocabTests, TestCaseWithPgDB, TestCaseWithThreads,
    TestCaseWithDBAndVocabs):

    def setUp(self):
        super(Test, self).setUp()
        transaction.begin()
        testdata.deleteAllFrom('ut__orm_months',
                               'ut__orm_reports',
                               'ut__orm_lob_test',
                               'ut__orm_users',
                               'ut__orm_units',
                               'ut__orm_test',
                               )
        testdata._initMonthsData()
        testdata._initReportsData()
        testdata._initUnitsData()
        testdata._initOrmUsersData()
        testdata._initLOBData()
        transaction.commit()

    def tearDown(self):
        psycopgcon.Connection.closing = False
        transaction.begin()
        testdata.deleteAllFrom('ut__orm_months',
                               'ut__orm_reports',
                               'ut__orm_lob_test',
                               'ut__orm_users',
                               'ut__orm_units',
                               'ut__orm_test',
                               )
        transaction.commit()
        super(Test, self).tearDown()

    def test_fetchonedict(self):
        c = dbquery("select * from ut__orm_users where user_id=1926")
        row = c.fetchonedict()
        self.assertEqual(row['user_id'], 1926)
        self.assertEqual(row['unit_id'], 1200)
        self.assertEqual(row['lname'], u'Новгородов')
        self.assertEqual(row['rank'], u'Начальник отдела')
        self.assertEqual(row['email'], u'unittest@test.com')
        self.assertEqual(row['patronymic'], u'Александрович')

    def test_fetchalldict(self):
        c = dbquery("select * from ut__orm_users where user_id=1926")
        row = c.fetchalldict()[0]
        self.assertEqual(row['user_id'], 1926)
        self.assertEqual(row['unit_id'], 1200)
        self.assertEqual(row['lname'], u'Новгородов')
        self.assertEqual(row['rank'], u'Начальник отдела')
        self.assertEqual(row['email'], u'unittest@test.com')
        self.assertEqual(row['patronymic'], u'Александрович')


    def testLOB(self):

        c = dbquery("select * from pg_largeobject")

        from rx.ormlite2.psycopgcon import con
        from rx.ormlite2 import lob, psycopgcon

        class IOb(IActiveRecord):
            recId = schema.Int(db_column="k",
                               primary_key=True, readonly=True)
            data = schema.LOB(db_column="data_lob", required=False)

        class Ob(ActiveRecord):
            implements(IOb)
            p_table_name = 'ut__orm_lob_test'


        ob = Ob(recId=22)
        self.assertTrue(isinstance(ob.data, psycopgcon.LOBIO))

        c = dbquery("select * from pg_largeobject")

        datalen = ob.data.write('xxx zzz')
        self.assertEquals(datalen, 7)
        ob.save()

        self.assertTrue(ob.data.oid)

        #con().c.lo_lseek(ob.data.fd, 0, 0)
        #ob.data.seek(0, 0)

        #data = con().c.lo_read(ob.data.fd)
        data = ob.data.read()
        self.assertEquals(data, 'xxx zzz')

        ob2 = Ob.load(recId=22)
        ob.data.open()
        f1 = ob.data.fd
        self.assertNotEqual(None, f1)
        ob.data.open()
        self.assertNotEqual(f1, ob.data.fd)
        ob.data.close()
        self.assertEqual(None, ob.data.fd)

        ob2 = Ob.load(recId=22)
        self.assertEquals(ob2.data.read(), 'xxx zzz')

        ob2.data.truncate(3)
        ob2.save()
        ob2 = Ob.load(recId=22)
        self.assertEquals(ob2.data.read(), 'xxx')
        #self.assertEquals(ob2.data.tell(), 3)
        #self.assertEquals(ob2.data.read(), '')

        #ob2.data.seek(-2, 1)
        #self.assertEquals(ob2.data.tell(), 1)
        #self.assertEquals(ob2.data.read(2), 'xx')

        ob2.data.unlink()

        c = dbquery("select * from pg_largeobject where loid=%s", ob2.data.oid)
        self.assertEquals(len(c.fetchall()), 0)

    def testDatabaseException(self):
        try:
            try:
                dbquery("insert into ut__orm_units values (222, 222, 'Collision', 'C', '222')")
                dbquery("insert into ut__orm_units values (222, 222, 'Collision', 'C', '222')")
            except dbop.dbapi.DatabaseError:
                pass
            else:
                self.fail('Must raise dbop.dbapi.DatabaseError')
        finally:
            dbop.rollback()


    def testBool(self):
        col_types = dbop.getColTypes('ut__bool_test')
        self.assertEqual(col_types['V'], 16)
        self.assertTrue(16 in dbop.dbapi.extensions.BOOLEAN.values)  # константа
        
        dbquery("insert into ut__bool_test values (%s, %s)", 1, True)
        dbquery("insert into ut__bool_test values (%s, %s)", 2, False)

        rows = dbquery("select * from ut__bool_test where k in (1, 2) order by k").fetchall()
        self.assertEqual(rows, [(1, True), (2, False)])

        dbquery("insert into ut__bool_test values (3, %s)" % dbop.sql_repr(True, dbop.dbapi.extensions.BOOLEAN))
        dbquery("insert into ut__bool_test values (4, %s)" % dbop.sql_repr(False, dbop.dbapi.extensions.BOOLEAN))

        rows = dbquery("select * from ut__bool_test where k in (3, 4) order by k").fetchall()
        self.assertEqual(rows, [(3, True), (4, False)])

        dbquery("insert into ut__bool_test values (5, '1')")
        dbquery("insert into ut__bool_test values (6, '0')")

        class IOb(IActiveRecord):
            recId = schema.Int(db_column='k', primary_key=True)
            flag = schema.Bool(db_column='v', required=False)

        class Ob(ActiveRecord):
            implements(IOb)
            p_table_name = 'ut__bool_test'

        ob = Ob.load(recId=1)
        self.assertEqual(ob.flag, True)
        ob = Ob.load(recId=2)
        self.assertEqual(ob.flag, False)

        ob.flag = True
        ob.save()
        row = dbquery("select * from ut__bool_test where k=2").fetchone()
        self.assertEqual(row, (2, True))

        ob = Ob(recId=10, flag=False)
        ob.save()
        row = dbquery("select * from ut__bool_test where k=10").fetchone()
        self.assertEqual(row, (10, False))


    def testHstore(self):
        if not dbop.con().hstore_oids:
            testoob.testing.skip('No HSTORE support in this database')
        
        try:
            dbquery('drop table ut__hstore')
        except dbop.dbapi.DatabaseError:
            dbop.rollback()

        dbquery('create table ut__hstore (k integer primary key, h hstore)')
        dbquery('insert into ut__hstore values (1, %s)', {'aaa': 'bbb', 'ccc': None})
        row = dbquery('select * from ut__hstore').fetchone()
        self.assertEqual(row, (1, {'aaa': 'bbb', 'ccc': None}))


    def testSelect(self):
        from zope import interface 
        class IOb(interface.Interface):
            obId = schema.Int(db_column="k", primary_key=True)
            value = schema.Text(db_column="v", required=False)
        
        class Ob(ActiveRecord):
            implements(IOb)
            p_table_name = 'ut__orm_test'

        for row in Ob.select(''):
            print row


class ConnectionTest(TestCaseWithPgDB):
    def setUp(self):
        super(ConnectionTest, self).setUp()
        transaction.begin()

    def tearDown(self):
        psycopgcon.Connection.closing = False
        psycopgcon.cons.clear()
        super(ConnectionTest, self).tearDown()
        
    def testCloseAll(self):
        dbquery('select now()')
        pg_con = dbop.con().c
        self.assertFalse(pg_con.closed)
        self.assertTrue(len(psycopgcon.cons) > 0)

        dbop.close()
        self.assertEqual(psycopgcon.Connection.closing, True)
        self.assertEqual(psycopgcon.cons, {})
        self.assertRaises(dbop.dbapi.DatabaseError, dbquery, 'select now()')
        self.assertTrue(pg_con.closed)


    def testCloseOnError(self):
        class TestCon:
            def __init__(self, *args, **kw):
                self.closed = False
            def close(self):
                self.closed = True

        con = dbop.con().c = TestCon()
        
        @psycopgcon.disconnect_on_error
        def fun():
            raise dbop.dbapi.OperationalError('test')
        
        self.assertRaises(dbop.dbapi.OperationalError, fun)
        self.assertTrue(con.closed)

    def testOperationalErrorRecovery(self):
        dbop.con().c.close()

        self.assertRaises(dbop.dbapi.InterfaceError, dbquery, 'select now()')
        dbquery('select now()')  # соединение восстановилось

        dbop.con().c.close()  # этот тест ломается с psycopg 2.4.4, нужна версия 2.4.5+
        self.assertRaises(dbop.dbapi.InterfaceError, dbop.defer_constraints)
        dbquery('select now()')  # соединение восстановилось


# Disable test if PostgeSQL DB Connection is not configured
if config.POSTGRES_DSN is None:
    import unittest

    class Test(unittest.TestCase):

        def testPg(self):
            testoob.testing.skip('No configuration for PostgreSQL (psycopg2) tests')


if __name__ == "__main__":
    testoob.main()
