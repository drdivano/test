# -*- coding: utf-8 -*-

"""
$Id: $
"""

from __future__ import with_statement
import sys
import psycopg2
import testoob
import os.path
import transaction
from zope.schema.interfaces import IVocabularyFactory, ITokenizedTerm
from zope.interface import implements, interface
from pyramid.ormlite import dbquery, dbop, ActiveRecord
from pyramid.ormlite import schema
from pyramid.vocabulary import getV
from pyramid.ormlite.utils import runSQL
from pyramid.vocabulary.mvcc import MvccVocabulary
from pyramid.tests import testlib
from pyramid.registry import makeVocabularyRegisterable
from autoreload import ReloadOnDbChange
import config


# для имитации изменения базы другим пользователем
POSTGRES_USER2 = 'rx_test2'
POSTGRES_PW2 = '123'

create_db_users = '''
create role rx_test login password '123';
create role rx_test2 login password '123';
grant rx_test to pyramid_test2;
'''

create_schema = '''
'''

def init_db():
    create_func = '''
create or replace function _change_notify() 
returns trigger as $$
  begin
    execute 'NOTIFY ' || tg_table_name || ', ' || quote_literal(current_user);
    return null;
  end;
$$ language plpgsql;
    '''
    dbquery(create_func)

    sql = '''
drop table if exists ut__orm_units;

create table ut__orm_units
(
    unit_id integer not null primary key,
    name varchar(500) not null,
    short_name varchar(500)
);

drop trigger if exists _change_notify_tg on ut__orm_units;

create trigger _change_notify_tg after insert or update or delete
on ut__orm_units
execute procedure _change_notify();
'''
    runSQL(sql)
    transaction.commit()


class IUnit(interface.Interface):
    unitId = schema.Int(db_column="unit_id",
                        primary_key=True, readonly=True)
    unitShortName = schema.TextLine(db_column="short_name")
    unitName = schema.TextLine(db_column="name")


class Unit(ActiveRecord):
    implements(IUnit, ITokenizedTerm)
    p_table_name = "ut__orm_units"

    @property
    def token(self):
        return str(self.unitId)

    @property
    def value(self):
        return self


class SyncUnitsVocabulary(MvccVocabulary):
    makeVocabularyRegisterable('units')
    object_class = Unit

    def reload(self):
        cursor = dbquery("select * from %s" % self.object_class.p_table_name)
        obs = Unit.bulkLoadList(cursor)
        self.replace_all(obs)

    factory = ReloadOnDbChange(MvccVocabulary.factory, reload, object_class.p_table_name)


class Test(testlib.TestCaseWithDBAndVocabs):
    def setUp(self):
        super(Test, self).setUp()
        from pyramid.vocabulary.mvcc import register
        register()
        row = dbquery("select null from pg_user where usename=%s", POSTGRES_USER2).fetchone()
        if not row:
            testoob.testing.skip('You must create PostgreSQL user %s to run this test (see code comments)' % POSTGRES_USER2)
        
        init_db()

        
    def tearDown(self):
        super(Test, self).tearDown()
        dbquery("UNLISTEN *")
        dbquery("delete from ut__orm_units")
    

    def _con2(self):
        import re
        dsn2 = re.sub(r"(password|user)='[^']*'", '', config.POSTGRES_DSN)
        dsn2 += " user='%s' password='%s'" % (POSTGRES_USER2, POSTGRES_PW2)
        return psycopg2.connect(dsn2)


    def test_change_notify(self):
        factory = ReloadOnDbChange(None, lambda *args: None, 'ut__orm_units')
        factory.start()
        changed_tables = factory.changed_tables
        con2 = self._con2()

        dbquery("LISTEN ut__orm_units")
        transaction.commit()
        con2.cursor().execute("insert into ut__orm_units (unit_id, short_name, name) values (-1, 'C-Suite', 'Дирекция')")
        con2.commit()
        transaction.begin()
        factory.poll()
        self.assertEqual(changed_tables, set(['ut__orm_units']))
        changed_tables.clear()
        transaction.commit()
        con2.cursor().execute("update ut__orm_units set name='Директорат' where unit_id=-1")
        con2.commit()
        transaction.begin()
        factory.poll()
        self.assertEqual(changed_tables, set(['ut__orm_units']))
        changed_tables.clear()
        transaction.begin()
        factory.poll()
        self.assertEqual(changed_tables, set([]))
        changed_tables.clear()
        # сообщения от самого себя должны игнорироваться
        dbquery("update ut__orm_units set name='xxx' where unit_id=-1")
        transaction.commit()
        transaction.begin()
        factory.poll()
        self.assertEqual(changed_tables, set([]))

        dbquery("UNLISTEN ut__orm_units")


    def test_abort(self):
        SyncUnitsVocabulary.register()
        con2 = self._con2()

        factory = getV('units').factory
        changed_tables = factory.changed_tables

        con2.cursor().execute("insert into ut__orm_units (unit_id, short_name, name) "
                              "values (-1, 'C-Suite', 'Дирекция')")
        con2.commit()
        transaction.begin()

        factory.poll()
        self.assertEqual(changed_tables, {'ut__orm_units'})

        vocab = getV('units')
        self.assertEqual(changed_tables, set([]))

        transaction.abort()
        self.assertEqual(changed_tables, {'ut__orm_units'})


    def test_autoreload(self):
        SyncUnitsVocabulary.register()

        mydir = os.path.split(__file__)[0] or '.'

        cmdline = '%s _autoreload_helper.py %s %s' % (sys.executable, POSTGRES_USER2, POSTGRES_PW2)

        import subprocess
        child = subprocess.Popen(cmdline, cwd=mydir, bufsize=1, shell=True,
                                 stdin=subprocess.PIPE, stdout=subprocess.PIPE)

        while 1:
            c = child.stdout.read(1)
            if c == '$':
                break

        transaction.begin()
        
        try:
            dbquery("insert into ut__orm_units (unit_id, short_name, name) values (-1, 'C-Suite', 'Дирекция')")
            transaction.commit()
            transaction.begin()
        finally:
            child.communicate('\r')

        child.wait()
        self.assertEqual(child.returncode, 0)

        transaction.begin()
        self.assertEqual(getV('units')[-1].unitName, u'Директорат')


# Disable test if PostgeSQL DB Connection is not configured
if config.POSTGRES_DSN is None:
    import unittest

    class Test(unittest.TestCase):

        def testPg(self):
            testoob.testing.skip('No configuration for PostgreSQL (psycopg2) tests')



if __name__ == "__main__":
    testoob.main()
