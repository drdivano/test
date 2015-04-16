# -*- coding: utf-8 -*-

from rx.ormlite2 import ActiveRecord
from rx.ormlite2.vocabulary.rdb import RDBVocabulary
from rx.ormlite2.vocabulary.mutable import _getCacheRoot
from rx.ormlite2.dbop import dbquery, DbAdapter
from datetime import datetime, date, time

from zope.interface import implements, interface
import zope.schema
from rx.ormlite2 import schema
from rx.ormlite2.interfaces import IActiveRecord
from rx.ormlite2 import cache
from pyramid.vocabulary import getV
from pyramid.registry import makeVocabularyRegisterable, registerAdapter
from pyramid.vocabulary.simple import SimpleVocabulary
from pyramid.vocabulary.base import ContextVocabulary, SubsetVocabulary

import transaction

from zope.component import provideUtility, adapts
from rx.ormlite2.interfaces import IDatabaseConfig
#import config
from pyramid.utils import Config as config

from rx.ormlite2 import dbop

from zope.interface import directlyProvides
from zope.schema.interfaces import IContextSourceBinder

from pyramid.tests import testlib

from zope.schema.vocabulary import ITitledTokenizedTerm, SimpleTerm
from ZODB.POSException import ConflictError

provideUtility(config, IDatabaseConfig)


from pyramid.app.server.interfaces import IEngine

class TestEngine(object):
    subscribed = set()
    def subscribe(self, channel, callback):
        if (channel, callback) not in self.subscribed:
            callback(1)
            self.subscribed.add((channel, callback))

    def unsubscribe(self, channel, callback):
        pass

provideUtility(TestEngine(), IEngine)


class IOb(interface.Interface):
    obId = schema.Int(db_column="k", primary_key=True)
    value = schema.Text(db_column="v", required=False)
    cTime = schema.Datetime(db_column="timestamp", required=False)
    recDate = schema.Date(db_column="rec_date", required=False)
    recTime = schema.Time(db_column="rec_time", required=False)


class Ob(ActiveRecord):
    implements(IOb)
    p_table_name = 'ut__orm_test'

    def __init__(self, ob_id=None, val=None):
        super(Ob, self).__init__()
        self.obId = ob_id
        self.value = val
        self.cTime = datetime.now()


class IObSub(IOb):
    value = schema.Int(db_column="v", required=False)


class ObSub(Ob):
    implements(IObSub)
    p_table_name = 'ut__orm_test'


class POb(cache.MutableElement):
    implements(IOb)
    p_table_name = 'ut__orm_test'

    def __init__(self, obId, value=None):
        super(POb, self).__init__()
        self.obId = obId
        self.value = value
    
    def _p_resolveConflict(self, old, saved, new):
        raise ConflictError()


def getUsersByUnit(unit):
    assert IUnit.providedBy(unit)
    c = dbquery("select * from ut__orm_users where unit_id=%s" %
                unit.unitId)
    users = User.bulkLoad(c)
    return SimpleVocabulary(users.values())
directlyProvides(getUsersByUnit, IContextSourceBinder)


class IUser(interface.Interface):
    userId = schema.Int(db_column="user_id", primary_key=True,
                        readonly=True, title=u"ID пользователя")
    userDN = schema.TextLine(db_column="user_dn", title=u"Distinguished Name")
    email = schema.TextLine(db_column="email", title=u"E-Mail", required=False)
    unit = schema.Choice(db_column="unit_id",
                         title=u"Отдел", required=True, vocabulary="units")
    terminated = schema.Date(db_column="terminated",
                             title=u"Уволен с", required=False)

class IUnit(interface.Interface):
    unitId = schema.Int(db_column="unit_id",
                        primary_key=True, readonly=True)
    unitShortName = schema.TextLine(db_column="short_name")
    unitName = schema.TextLine(db_column="name")
#    unitLeader = schema.Choice(db_column="leader_id",
#                               required=False, source=getUsersByUnit)
    unitLeader = schema.Choice(db_column="leader_id",
                               required=False, vocabulary="users_by_unit")

    substUnit = schema.Choice(db_column="subst_unit_id",
                              vocabulary="units",
                              required=False)
    substUnitLeader = schema.Choice(db_column="subst_leader_id",
                                    required=False,
                                    vocabulary="users_by_subst_unit")


class User(ActiveRecord):
    implements(IUser)
    p_table_name = "ut__orm_users"

class Unit(ActiveRecord):
    implements(IUnit)
    p_table_name = "ut__orm_units"




def unit2term(ob):
    return SimpleTerm(ob, ob.unitId, ob.unitShortName)

#def user2term(ob):
#    return SimpleTerm(ob, ob.userId, ob.email)

class UserAdapter(object):
    implements(ITitledTokenizedTerm)
    adapts(IUser)
    def __new__(self, ob):
        return SimpleTerm(ob, ob.userId, ob.email)


#from zope import component
#component.provideAdapter(unit2term, [IUnit], ITitledTokenizedTerm)
#component.provideAdapter(user2term, [IUser], ITitledTokenizedTerm)

registerAdapter(unit2term, [IUnit], ITitledTokenizedTerm)
registerAdapter(UserAdapter)


class UsersVocabulary(RDBVocabulary):
    select = "select user_id, email, unit_id, terminated from ut__orm_users"
    objectC = User
    makeVocabularyRegisterable('users')

class UnitsVocabulary(RDBVocabulary):
    select = "select unit_id, short_name, name, leader_id from ut__orm_units"
    objectC = Unit
    makeVocabularyRegisterable('units')


class UsersByUnitVocabulary(ContextVocabulary, SubsetVocabulary):
    superset = 'users'
    contextI = IUnit
    filter = lambda self, ob: ob.unit.unitId == self.context.unitId
    makeVocabularyRegisterable('users_by_unit')

#class UsersBySubstUnitVocabulary(UsersByUnitVocabulary):
class UsersBySubstUnitVocabulary(ContextVocabulary, RDBVocabulary):
    objectC = User
    contextI = IUnit

    def _load(self):
        c = dbop.selectFrom(tabName='ut__orm_users',
                            whereDict={'unit_id': self.context.unitId})
        users = self.objectC.bulkLoad(c)
        return users.values()
    makeVocabularyRegisterable('users_by_subst_unit')


class CommonTests(object):
    def testDbqueryUnicode(self):
        dbquery(u"insert into ut__orm_test (k, v) values ('123', 'фыва')")
        select_q = "select v from ut__orm_test where k='123'"

        row = dbquery(select_q).fetchone()
        self.assertEqual(row[0], u'фыва')
        self.assertEqual(type(row[0]), unicode)

        row = dbquery(select_q).fetchall()[0]
        self.assertEqual(row[0], u'фыва')
        self.assertEqual(type(row[0]), unicode)

        row = [ row for row in dbquery(select_q) ][0]
        self.assertEqual(row[0], u'фыва')
        self.assertEqual(type(row[0]), unicode)


    def testOb(self):
        ob = Ob()
        for name in ('obId', 'value', 'cTime', 'recDate',):
            self.assert_(hasattr(ob, name), 'No field "%s"' % name)

    def testLoad(self):
        ob = Ob(1, "test")
        ob.save()

        ob2 = Ob.load(ob_id=1)
        self.assertEquals(ob2.value, "test")

    def testBulkLoad(self):
        objects = Ob.bulkLoad(dbquery("select * from ut__orm_test"))
        self.assertEqual(len(objects), 0)

        dbquery(u"insert into ut__orm_test (k, v) values ('123', 'фыва')")
        d = Ob.bulkLoad(dbquery("select * from ut__orm_test"))
        self.assertEqual(len(objects), 0)
        self.assertEqual(d.keys(), [123])
        self.assertEqual(d[123].value, u'фыва')
        
    def testSave(self):
        #'сохранение объекта ActiveRecord'
        ob = Ob(1, "test")
        ob2 = Ob(2, "test2")
        ob.save()
        ob2.save()
        ob.value = "test1"
        ob.reload()
        self.assertEquals(ob.value, "test")
        ob.save()

        self.assertTrue(isinstance(ob.cTime, datetime))

        objects = Ob.bulkLoad(dbquery("select * from ut__orm_test"))
        self.assertEqual(len(objects), 2)
        self.assertTrue(isinstance(objects.values()[0].cTime, datetime))

        for i, n in (20, ".aaa"), (4, ".zzz"), (3, ".qqq"), (111, ".vvv"):
            Ob(i, n).save()
        cursor = dbquery("select * from ut__orm_test where v like '.%%' order by k")
        obList = Ob.bulkLoadList(cursor)

        n = 0
        for i in 111, 20, 3, 4:
            self.assertEquals(obList[n].obId, i)
            n += 1

        cursor = dbquery("select * from ut__orm_test where v like '.%%' order by v")
        obList = Ob.bulkLoadList(cursor)

        n = 0
        for v in ".aaa", ".qqq", ".vvv", ".zzz":
            self.assertEquals(obList[n].value, v)
            n += 1

        ob.value = u"Тест"
        ob.save()

        ob.value = "% percent"
        ob.save()
        ob.reload()
        self.assertEquals(ob.value, "% percent")

        ob.value = ":1 colon"
        ob.save()
        ob.reload()
        self.assertEquals(ob.value, ":1 colon")

        d = '05\\03\\2008'
        ob.value = d
        ob.save()
        ob.reload()
        self.assertEquals(ob.value, d)

    def testDate(self):
        o = Ob()
        o.obId = 888
        o.recDate = date(1999, 1, 2)
        o.save()
        o2 = Ob.load(ob_id=888)
        self.assertEquals(o2.recDate, o.recDate)

    def testTime(self):
        o = Ob()
        o.obId = 787
        o.recTime = time(10,43)
        o.save()
        o2 = Ob.load(ob_id=787)
        self.assertEquals(o2.recTime, o.recTime)

    def testTimestamp(self):
        class IOb2(IOb):
            cTime = schema.Date(db_column="timestamp")

        class Ob2(ActiveRecord):
            implements(IOb2)
            p_table_name = 'ut__orm_test'

        o = Ob2()
        o.obId = 999
        o.value = "t"
        o.cTime = date.today()
        #from core.ORMLite.schema import IDate
        field = o.p_fields["cTime"]

        o.save()
        o.reload()

        self.assertEqual(o.cTime, date.today())


    def testChoice(self):
        #'lazy loading объекта типа Choice'
        UsersVocabulary.register()
        UnitsVocabulary.register()
        UsersByUnitVocabulary.register()
        # The following is added just to make the test pass
        UsersBySubstUnitVocabulary.register()

        user = User(userId=1926)
        user.reload()

        self.assertTrue(user.unit().unitShortName, u"ОПР")

        voc = getV("units")
        unit2 = voc.getTerm(user.unit()).value

        self.assertEqual(user.unit(), unit2)

        user.unit().unitShortName = "-unittest-"

        user2 = User(userId=user.userId)
        user2.reload()

        self.assertEqual(user2.unit().unitShortName, "-unittest-")

        transaction.commit()

        # транзакция должна вызвать перезагрузку vocabulary
        UnitsVocabulary.register()

        user3 = User(userId=user.userId)
        user3.reload()
        self.assertNotEqual(user3.unit().unitShortName, "-unittest-")

        user.save()


    def testPrimaryKey(self):
        ob1 = Ob(1, 'a')
        self.assertEqual(ob1.primary_key, (1,))

        class IMultiKeyOb(interface.Interface):
            k1 = schema.Int(db_column="k", primary_key=True)
            k2 = schema.Text(db_column="v", primary_key=True)
        
        
        class MKOb(ActiveRecord):
            implements(IMultiKeyOb)
            p_table_name = 'ut__orm_test'

        mkob = MKOb(k1=1, k2='a')
        self.assertEqual(mkob.primary_key, (1, 'a'))

    def testEq(self):
        ob1 = Ob(1, 'a')
        ob1.cTime = None
        ob2 = Ob(1, 'a')
        ob2.cTime = None
        self.assertEqual(ob1, ob2)
        
        UsersVocabulary.register()
        UnitsVocabulary.register()
        UsersByUnitVocabulary.register()
        UsersBySubstUnitVocabulary.register()

        user1 = User.load(userId=1926)
        user2 = User.load(userId=1926)
        self.assertEqual(user1, user2)

        user2.unit.key = Unit.load(unitId=800).primary_key
        self.assertNotEqual(user1, user2)

    def testSetChoice(self):
        UsersVocabulary.register()
        UnitsVocabulary.register()
        UsersByUnitVocabulary.register()
        UsersBySubstUnitVocabulary.register()

        user = User.load(userId=1926)
        user.unit.set(None)
        self.assertEqual(user.unit.key, None)
        
        unit8 = Unit.load(unitId=800)
        #unit12 = Unit.load(unitId=1200)
        
        user.unit.set(unit8)
        self.assertEqual(user.unit.key, (800,))

    def testUpdate(self):
        UsersVocabulary.register()
        UnitsVocabulary.register()
        UsersByUnitVocabulary.register()
        UsersBySubstUnitVocabulary.register()
        user1 = User.load(userId=1926)
        user2 = User.load(userId=1927)
        user3 = User.load(userId=1927)

        self.assertNotEquals(user1, user2)
        self.assertEquals(user2, user3)

        user2._update(user1)
        self.assertNotEquals(user1, user2)
        user2.userId = user1.userId
        self.assertEquals(user1, user2)
        self.assertNotEquals(user2, user3)

        user2._update({'email': 'invalid@invalid.org'})
        self.assertEquals('invalid@invalid.org', user2.email)

        user2._update(user1, **dict({'email': 'invalid2@invalid.org'}))
        self.assertEquals('invalid2@invalid.org', user2.email)

        class _TestU(UsersByUnitVocabulary):
            filter = lambda self, ob: True
            makeVocabularyRegisterable('users_by_unit')

        _TestU.register()
        unit1 = Unit.load(unitId=1200)
        unit2 = Unit.load(unitId=800)
        self.assertNotEquals(unit1, unit2)

        unit2.unitLeader = unit1.unitLeader
        unit2._update(unit1)
        unit2.unitId = unit1.unitId
        self.assertEquals(unit1, unit2)

    def testFixedPoint(self):
        class IDetailsRecord(IActiveRecord):
            recId = schema.Int(db_column="k",
                               primary_key=True, readonly=True)
            name = schema.TextLine(title=u"Наименование статьи",
                                   db_column="v",
                                   required=False)
            amount = schema.FixedPoint(title=u"Сумма", precision=2,
                                       db_column="amount")

        class DetailsRecord(ActiveRecord):
            implements(IDetailsRecord)
            p_table_name = 'ut__orm_test'

        rec = DetailsRecord()
        rec.recId = 1
        rec.name = "test"
        rec.amount = 1.2345
        rec.save()
        rec.reload()

        self.assertEquals(rec.amount, 1.23)

    def testList(self):
        class IListOb(IActiveRecord):
            recId = schema.Int(db_column="k",
                               primary_key=True, readonly=True)
            ls = schema.List(title=u"Список",
                             db_column="v",
                             value_type=zope.schema.Int(),
                             required=False)
        class ListOb(ActiveRecord):
            implements(IListOb)
            p_table_name = 'ut__orm_test'

        ob = ListOb()
        ob.recId = 1
        ob.ls = [1, 2, 3]
        ob.save()
        ob2 = ListOb.load(recId=1)
        self.assertEquals(ob2.ls, [1, 2, 3])

        row = dbquery("select v from ut__orm_test where k='1'").fetchone()
        self.assertEqual(row[0], '1,2,3')

    def testListSeparator(self):
        class IListOb2(IActiveRecord):
            recId = schema.Int(db_column="k",
                               primary_key=True, readonly=True)
            ls = schema.List(title=u"Список",
                             db_column="v",
                             value_type=zope.schema.TextLine(),
                             required=False,
                             separator='**')
        class ListOb2(ActiveRecord):
            implements(IListOb2)
            p_table_name = 'ut__orm_test'


        ob2 = ListOb2()
        ob2.recId = 2
        ob2.ls = ['44', '55', '66']
        ob2.save()
        row = dbquery("select v from ut__orm_test where k='2'").fetchone()
        self.assertEqual(row[0], '44**55**66')

    def testListUnicode(self):
        class IListOb(IActiveRecord):
            recId = schema.Int(db_column="k",
                               primary_key=True, readonly=True)
            ls = schema.List(title=u"Список",
                             db_column="v",
                             required=False)
        class ListOb(ActiveRecord):
            implements(IListOb)
            p_table_name = 'ut__orm_test'


        ob = ListOb()
        ob.recId = 3
        ob.ls = [u'я']
        ob.save()
        
        ob.load(recId=3)
        self.assertEquals(type(ob.ls[0]), unicode)
        
        obs = ListOb.bulkLoad(dbquery("select * from ut__orm_test where k='3'"))
        self.assertEqual(obs[3].ls[0], u'я')
        self.assertEqual(type(obs[3].ls[0]), unicode)

    def testDatabaseException(self):
        try:
            try:
                dbquery("insert into ut__orm_units values (222, 222, 'Collision', 'C', '222')")
                dbquery("insert into ut__orm_units values (222, 222, 'Collision', 'C', '222')")
            except dbop.dbapi.Error:
                pass
            else:
                self.fail('Must raise dbop.dbapi.DatabaseError')
        finally:
            dbop.rollback()


    def testGetNewId(self):
        u = User()
        try:
            u.getNewId()
        except dbop.dbapi.DatabaseError, e:
            dbop.commit()
            #self.fail("getNewId() shouldn't fail")
            raise e

    def testCompareToDb(self):
        #'сравнение объекта с записью в БД'
        ob = Ob(222, "test222")
        self.assertTrue(ob.differsFromDb())

        ob.save()
        self.assertFalse(ob.differsFromDb())

        ob.value = 333
        self.assertTrue(ob.differsFromDb())


    def testDeferConstraints(self):
        class IUser2(interface.Interface):
            userId = schema.Int(db_column="user_id", primary_key=True,
                                readonly=True, title=u"ID пользователя")
            userDN = schema.TextLine(db_column="user_dn", title=u"Distinguished Name")
            unitId = schema.Int(db_column="unit_id",
                                title=u"ID отдела")

        class IUnit2(interface.Interface):
            unitId = schema.Int(db_column="unit_id",
                                primary_key=True, readonly=True)
            unitShortName = schema.TextLine(db_column="short_name")
            unitName = schema.TextLine(db_column="name")

        class User2(ActiveRecord):
            implements(IUser2)
            p_table_name = "ut__orm_users"

        class Unit2(ActiveRecord):
            implements(IUnit2)
            p_table_name = "ut__orm_units"

        unit = Unit2(unitId=8181,
                    unitName='defer constraints',
                    unitShortName='d')

        user = User2(userId=1888,
                     userDN='defer constraints',
                     email=u'mail@some.domain',
                     unitId=unit.unitId)
        # произвольный порядок выполнения save

        dbop.defer_constraints(defer=False)
        try:
            user.save()
            self.fail('Must raise exception due to foreign key')
        except dbop.dbapi.DatabaseError:
            pass

        dbop.rollback()
        dbop.defer_constraints(defer=True)

        user.save()
        unit.save()
        dbop.commit()

        dbop.defer_constraints(defer=True)
        user.unitId = 8182
        user.save()

        try:
            dbop.defer_constraints(defer=False)
            self.fail('Foreign key constraint did not fire up')
        except dbop.dbapi.DatabaseError:
            pass
        #dbop.commit()


    def testInheritedInterfaces(self):
        #'''load объекта базового и унаследованного класса
        #   с разными интерфейсами'''

        ob = Ob(1, '100')
        ob.save()

        ob2 = Ob.load(1)
        self.assertEquals(ob2.value, '100')

        obsub = ObSub.load(1)
        self.assertEquals(obsub.value, 100)

    def testDbqueryWithKwParams(self):
        dbquery = dbop.dbquery
        # No params
        dbquery("insert into ut__orm_test (k, v) values ('1', 'abc')")
        d = dbquery("select * from ut__orm_test where k='1'").fetchonedict()
        d = dict([ (k.upper(), v) for k, v in d.items() ])
        self.assertEqual('abc', d['V'])
        # Has params
        p = dbop.getParamName('v')
        dbquery("insert into ut__orm_test (k, v) values ('2', %s)" % p, v='def')
        d = dbquery("select * from ut__orm_test where k='2'").fetchonedict()
        d = dict([ (k.upper(), v) for k, v in d.items() ])
        self.assertEqual('def', d['V'])
        
    def testDbCommitInRightOrder(self):
        # Our DB connectors do not support two-phase commits,
        # so they MUST commit transaction after ZODB connector
        root = _getCacheRoot()
        root['POb'] = POb(obId=101, value='A')
        transaction.commit()
        
        def _changePOb(ob_id, value):
            root = _getCacheRoot()
            root['POb'].value = value

        # Correct commit order: zodb_connection -> db_connection 
        self._run_conflicting_threads(_changePOb, (101, 'B'), (101, 'C'))
        self.assertEqual([2], self.conflict_errors) # T2 raised a ConflictError
        
        root = _getCacheRoot()
        self.assertEqual('B', root['POb'].value)
        db_ob = POb.load(obId=101)
        self.assertEqual('B', db_ob.value)
        
        # Incorrect commit order: db_connection -> zodb_connection 
        dbcon = DbAdapter()
        try:
            sortKey = dbcon.RDBConManager.sortKey
            dbcon.RDBConManager.sortKey = lambda self: ''

            self._run_conflicting_threads(_changePOb, (101, 'D'), (101, 'E'))
            self.assertEqual([2], self.conflict_errors) # T2 raised a ConflictError
            
            root = _getCacheRoot()
            self.assertEqual('D', root['POb'].value)
            db_ob = POb.load(obId=101)
            self.assertEqual('E', db_ob.value)
        finally:
            dbcon.RDBConManager.sortKey = sortKey
