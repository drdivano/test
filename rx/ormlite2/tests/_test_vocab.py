# -*- coding: utf-8 -*-

from zope.interface import implements, interface
from zope.schema.interfaces import ITokenizedTerm
from zope.schema.vocabulary import SimpleTerm
import transaction
import persistent
import thread
from ZODB.POSException import ConflictError

from rx.ormlite2 import cache, schema, dbop, dbquery
from rx.ormlite2.exc import PersistenceError
from rx.ormlite2.interfaces import IObjectChangeLogger
from rx.ormlite2.vocabulary import big, mutable, rdb
from rx.ormlite2.vocabulary.adapters import Record2TermAdapter
from pyramid.registry import makeIndexerRegisterable
from pyramid.registry import makeVocabularyRegisterable
from pyramid.registry.interfaces import IRegisterableVocabulary
from pyramid.tests.testlib import unregisterAll
from pyramid.vocabulary import getV, getVI
from pyramid.vocabulary.base import ContextVocabulary, ContextSubsetVocabulary
from pyramid.vocabulary.base import SubsetVocabulary
from pyramid.vocabulary.factory import VocabularyFactory
from pyramid.vocabulary.interfaces import IMutableVocabulary
from pyramid.vocabulary.interfaces import IPersistentVocabulary

from _test_common import Unit, User, IUnit
from rx.ormlite2.cache import closeCon


cache.init()

class UnitRDBVocabulary(rdb.RDBVocabulary):
    objectI = IUnit
    objectC = Unit

class MutableUnit(cache.MutableElement, Unit):
    _save_called = False

    def save(self, replace=True):
        super(MutableUnit, self).save(replace=replace)
        MutableUnit._save_called = True

class MutableUnitsVocabulary(mutable.PersistentVocabulary):
    select = 'select unit_id, short_name, name, leader_id from ut__orm_units'
    objectC = MutableUnit
    makeVocabularyRegisterable('units')

class MutableOddUnitsVocabulary(mutable.PersistentSubsetVocabulary):
    superset = 'units'
    filter = lambda self, unit: (unit.unitId % 2 > 0) # Odd ids
    makeVocabularyRegisterable('units_odd')

class MutableUser(cache.MutableElement, User):
    pass

class MutableUsersVocabulary(mutable.PersistentVocabulary):
    select = "select user_id, email, unit_id, terminated from ut__orm_users"
    objectC = MutableUser
    makeVocabularyRegisterable('users')


class UsersByUnitIndexer(mutable.PersistentVocabularyIndexer):
    vocabulary = 'users'
    contextI = IUnit
    objectIndex = lambda self, user: user.unit.key
    contextIndex = lambda self, unit: unit.unitId if unit is not None else None
    makeIndexerRegisterable('user_index_by_unit')

class UsersByUnitVocabulary(mutable.PersistentIndexVocabulary):
    indexer = 'user_index_by_unit'
    contextI = IUnit
    makeVocabularyRegisterable('users_by_unit')


#class UsersByUnitSV(SubsetVocabulary):
#    superset = 'users'
#    contextI = IUnit
#    filter = lambda self, user: user.unit is self.context

#class UsersBySubstUnitSV(SubsetVocabulary):
#    superset = 'users'
#    contextI = IUnit
#    filter = lambda self, user: user.unit is self.context.substUnit

class OverridedUnit(MutableUnit):
    def _get_title(self):
        return 'Unit(%s)' % self.unitShortName

#class ActiveUsersByUnitSV(SubsetVocabulary):
#    superset = 'users_by_unit'
#    contextI = None
#    filter = lambda self, user: not user.terminated

# ----------------------------------------

class IReport(interface.Interface):
    reportId = schema.Int(db_column='report_id', primary_key=True)
    month = schema.Int(db_column='month')
    year = schema.Int(db_column='year')

class IFinancialMonth(interface.Interface):
    month = schema.Int(db_column='month', primary_key=True)
    year = schema.Int(db_column='year', primary_key=True)
    status = schema.Choice(db_column='status', values=['O', 'C'])
    mainReport = schema.Choice(db_column='mainReportId',
                               required=False,
                               vocabulary='reports_by_month')
#    reports = schema.Subset(vocabulary='reports_by_month')

class Report(cache.MutableElement):
    implements(IReport)
    p_table_name = 'ut__orm_reports'

class FinancialMonth(cache.MutableElement):
    implements(IFinancialMonth)
    p_table_name = 'ut__orm_months'


class ReportsV(mutable.PersistentVocabulary):
    select = 'select report_id, month, year from ut__orm_reports'
    objectC = Report
    makeVocabularyRegisterable('reports')

class MonthsV(mutable.PersistentVocabulary):
    select = 'select * from ut__orm_months'
    objectC = FinancialMonth
    makeVocabularyRegisterable('months')

class ReportsByMonthSV(ContextVocabulary, mutable.PersistentSubsetVocabulary):
    superset = 'reports'
    contextI = IFinancialMonth
    filter = lambda self, finMonth: (finMonth.month == self.context.month and
                                     finMonth.year == self.context.year)
    makeVocabularyRegisterable('reports_by_month')

class UsersVocabulary(rdb.RDBVocabulary):
    select = "select user_id, email, unit_id, terminated from ut__orm_users"
    objectC = User
    makeVocabularyRegisterable('users')

class UnitsVocabulary(rdb.RDBVocabulary):
    select = "select unit_id, short_name, name, leader_id from ut__orm_units"
    objectC = Unit
    makeVocabularyRegisterable('units')

class UsersByUnitSV(ContextVocabulary, SubsetVocabulary):
    superset = 'users'
    contextI = IUnit
    filter = lambda self, user: user.unit() is self.context
    makeVocabularyRegisterable('users_by_unit')

class UsersBySubstUnitSV(ContextVocabulary, SubsetVocabulary):
    superset = 'users'
    contextI = IUnit
    filter = lambda self, user: user.unit() is self.context.substUnit
    makeVocabularyRegisterable('users_by_subst_unit')

class ActiveUsersByUnitSV(ContextSubsetVocabulary):
    superset = 'users_by_unit'
    contextI = None
    filter = lambda self, user: not user.terminated
    makeVocabularyRegisterable('active_users_by_unit')

# Big Vocabulary ----------------------------------------

class UnitsBV(big.BigRDBVocabulary):
    objectC = Unit
    makeVocabularyRegisterable('units')

class UsersBV(big.BigRDBVocabulary):
    objectC = User
    makeVocabularyRegisterable('users')


# -------------------------------------------------------


class VocabTests(object):


    def setUp(self):
        super(VocabTests, self).setUp()
        Record2TermAdapter.register()

    def tearDown(self):
        unregisterAll()
        super(VocabTests, self).tearDown()
        transaction.begin()
        from rx.ormlite2.vocabulary.mutable import _getCacheRoot
        r = _getCacheRoot()
        r.clear()
        transaction.commit()

    def testObjectChangeLogger(self):
        class TestLogger(object):
            implements(IObjectChangeLogger)
            def __init__(self):
                self.added = []
                self.changed = []
                self.deleted = []

            def log(self, added, changed, deleted):
                self.added.extend(added)
                self.changed.extend(changed)
                self.deleted.extend(deleted)

        from zope import component
        logger = TestLogger()
        component.getGlobalSiteManager().registerUtility(logger, IObjectChangeLogger)

        MutableUnitsVocabulary.register()
        UsersByUnitSV.register()
        UsersBySubstUnitSV.register()

        self.assertFalse(logger.added)
        transaction.begin()
        voc = getV('units')
        v = voc.new()
        v.unitId = 11
        v.unitName = u'Logged Unit'
        voc.add(v)
        transaction.commit()
        self.assertTrue(logger.added)
        self.assertEqual(1, len(logger.added))
        self.assertTrue(v in logger.added)

        self.assertFalse(logger.deleted)
        transaction.begin()
        voc = getV('units')
        del voc[11]
        transaction.commit()
        self.assertTrue(logger.deleted)
        self.assertEqual(1, len(logger.deleted))
        self.assertTrue(v in logger.deleted)




    def testElementChangeNotify(self):
        rep = Report(reportId=1, month=1, year=2001)

        changes = []
        class C(object):
            def p_onChange(self, event, obs, attr, new):
                changes.append([obs, attr, new])
        ob = C()
        rep.p_subscribe(ob)
        rep.month = 2
        self.assertEquals(changes, [[(rep,), 'month', 2]])
        self.assertEquals(rep.p_version, 4)

        rep.p_unsubscribe(ob)
        rep.month = 3
        self.assertEquals(changes, [[(rep,), 'month', 2]])
        self.assertEquals(rep.p_version, 5)


    def testSubsetChoice(self):
        UsersVocabulary.register()
        UnitsVocabulary.register()
        UsersByUnitSV.register()
        UsersBySubstUnitSV.register()

        voc = getV('users')
        user = voc[1926]

        self.assertTrue(user.unit().unitShortName, u'ОПР')

        voc = getV('units')
        unit2 = voc.getTerm(user.unit()).value

        self.assertTrue(user.unit() is unit2)

        user.unit().substUnit.set(user.unit())
        self.assertTrue(user.unit().unitLeader is user.unit().substUnit().unitLeader)

        user.unit().substUnitLeader.set(user)


    def testCachedVocab(self):
        MutableUnit._save_called = False
        MutableUnitsVocabulary.register()
        MutableUsersVocabulary.register()
        UsersByUnitSV.register()
        UsersBySubstUnitSV.register()
        transaction.begin()
        cache.con().root()['vocabCache'].clear()
        self.assertEquals(len(transaction.get()._before_commit), 0)
        voc = getV('units')
        self.assertEquals(len(transaction.get()._before_commit), 1)
        transaction.commit()
        self.assertFalse(MutableUnit._save_called)

        try: u = voc[99902]
        except PersistenceError: pass
        except KeyError:
            self.fail('Mutable vocab should raise a PersistenceError exception '
                'for non-existing element')
        else:
            self.fail('Mutable vocab should raise a PersistenceError exception '
                'for non-existing element')

        transaction.begin()
        voc = getV('units')
        v = voc[1200]
        self.assertTrue(isinstance(voc[1200], persistent.Persistent))
        self.assertEquals(voc[1200].unitName, u'Test Unit')
        voc[1200].unitShortName = u'TTTT1'
        self.assertTrue(voc[1200]._p_changed)
        self.assertFalse(voc[1200] in cache.getTxnLocalData().added_objects)

        transaction.commit()
        self.assertTrue(MutableUnit._save_called)

        transaction.begin()
        voc = getV('units')
        self.assertFalse(voc[1200]._p_changed)
        self.assertFalse(voc[1200] in cache.getTxnLocalData().added_objects)

        s = dbquery('select short_name from ut__orm_units where unit_id=1200').fetchone()[0]
        self.assertTrue(s, u'TTTT')

        voc[1200].unitName = None
        self.assertTrue(voc[1200]._p_changed)

        try:
            transaction.commit()
            self.fail('commit must fail because unitName=None')
        except dbop.dbapi.Error:
            pass
        finally:
            transaction.abort()

        transaction.begin()
        voc[1200].unitShortName = u'XXXX'
        transaction.commit()

        transaction.begin()
        voc = getV('units')

        self.assertNotEquals(voc[1200].unitName, None)
        transaction.commit()

        transaction.begin()
        voc = getV('units')
        u = MutableUnit(unitId=99901)
        voc.add(u)
        u.unitShortName = 'UT'
        u.unitName = 'UT Unit (999.01)'
        self.assertTrue(voc[99901] in cache.getTxnLocalData().added_objects)

        transaction.commit()

        transaction.begin()
        voc = getV('units')
        self.assertFalse(voc[99901] in cache.getTxnLocalData().added_objects)
        self.assertEquals(voc[99901].unitShortName, 'UT')

        transaction.commit()

        transaction.begin()
        c = dbop.dbquery('select * from ut__orm_units where unit_id=%s' % 99901)
        self.assertTrue(c.fetchall(), 'Unit 99901 must be present in the table')

        voc = getV('units')
        del voc[99901]
        transaction.commit()

        c = dbop.dbquery('select * from ut__orm_units where unit_id=%s' % 99901)
        self.assertFalse(c.fetchall(), 'Unit 99901 must be deleted')


    def testCompositeKeyJoin(self):
        #''' SubsetVocabulary с сопоставлением по двум полям '''
        MonthsV.register()
        ReportsV.register()
        ReportsByMonthSV.register()

        voc = getV('reports')
        rep1 = voc[88991]

        voc = getV('months')

        # в марте было 3 репорта
        mon3_08 = voc['3-2008']
        field = IFinancialMonth['mainReport'].bind(mon3_08)
        self.assertEquals(len(field.vocabulary.values()), 3)

        # в январе - 1 репорт
        mon1_08 = voc['1-2008']
        field = IFinancialMonth['mainReport'].bind(mon1_08)
        self.assertEquals(len(field.vocabulary.values()), 1)

        self.assertTrue(voc['1-2008'] is voc[(1, 2008)])

    def testSubsetOfSubset(self):
        #''' Подмножество от подмножества '''
        UsersVocabulary.register()
        UnitsVocabulary.register()
        UsersByUnitSV.register()
        ActiveUsersByUnitSV.register()

        voc = getV('active_users_by_unit', context=getV('units')[1200])
        # юзеров с полем terminated не должно быть в списке
        self.assertEquals(voc.keys(), ['1926'])


    def testTxnLocalData(self):
        #''' Хранилище данных, локальных для транзакции '''
        transaction.begin()
        cache.getTxnLocalData().added_objects.append('aaa')
        self.assertEquals(cache.getTxnLocalData().added_objects, ['aaa'])
        transaction.commit()
        transaction.begin()
        self.assertFalse('aaa' in cache.getTxnLocalData().added_objects)
        transaction.commit()

# не требуется, т.к. RDBVocabulary теперь read-only
#    def testRDBVocabularyReload(self):
#        ''' Перезагрузка RDBVocabulary по новой транзакции '''
#        transaction.begin()
#        UsersVocabulary.register()
#        u1 = getV('users')[1926]
#        u11 = getV('users')[1926]
#        self.assertTrue(u1 is u11)
#        transaction.commit()
#        transaction.begin()
#        u2 = getV('users')[1926]
#        self.assertTrue(u1 is not u2)

    def testDeleteModified(self):
        #''' Удаление модифицированного объекта '''
        UnitsVocabulary.register()
        MutableUsersVocabulary.register()
        transaction.begin()
        cache.con().root()['vocabCache'].clear()
#        c = dbop.dbquery("select * from ut__orm_units")
#        q = c.fetchall()
#        print '*-**', q
        getV("units")
        u = MutableUser(userId=1999, unit=getV("units")[1200],
                        userDN="test")
        getV('users').add(u)
        transaction.commit()

        transaction.begin()
        unit = getV('users')[1999]
        unit.email = 'aaa'
        del getV('users')[1999]
        try:
            transaction.commit()
        except AssertionError:
            pass
        transaction.abort()

        from rx.ormlite2.vocabulary.mutable import _getCacheRoot
        r = _getCacheRoot()
        r.clear()
#        c = dbop.dbquery("select user_id from ut__orm_users where user_id=1999")
#        self.assertEquals(len(c.fetchall()), 0)

    def testRDBVocabulary(self):
        vocab = UnitRDBVocabulary()
        unit = Unit.load(unitId=1200)
        self.assertTrue(rdb.IObjectVocabulary.providedBy(vocab))
        self.assertEqual(2, len(vocab))
        self.assertEqual(2, len([o for o in vocab]))
        self.assertTrue(unit in vocab)
        self.assertTrue(1200 in vocab)
        self.assertTrue('1200' in vocab)
        self.assertFalse(200 in vocab)
        term = ITokenizedTerm(unit)
        self.assertTrue(term in vocab)

        self.assertEqual(unit, vocab[1200])
        self.assertEqual(unit, vocab['1200'])
        self.assertEqual(unit, vocab[{'unitId': 1200}])
        try: vocab[200]
        except PersistenceError: pass
        except Exception, e: self.fail('`%s` raised instead of `PersistenceError`' % e.__name__)
        else: self.fail('Object for token `200` should not be found')

        term = vocab.getTerm(unit)
        self.assertTrue(ITokenizedTerm.providedBy(term))
        self.assertEqual('1200', term.token)
        self.assertEqual(unit, term.value)
        self.assertRaises(PersistenceError, vocab.getTerm, Unit(unitId=200))

        term = vocab.getTermByToken(1200)
        self.assertTrue(ITokenizedTerm.providedBy(term))
        self.assertEqual('1200', term.token)
        self.assertEqual(unit, term.value)
        self.assertRaises(PersistenceError, vocab.getTermByToken, '200')

        self.assertEqual(unit, vocab.get(1200))
        self.assertEqual(unit, vocab.get('1200'))
        self.assertEqual(None, vocab.get(200))

        f = frozenset
        tokens = vocab.keys()
        self.assertEqual(f(('800', '1200')), f(tokens))
        values = vocab.values()
        self.assertEqual(2, len(values))
        self.assertTrue(unit in values)
        self.assertEqual(f(zip(tokens, values)), f(vocab.items()))

    def testPersistentVocabulary(self):
        #"""Test PersistentVocabulary"""
        class _TestPersistentVocabulary(mutable.PersistentVocabulary):
            objectC = MutableUser
            makeVocabularyRegisterable('users')
        _TestPersistentVocabulary.register()
        vocab = _TestPersistentVocabulary()

        self.assertTrue(IMutableVocabulary.providedBy(vocab))
        self.assertTrue(IPersistentVocabulary.providedBy(vocab))
        self.assertEqual(3, len(vocab))
        self.assertEqual(3, len([o for o in vocab]))
        user = MutableUser.load(userId=1926)
        self.assertTrue(user in vocab)
        self.assertTrue(1926 in vocab)
        self.assertTrue('1926' in vocab)
        self.assertTrue(SimpleTerm(user, 1926) in vocab)
        self.assertFalse(SimpleTerm(user, 9926) in vocab)

        self.assertEqual(user, vocab[1926])
        self.assertEqual(user, vocab['1926'])
        try: vocab[9926]
        except LookupError: pass
        except Exception, e: self.fail('`%s` raised instead of `LookupError`' % e.__name__)
        else: self.fail('Object for token `9926` should not be found')

    def testPersistentVocabularyConflicts(self):
        MutableUnitsVocabulary.register()
        self.registerTestVocabulary('fake',
            alias=('users_by_unit', 'users_by_subst_unit'))

        # Persistent vocab MUST be pre-initialized before multi-thread access!
        getV('units')
        transaction.commit()

        def _addUnit(unit_id):
            unit = MutableUnit(unitId=unit_id, unitName=str(unit_id))
            getV('units').add(unit)

        def _delUnit(unit_id):
            del getV('units')[unit_id]

        def _changeUnit(unit_id, params):
            u = getV('units')[unit_id]
            for name, value in params.items():
                setattr(u, name, value)

        vocab = getV('units')
        self.assertFalse(991 in vocab)
        self.assertFalse(992 in vocab)
        self.assertFalse(993 in vocab)
        self.assertEqual([], self.conflict_errors)

        # Test for different objects adding
        self._run_conflicting_threads(_addUnit, (991,), (992,))
        vocab = getV('units')
        self.assertTrue(991 in vocab)
        self.assertTrue(991 in vocab)
        self.assertEqual([], self.conflict_errors)

        # Test for same object adding
        self._run_conflicting_threads(_addUnit, (993,), (993,))
        vocab = getV('units')
        self.assertTrue(991 in vocab)
        self.assertTrue(991 in vocab)
        self.assertTrue(993 in vocab)
        self.assertEqual([2], self.conflict_errors) # T2 raised a ConflictError

        # Test for same object editing
        vocab = getV('units')
        u = vocab[991]
        u.unitName = 'old_name'
        u.unitShortName = 'old_sname'
        transaction.commit()
        vocab = getV('units')
        u = vocab[991]
        self.assertEqual('old_name', u.unitName)
        self.assertEqual('old_sname', u.unitShortName)

        self._run_conflicting_threads(_changeUnit,
            (991, {'unitName': 'name1', 'unitShortName': 'sname1'}),
            (991, {'unitName': 'name2'}))
        vocab = getV('units')
        self.assertEqual('name2', u.unitName)
        self.assertEqual('sname1', u.unitShortName)
        self.assertEqual([], self.conflict_errors)

        # Test for different objects deleting
        self._run_conflicting_threads(_delUnit, (991,), (992,))
        vocab = getV('units')
        self.assertFalse(991 in vocab)
        self.assertFalse(991 in vocab)
        self.assertTrue(993 in vocab)
        self.assertEqual([], self.conflict_errors)

        # Test for same object deleting
        self._run_conflicting_threads(_delUnit, (993,), (993,))
        vocab = getV('units')
        self.assertFalse(993 in vocab)
        self.assertEqual([], self.conflict_errors)


    def testPersistentVocabChangeNotify(self):
        class _TestReportsVocabulary(mutable.PersistentVocabulary):
            objectC = Report
            makeVocabularyRegisterable('p_reports')
        _TestReportsVocabulary.register()

        changes = []
        class C(object):
            def p_onChange(self, event, obs, attr, new):
                changes.append([obs, attr, new])
        ob = C()

        voc = getV('p_reports')
        voc.p_subscribe(ob)
        rep1 = voc[88991]
        rep1.month = 2

        #self.assertEquals(changes, [[(voc, rep1), 'month', 2]])
        self.assertEquals(changes, [[(rep1,), 'month', 2]])

    def testPersistentSubsetVocabulary(self):
        class _TestUsersVocabulary(mutable.PersistentVocabulary):
            objectC = MutableUser
            makeVocabularyRegisterable('users')
        _TestUsersVocabulary.register()
        class _TestUnitsVocabulary(mutable.PersistentVocabulary):
            objectC = MutableUnit
            makeVocabularyRegisterable('units')
        _TestUnitsVocabulary.register()
        class _TestUsersByUnitVocabulary(ContextVocabulary, mutable.PersistentSubsetVocabulary):
            superset = 'users'
            contextI = IUnit
            filter = lambda self, user: user.unit() is self.context
            makeVocabularyRegisterable('users_by_unit')
        _TestUsersByUnitVocabulary.register()

        unitsV = getV('units')
        unit = unitsV[1200]
        uuCSV = getV('users_by_unit', context=unit)

        user = uuCSV.get(1926)
        self.assertNotEquals(user, None)

        user = uuCSV.get(1111)
        self.assertEquals(user, None)

        user = uuCSV.get(1999)
        self.assertEquals(user, None)

        self.assertEquals(uuCSV.keys(), ['1926', '1927'])

        u = MutableUser(userId=1999, unit=getV("units")[1200],
                        userDN="test")
        ln = len(uuCSV)
        getV('users').add(u)
#        self.assertEquals(uuCSV.by_token, None,
#            "PersistentSubsetVocabulary.by_token is not being discarded on data change")
        self.assertEqual(ln+1, len(uuCSV))
        user = uuCSV.get(1999)
        self.assertNotEquals(user, None)

        # Тест на клонирование
        user = uuCSV[1926]
        clone_user = user.copy()
        try:
            clone_user.userDN = 'newDN'
        except ValueError:
            self.fail("MutableElement's clone should not trigger "
                      "modification event to subscribers")

        ref1 = uuCSV.by_token

        user = uuCSV.get(2398)
        self.assertEquals(user, None)

        ref2 = uuCSV.by_token
        self.assertEquals(ref1, ref2,
            "PersistentSubsetVocabulary.by_token is being discarded without any reason")
        del ref1
        del ref2

        self.assertEquals(len(uuCSV), 3)
        self.assertEquals(uuCSV.keys(), ['1999', '1926', '1927'])
        self.assertEquals(uuCSV.values(), [uuCSV[1999], uuCSV[1926], uuCSV[1927]])
        self.assertEquals(uuCSV.items(), [
                ('1999', uuCSV[1999]),
                ('1926', uuCSV[1926]),
                ('1927', uuCSV[1927])]
                          )
        self.assertTrue(getV('users')[1926] in uuCSV)
        self.assertFalse(getV('users')[2398] in uuCSV)

        self.assertTrue(1927 in uuCSV)
        self.assertFalse(2398 in uuCSV)

        self.assertEquals(uuCSV.getTerm(getV('users')[1926]).token, '1926')
        self.assertEquals(uuCSV.getTermByToken(1926).value, getV('users')[1926])

        emails = []
        for user in uuCSV:
            emails.append(user.email)
        self.assertEquals(emails, [None, 'unittest@test.com', 'unittest0@test.com'])

        uuCSV8 = getV('users_by_unit', context=unit)

        u.unit.set(getV('units')[800])

        self.assertEquals(uuCSV.keys(), ['1926', '1927'])
        self.assertTrue(u not in uuCSV8)

        u.unit.set(getV('units')[1200])
        self.assertEquals(uuCSV.keys(), ['1999', '1926', '1927'])
        self.assertTrue(u in uuCSV8)

        del getV('users')['1999']
        self.assertEquals(uuCSV.keys(), ['1926', '1927'])

    def testPersistentSubsetVocabularyConflicts(self):
        MutableUnitsVocabulary.register()
        MutableOddUnitsVocabulary.register()
        self.registerTestVocabulary('fake',
            alias=('users_by_unit', 'users_by_subst_unit'))

        # Persistent vocabs MUST be pre-initialized before multi-thread access!
        getV('units')
        getV('units_odd')
        transaction.commit()

        def _addUnit(unit_id):
            unit = MutableUnit(unitId=unit_id, unitName=str(unit_id))
            getV('units').add(unit)

        def _delUnit(unit_id):
            del getV('units')[unit_id]

        def _changeUnit(unit_id, params):
            u = getV('units')[unit_id]
            for name, value in params.items():
                setattr(u, name, value)

        vocab = getV('units_odd')
        self.assertFalse(991 in vocab)
        self.assertFalse(993 in vocab)
        self.assertFalse(995 in vocab)
        self.assertEqual([], self.conflict_errors)

        # Test for different objects adding
        self._run_conflicting_threads(_addUnit, (991,), (993,))
        vocab = getV('units_odd')
        self.assertTrue(991 in vocab)
        self.assertTrue(993 in vocab)
        self.assertEqual([], self.conflict_errors)

        # Test for same object adding
        self._run_conflicting_threads(_addUnit, (995,), (995,))
        vocab = getV('units_odd')
        self.assertTrue(991 in vocab)
        self.assertTrue(993 in vocab)
        self.assertTrue(995 in vocab)
        self.assertEqual([2], self.conflict_errors) # T2 raised a ConflictError

        # Test for same object editing
        vocab = getV('units_odd')
        u = vocab[991]
        u.unitName = 'old_name'
        u.unitShortName = 'old_sname'
        transaction.commit()
        vocab = getV('units_odd')
        u = vocab[991]
        self.assertEqual('old_name', u.unitName)
        self.assertEqual('old_sname', u.unitShortName)

        self._run_conflicting_threads(_changeUnit,
            (991, {'unitName': 'name1', 'unitShortName': 'sname1'}),
            (991, {'unitName': 'name2'}))
        vocab = getV('units_odd')
        self.assertEqual('name2', u.unitName)
        self.assertEqual('sname1', u.unitShortName)
        self.assertEqual([], self.conflict_errors)

        # Test for different objects deleting
        self._run_conflicting_threads(_delUnit, (991,), (993,))
        vocab = getV('units_odd')
        self.assertFalse(991 in vocab)
        self.assertFalse(993 in vocab)
        self.assertTrue(995 in vocab)
        self.assertEqual([], self.conflict_errors)

        # Test for same object deleting
        self._run_conflicting_threads(_delUnit, (995,), (995,))
        vocab = getV('units_odd')
        self.assertFalse(995 in vocab)
        self.assertEqual([], self.conflict_errors)

    def testPersistentSubsetChangeNotifyBug(self):

        class _TestUsersVocabulary(mutable.PersistentVocabulary):
            objectC = MutableUser
            makeVocabularyRegisterable('users')
        _TestUsersVocabulary.register()

        class _TestNotTerminatedUsersVocabulary(mutable.PersistentSubsetVocabulary):
            superset = 'users'
            filter = lambda self, user: user.terminated is None # Не уволенные
            makeVocabularyRegisterable('users2')
        _TestNotTerminatedUsersVocabulary.register()

        class _TestSecondUsersVocabulary(mutable.PersistentSubsetVocabulary):
            superset = 'users2'
            filter = lambda self, user: user.email.startswith('unittest0') # Подходит для юзера 1927
            makeVocabularyRegisterable('users3')
        _TestSecondUsersVocabulary.register()

        users = getV('users')
        users2 = getV('users2')
        users3 = getV('users3')

        user = users['1927']
        self.assertTrue(user.email.startswith('unittest0'))
        self.assertTrue(user not in users2) # Т.к. уволен
        self.assertTrue(user not in users3) # Т.к. подходит, но нет в users2

        user.email = user.email # Инициируем событие изменения атрибута,
                                # которое не должно повлиять на словари
        self.assertTrue(user not in users2) # Т.к. по-прежнему уволен
        self.assertTrue(user not in users3) # А вот здесь был баг

    def testPersistentVocabularyIndexerConflicts(self):
        MutableUnitsVocabulary.register()
        MutableUsersVocabulary.register()
        UsersByUnitIndexer.register()
        self.registerTestVocabulary('fake',
            alias=('users_by_unit', 'users_by_subst_unit'))

        # Persistent vocabs MUST be pre-initialized before multi-thread access!
        getV('units')
        getV('users')
        getVI('user_index_by_unit')
        transaction.commit()

        units = getV('units')
        units.add(MutableUnit(unitId=991, unitName='Name991'))
        units.add(MutableUnit(unitId=992, unitName='Name992'))
        units.add(MutableUnit(unitId=993, unitName='Name993'))
        users = getV('users')
        users.add(MutableUser(userId=771, userDN='DN771', unit=units[991]))
        users.add(MutableUser(userId=772, userDN='DN772', unit=units[992]))
        transaction.commit()

        index = getVI('user_index_by_unit')
        units = getV('units')
        users = getV('users')

        items = index(units[991])
        self.assertEqual(1, len(items))
        self.assertTrue(users[771] in items)
        items = index(units[992])
        self.assertEqual(1, len(items))
        self.assertTrue(users[772] in items)
        items = index(units[993])
        self.assertEqual(0, len(items))

        def _addUnit(unit_id):
            unit = MutableUnit(unitId=unit_id, unitName=str(unit_id))
            getV('units').add(unit)

        def _delUnit(unit_id):
            del getV('units')[unit_id]

        def _addUser(user_id, unit_id):
            unit = getV('units')[unit_id]
            user = MutableUser(userId=user_id, userDN=str(user_id), unit=unit)
            getV('users').add(user)

        def _delUser(user_id):
            del getV('users')[user_id]

        def _changeUserUnit(user_id, unit_id):
            getV('users')[user_id].unit.key = (unit_id,)

        # Test for indexing objects with different context adding
        self._run_conflicting_threads(_addUser, (781, 991), (782, 992))
        self.assertEqual([], self.conflict_errors)
        index = getVI('user_index_by_unit')
        units = getV('units')
        users = getV('users')
        items = index(units[991])
        self.assertEqual(2, len(items))
        self.assertTrue(users[781] in items)
        items = index(units[992])
        self.assertEqual(2, len(items))
        self.assertTrue(users[782] in items)
        items = index(units[993])
        self.assertEqual(0, len(items))

        # Test for indexing objects with same context adding
        self._run_conflicting_threads(_addUser, (783, 991), (784, 991))
        self.assertEqual([], self.conflict_errors)
        index = getVI('user_index_by_unit')
        units = getV('units')
        users = getV('users')
        items = index(units[991])
        self.assertEqual(4, len(items))
        self.assertTrue(users[783] in items)
        self.assertTrue(users[784] in items)
        items = index(units[992])
        self.assertEqual(2, len(items))
        items = index(units[993])
        self.assertEqual(0, len(items))

        # Test for same indexing object adding
        self._run_conflicting_threads(_addUser, (785, 991), (785, 991))
        self.assertEqual([2], self.conflict_errors) # T2 raised a ConflictError
        index = getVI('user_index_by_unit')
        units = getV('units')
        users = getV('users')
        items = index(units[991])
        self.assertEqual(5, len(items))
        self.assertTrue(users[785] in items)
        items = index(units[992])
        self.assertEqual(2, len(items))
        items = index(units[993])
        self.assertEqual(0, len(items))

        # Test for different context objects adding
        self._run_conflicting_threads(_addUnit, (981,), (982,))
        self.assertEqual([], self.conflict_errors)
        index = getVI('user_index_by_unit')
        units = getV('units')
        self.assertEqual(0, len(index(units[981])))
        self.assertEqual(0, len(index(units[982])))

        # Test for same context object adding
        self._run_conflicting_threads(_addUnit, (983,), (983,))
        self.assertEqual([2], self.conflict_errors) # T2 raised a ConflictError
        index = getVI('user_index_by_unit')
        units = getV('units')
        self.assertEqual(0, len(index(units[983])))

        # Test for different index objects editing
        self._run_conflicting_threads(_changeUserUnit, (781, 992), (782, 991))
        self.assertEqual([], self.conflict_errors)
        index = getVI('user_index_by_unit')
        units = getV('units')
        users = getV('users')
        items = index(units[991])
        self.assertFalse(users[781] in items)
        self.assertTrue(users[782] in items)
        items = index(units[992])
        self.assertTrue(users[781] in items)
        self.assertFalse(users[782] in items)

        # Test for same index object editing
        self._run_conflicting_threads(_changeUserUnit, (783, 992), (783, 993))
        self.assertEqual([2], self.conflict_errors) # T2 raised a ConflictError
        index = getVI('user_index_by_unit')
        units = getV('units')
        users = getV('users')
        self.assertFalse(users[783] in index(units[991]))
        self.assertTrue(users[783] in index(units[992]))
        self.assertFalse(users[783] in index(units[993]))

        # Test for indexing objects with different context deleting
        items = index(units[992])
        self.assertTrue(users[781] in items)
        self.assertEqual(3, len(items))
        items = index(units[991])
        self.assertTrue(users[782] in items)
        self.assertEqual(4, len(items))
        self._run_conflicting_threads(_delUser, (781,), (782,))
        self.assertEqual([], self.conflict_errors)
        index = getVI('user_index_by_unit')
        units = getV('units')
        users = getV('users')
        self.assertEqual(2, len(index(units[992])))
        self.assertEqual(3, len(index(units[991])))

        # Test for indexing objects with same context deleting
        items = index(units[991])
        self.assertTrue(users[784] in items)
        self.assertTrue(users[785] in items)
        self.assertEqual(3, len(items))
        self._run_conflicting_threads(_delUser, (784,), (785,))
        self.assertEqual([], self.conflict_errors)
        index = getVI('user_index_by_unit')
        units = getV('units')
        users = getV('users')
        self.assertEqual(1, len(index(units[991])))

    def testPersistentIndexVocabulary(self):

        class _TestUsersVocabulary(mutable.PersistentVocabulary):
            objectC = MutableUser
            makeVocabularyRegisterable('users')
        _TestUsersVocabulary.register()

        class _TestUnitsVocabulary(mutable.PersistentVocabulary):
            objectC = MutableUnit
            makeVocabularyRegisterable('units')
        _TestUnitsVocabulary.register()

        class _TestUsersByUnitIndexer(mutable.PersistentVocabularyIndexer):
            vocabulary = 'users'
            contextI = IUnit
            objectIndex = lambda self, user: user.unit.key[0]
            contextIndex = lambda self, unit: unit.unitId if unit is not None else None
            makeIndexerRegisterable('user_index_by_unit')
        _TestUsersByUnitIndexer.register()

        class _TestUsersByUnitVocabulary(mutable.PersistentIndexVocabulary):
            indexer = 'user_index_by_unit'
            contextI = IUnit
            makeVocabularyRegisterable('users_by_unit')
        _TestUsersByUnitVocabulary.register()

        unitsV = getV('units')
        unit = unitsV[1200]
        uuCSV = getV('users_by_unit', context=unit)

        user = uuCSV.get(1926)
        self.assertNotEquals(user, None)

        user = uuCSV.get(1111)
        self.assertEquals(user, None)

        user = uuCSV.get(1999)
        self.assertEquals(user, None)

        self.assertEquals(uuCSV.keys(), ['1926', '1927'])

        u = MutableUser(userId=1999, unit=getV("units")[1200],
                        userDN="test")
        ln = len(uuCSV)
        getV('users').add(u)
#        self.assertEquals(uuCSV.by_token, None,
#            "PersistentSubsetVocabulary.by_token is not being discarded on data change")
        self.assertEqual(ln+1, len(uuCSV))
        user = uuCSV.get(1999)
        self.assertNotEquals(user, None)

        # Тест на клонирование
        user = uuCSV[1926]
        clone_user = user.copy()
        try:
            clone_user.userDN = 'newDN'
        except ValueError:
            self.fail("MutableElement's clone should not trigger "
                      "modification event to subscribers")

        ref1 = uuCSV.by_token

        user = uuCSV.get(2398)
        self.assertEquals(user, None)

        ref2 = uuCSV.by_token
        self.assertEquals(ref1, ref2,
            "PersistentSubsetVocabulary.by_token is being discarded without any reason")
        del ref1
        del ref2

        self.assertEquals(len(uuCSV), 3)
        self.assertEquals(uuCSV.keys(), ['1999', '1926', '1927'])
        self.assertEquals(uuCSV.values(), [uuCSV[1999], uuCSV[1926], uuCSV[1927]])
        self.assertEquals(uuCSV.items(), [
                ('1999', uuCSV[1999]),
                ('1926', uuCSV[1926]),
                ('1927', uuCSV[1927])]
                          )
        self.assertTrue(getV('users')[1926] in uuCSV)
        self.assertFalse(getV('users')[2398] in uuCSV)

        self.assertTrue(1927 in uuCSV)
        self.assertFalse(2398 in uuCSV)

        self.assertEquals(uuCSV.getTerm(getV('users')[1926]).token, '1926')
        self.assertEquals(uuCSV.getTermByToken(1926).value, getV('users')[1926])

        emails = []
        for user in uuCSV:
            emails.append(user.email)
        self.assertEquals(emails, [None, 'unittest@test.com', 'unittest0@test.com'])

        uuCSV8 = getV('users_by_unit', context=unit)

        u.unit.set(getV('units')[800])
        self.assertEquals(uuCSV.keys(), ['1926', '1927'])
        self.assertTrue(u not in uuCSV8)

        u.unit.set(getV('units')[1200])
        self.assertEquals(uuCSV.keys(), ['1999', '1926', '1927'])
        self.assertTrue(u in uuCSV8)

        del getV('users')['1999']
        self.assertEquals(uuCSV.keys(), ['1926', '1927'])


    def testPersistentIndexVocabularyConflicts(self):
        MutableUnitsVocabulary.register()
        MutableUsersVocabulary.register()
        UsersByUnitIndexer.register()
        UsersByUnitVocabulary.register()
        self.registerTestVocabulary('users_by_subst_unit')

        # Persistent vocabs MUST be pre-initialized before multi-thread access!
        getV('units')
        getV('users')
        getVI('user_index_by_unit')
        transaction.commit()

        units = getV('units')
        units.add(MutableUnit(unitId=991, unitName='Name991'))
        units.add(MutableUnit(unitId=992, unitName='Name992'))
        units.add(MutableUnit(unitId=993, unitName='Name993'))
        users = getV('users')
        users.add(MutableUser(userId=771, userDN='DN771', unit=units[991]))
        users.add(MutableUser(userId=772, userDN='DN772', unit=units[992]))
        transaction.commit()

        units = getV('units')
        users = getV('users_by_unit', units[991])
        self.assertEqual(1, len(list(users)))
        self.assertTrue(771 in users)
        users = getV('users_by_unit', units[992])
        self.assertEqual(1, len(list(users)))
        self.assertTrue(772 in users)
        users = getV('users_by_unit', units[993])
        self.assertEqual(0, len(list(users)))

        def _addUnit(unit_id):
            unit = MutableUnit(unitId=unit_id, unitName=str(unit_id))
            getV('units').add(unit)

        def _delUnit(unit_id):
            del getV('units')[unit_id]

        def _addUser(user_id, unit_id):
            unit = getV('units')[unit_id]
            user = MutableUser(userId=user_id, userDN=str(user_id), unit=unit)
            getV('users').add(user)

        def _delUser(user_id):
            del getV('users')[user_id]

        def _changeUserUnit(user_id, unit_id):
            getV('users')[user_id].unit.key = (unit_id,)

        # Test for indexing objects with different context adding
        self._run_conflicting_threads(_addUser, (781, 991), (782, 992))
        self.assertEqual([], self.conflict_errors)
        units = getV('units')
        users = getV('users_by_unit', units[991])
        self.assertEqual(2, len(list(users)))
        self.assertTrue(781 in users)
        users = getV('users_by_unit', units[992])
        self.assertEqual(2, len(list(users)))
        self.assertTrue(782 in users)

        # Test for indexing objects with same context adding
        self._run_conflicting_threads(_addUser, (783, 991), (784, 991))
        self.assertEqual([], self.conflict_errors)
        units = getV('units')
        users = getV('users_by_unit', units[991])
        self.assertEqual(4, len(list(users)))
        self.assertTrue(783 in users)
        self.assertTrue(784 in users)

        # Test for same indexing object adding
        self._run_conflicting_threads(_addUser, (785, 991), (785, 991))
        self.assertEqual([2], self.conflict_errors) # T2 raised a ConflictError
        units = getV('units')
        users = getV('users_by_unit', units[991])
        self.assertEqual(5, len(list(users)))
        self.assertTrue(785 in users)

        # Test for different context objects adding
        self._run_conflicting_threads(_addUnit, (981,), (982,))
        self.assertEqual([], self.conflict_errors)
        units = getV('units')
        self.assertEqual(0, len(list(getV('users_by_unit', units[981]))))
        self.assertEqual(0, len(list(getV('users_by_unit', units[982]))))

        # Test for same context object adding
        self._run_conflicting_threads(_addUnit, (983,), (983,))
        self.assertEqual([2], self.conflict_errors) # T2 raised a ConflictError
        units = getV('units')
        self.assertEqual(0, len(list(getV('users_by_unit', units[983]))))

        # Test for different index objects editing
        units = getV('units')
        users = getV('users_by_unit', units[991])
        self.assertTrue(781 in users)
        self.assertFalse(782 in users)
        users = getV('users_by_unit', units[992])
        self.assertFalse(781 in users)
        self.assertTrue(782 in users)
        self._run_conflicting_threads(_changeUserUnit, (781, 992), (782, 991))
        self.assertEqual([], self.conflict_errors)
        users = getV('users_by_unit', units[991])
        self.assertFalse(781 in users)
        self.assertTrue(782 in users)
        users = getV('users_by_unit', units[992])
        self.assertTrue(781 in users)
        self.assertFalse(782 in users)

        # Test for same index object editing
        units = getV('units')
        users = getV('users_by_unit', units[991])
        self.assertTrue(783 in users)
        self._run_conflicting_threads(_changeUserUnit, (783, 992), (783, 993))
        self.assertEqual([2], self.conflict_errors) # T2 raised a ConflictError
        units = getV('units')
        users = getV('users_by_unit', units[991])
        self.assertFalse(783 in users)
        users = getV('users_by_unit', units[992])
        self.assertTrue(783 in users)
        users = getV('users_by_unit', units[993])
        self.assertFalse(783 in users)

        # Test for indexing objects with different context deleting
        units = getV('units')
        users = getV('users_by_unit', units[991])
        self.assertTrue(782 in users)
        users = getV('users_by_unit', units[992])
        self.assertTrue(781 in users)
        self._run_conflicting_threads(_delUser, (781,), (782,))
        self.assertEqual([], self.conflict_errors)
        units = getV('units')
        users = getV('users_by_unit', units[991])
        self.assertFalse(782 in users)
        users = getV('users_by_unit', units[992])
        self.assertFalse(781 in users)

        # Test for indexing objects with same context deleting
        units = getV('units')
        users = getV('users_by_unit', units[991])
        self.assertTrue(784 in users)
        self.assertTrue(785 in users)
        self._run_conflicting_threads(_delUser, (784,), (785,))
        self.assertEqual([], self.conflict_errors)
        units = getV('units')
        users = getV('users_by_unit', units[991])
        self.assertFalse(784 in users)
        self.assertFalse(785 in users)


    def testBigVocabulary(self):
        UnitsBV().register()
        UsersBV().register()

        transaction.begin()

        unitsV = getV('units')
        unit8 = unitsV['800']

        self.assertTrue('1200' in unitsV)
        unit12 = unitsV['1200']

        usersV = getV('users')
        user = usersV['1926']

        self.assertTrue(user.unit() is unit12)

        self.assertEquals(unitsV.get('222', 'MISSING'), 'MISSING')

        # Теперь проверяем, что при начале новой транзакции кэш сбросился
        transaction.abort()
        transaction.begin()

        unitsV = getV('units')
        self.assertEquals(unitsV._data, {})

        t8 = unitsV.getTermByToken('800')
        self.assertEquals(t8.token, '800')

        t8_2 = unitsV.getTerm(unitsV['800'])
        self.assertEquals(t8_2.value, t8.value)


    def testReference(self):
        class IRUser(interface.Interface):
            userId = schema.Int(db_column="user_id", primary_key=True,
                                readonly=True, title=u"ID пользователя")
            unit = schema.Reference(db_column="unit_id",
                                    title=u"Отдел", required=False,
                                    vocabulary="units")
        class IRUnit(interface.Interface):
            unitId = schema.Int(db_column="unit_id",
                                primary_key=True, readonly=True)
            unitShortName = schema.TextLine(db_column="short_name")
            unitName = schema.TextLine(db_column="name")

        from rx.ormlite2 import ActiveRecord

        class RUnit(ActiveRecord):
            implements(IRUnit)
            p_table_name = "ut__orm_units"

        class RUser(ActiveRecord):
            implements(IRUser)
            p_table_name = "ut__orm_users"

        class UsersBV(big.BigRDBVocabulary):
            objectC = RUser
            makeVocabularyRegisterable('users')

        class UnitsBV(big.BigRDBVocabulary):
            objectC = RUnit
            makeVocabularyRegisterable('units')

        UsersBV().register()
        UnitsBV().register()

        user = RUser.load(userId=1926)
        user.unit = None
        user.save()

        user = getV('users')[1926]
        self.assertEquals(user.unit, None)

        unit999 = RUnit()
        user.unit = unit999
        unit999.unitId = 999
        unit999.unitName = 'Department of Energy'
        unit999.unitShortName = 'DoE'
        user.save()

    def testTransactionLocalVocabularyFactory(self):

        from rx.ormlite2.vocabulary.local import TransactionLocalFactory

        class _TestUnitsVocabulary(UnitsVocabulary):
            makeVocabularyRegisterable('units', factory = VocabularyFactory)
        class _TestCachedUnitsVocabulary(UnitsVocabulary):
            makeVocabularyRegisterable('cached_units', factory = TransactionLocalFactory)

        IRegisterableVocabulary(_TestUnitsVocabulary).register()
        IRegisterableVocabulary(_TestCachedUnitsVocabulary).register()
        # Нужны для работы модели Unit
        IRegisterableVocabulary(UsersByUnitSV).register()
        IRegisterableVocabulary(UsersBySubstUnitSV).register()

        # Начинаем транзакцию
        transaction.begin()

        self.assertEqual(2, len(getV('units')))
        self.assertEqual(2, len(getV('cached_units')))

        # Добавляем новый элемент в БД
        newU = Unit(unitId=999, unitName='Department of Energy', unitShortName='DoE')
        newU.save()

        self.assertEqual(3, len(getV('units')))
        self.assertEqual(2, len(getV('cached_units')))

        # Теперь проверяем, что при начале новой транзакции кэш обновился
        transaction.commit()
        transaction.begin()

        self.assertEqual(3, len(getV('units')))
        self.assertEqual(3, len(getV('cached_units')))

        transaction.abort()
