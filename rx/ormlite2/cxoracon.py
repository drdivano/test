# -*- coding: utf-8 -*-

"""Oracle (cx_Oracle) DB Adapter.

$Id: cxoracon.py 1001 2012-11-12 13:42:19Z vmozhaev $
"""

import time
from datetime import datetime, date, time as time_
from StringIO import StringIO
import thread
import log

from zope.interface import implements
from zope.component import getUtility
import transaction.interfaces
import cx_Oracle

from rx.ormlite2.interfaces import IDatabaseConfig


dbapi = cx_Oracle
cons = {}


def printableDbName():
    config = getUtility(IDatabaseConfig)
    return "%s@%s/%s" % \
        (config.ORACLE_USER, config.ORACLE_HOST, config.ORACLE_SID)


class Connection(object):
    def __init__(self):
        config = getUtility(IDatabaseConfig)
        db_dsn = cx_Oracle.makedsn(
            config.ORACLE_HOST, config.ORACLE_PORT, config.ORACLE_SID)
        c = cx_Oracle.connect(config.ORACLE_USER, config.ORACLE_PW, db_dsn)
        self.c = c
        self._txn_registered = False
        self._txn_defered_constraints = False
        self._defer_constraints = \
            getattr(config, 'ORACLE_DEFER_CONSTRAINTS', False)

    def __repr__(self):
        r = '<%s.%s(%s) at 0x%X>' % (
            self.__class__.__module__,
            self.__class__.__name__,
            ('registered=%s' % self._txn_registered),
            id(self))
        return r

    def registerForTxn(self):
        'See IZopeConnection'
        if not self._txn_registered:
            dm = RDBConManager(self)
            txn_status = transaction.get().status
            if txn_status == 'Active':
                transaction.get().join(dm)
            self._txn_registered = True
            log.debug('register %r for txn with status %r' % \
                          (self, txn_status))

    def commit(self):
        # print "COMMIT"
        self._txn_registered = False
        self._txn_defered_constraints = False
        r = self.c.commit()
        log.debug('commit %r' % self)
        return r

    def rollback(self):
        # print "ROLLBACK"
        self._txn_registered = False
        self._txn_defered_constraints = False
        r = self.c.rollback()
        log.debug('rollback %r' % self)
        return r

    def cursor(self):
        self.registerForTxn()
        cursor = self.c.cursor()
        if not self._txn_defered_constraints:
            if self._defer_constraints:
                _defer_constraints(cursor)
            self._txn_defered_constraints = True
        return cursor

    def close(self):
        self._txn_registered = False
        self._txn_defered_constraints = False
        r = self.c.close()
        log.debug('close %r' % self)
        return r


def con():
    t_id = thread.get_ident()
    try:
        return cons[t_id]
    except KeyError:
        c = cons[t_id] = Connection()
        return c


def toUnicode(s):
    if s is None or isinstance(s, unicode):
        return s
    return unicode(s, 'utf-8')

def fromUnicode(u):
    if u is None or not isinstance(u, unicode):
        return u
    return u.encode('utf-8')


class CursorProxy(object):

    def _exec(self, *args):
        try:
            return self._cursor.execute(*args)
        except:
            # print "(***) cxoracon:", repr(args)
            raise

    def execute(self, statement, params=None, kw=None):
        assert not (params and kw)
        convStatement = fromUnicode(statement)
        from dbop import SqlParam
        if params:
            convParams = [ fromUnicode(SqlParam.getValue(v)) \
                for v in (params or ()) ]
#            print '***', convStatement
#            print '???', convParams
            return self._exec(convStatement, convParams)
        if kw:
            convKw = dict([ (k, fromUnicode(SqlParam.getValue(v))) \
                for k, v in kw.items() ])
            # Parameters type declaration
            kTypes = dict([ (p.name, p.type) for p in kw.values() \
                if isinstance(p, SqlParam) ])
            if kTypes:
                self._cursor.setinputsizes(**kTypes)
#            print '***', convStatement
#            print '+++', convKw
#            print '###', kTypes
            return self._exec(convStatement, convKw)
#        print '***', convStatement
        return self._exec(convStatement)


    def __init__(self, cursor):
        self._cursor = cursor

    def __iter__(self):
        return self._cursor

    def _converters(self):
        converters = []
        for colDescr in self.description or ():
            conv = colDescr[1] == cx_Oracle.STRING and toUnicode or None
            converters.append(conv)
        return converters

    @property
    def rowcount(self):
        return self._cursor.rowcount

    @property
    def description(self):
        return self._cursor.description

    def fetchone(self):
        row = []
        dbrow = self._cursor.fetchone()
        if not dbrow:
            return dbrow
        for i, val in enumerate(dbrow):
            conv = self._converters()[i]
            if conv:
                row.append(conv(val))
            else:
                row.append(val)
        return row

    def fetchall(self):
        rows = []
        converters = self._converters()
        for dbrow in self._cursor:  # .fetchall():
            row = []
            for i, val in enumerate(dbrow):
                conv = converters[i]
                if conv:
                    row.append(conv(val))
                else:
                    row.append(val)
            rows.append(row)
        return rows

    def _formatRowToDict(self, dbrow):
        if not dbrow:
            return dbrow
        row = {}
        converters = self._converters()
        desc = self.description
        for i, val in enumerate(dbrow):
            conv = converters[i]
            if conv:
                val = conv(val)
            row.update({desc[i][0]: val})
        return row

    def fetchonedict(self):
        dbrow = self._cursor.fetchone()
        return self._formatRowToDict(dbrow)

    def fetchalldict(self):
        rows = []
        for dbrow in self._cursor.fetchall():
            rows.append(self._formatRowToDict(dbrow))
        return rows


class RDBConManager(object):
    implements(transaction.interfaces.IDataManager)

    def __init__(self, con):
        self.con = con

    def __repr__(self):
        r = '<%s.%s at 0x%X>' % (
            self.__class__.__module__,
            self.__class__.__name__,
            id(self))
        return r

    def tpc_begin(self, t):
        pass

    def tpc_vote(self, t):
        pass

    def tpc_finish(self, t):
        pass

    def tpc_abort(self, t):
        pass

    def commit(self, t):
        self.con.commit()

    def abort(self, t):
        self.con.rollback()

    def sortKey(self):
        sort_key = printableDbName()
        return '2:cxoracon:%s' % sort_key

debugQuery = False
def dbquery(q, *params, **kw):

    if debugQuery:
        import inspect
        stack_items = []
        for frame in inspect.stack()[1:]:
            filename = frame[1]
            linenum = frame[2]
            funcname = frame[3]
            stack_items.append((funcname, filename, linenum,))
            if filename.find('layout.py') >= 0:
                break
        print '------------------------------------------'
        print '>>> Stack:'
        for item in stack_items:
            print "      %s() from '%s', Line %s" % item
        print '>>> Query: %s' % repr(q)
        print '>>> Params: %s' % repr(params)
        print '>>> kwargs: %s' % repr(kw)
        print '------------------------------------------\n'

    c = con().cursor()
    c.arraysize = 100
    c = CursorProxy(c)
    try:
        c.execute(q, params, kw)
    except cx_Oracle.DatabaseError, v:
        raise cx_Oracle.DatabaseError("%s\n%r\n%r\n%r" % (v, q, params, kw))

    return c

def begin_transaction():
    con().begin()

def commit_transaction():
    con().commit()

def rollback_transaction():
    con().rollback()

def close_connection():
    con().close()

def _defer_constraints(cursor, defer=True):
    cursor.execute(
        'SET CONSTRAINTS ALL %s' % ('DEFERRED' if defer else 'IMMEDIATE'))

def defer_constraints(defer=True):
    _defer_constraints(con().cursor(), defer=defer)

_col_type_cache = {}

def getColTypes(tabName):
    if _col_type_cache.has_key(tabName):
        return _col_type_cache[tabName]
    else:
        col_types = {}
        c = dbquery("SELECT * FROM %s WHERE ROWNUM < 0" % tabName)
        for (name, type_code, display_size, internal_size, precision,
             scale, null_ok) in c.description:
            col_types[name.upper()] = type_code
        _col_type_cache[tabName] = col_types
        return col_types


def sql_quote(text):
    if type(text) not in (type(""), type(u"")):
        text = unicode(text)
    return text.replace("'", "''")


def sql_repr(v, col_type=None):
    if v is None:
        return 'NULL'

    if type(v) in (type([]), type(())):
        return "(%s)" % ",".join(map(lambda e: sql_repr(e, col_type), v))

    if col_type is dbapi.DATETIME or isinstance(v, (date, time_, datetime)):
        exprFmt = "TO_DATE('%s', 'YYYY-MM-DD HH24:MI:SS')"
        # проверить если дата в POSIX int формате
        try:
            if isinstance(v, datetime):
                v = v.strftime("%Y-%m-%d %H:%M:%S")
            elif isinstance(v, date):
                v = v.strftime("%Y-%m-%d")
                exprFmt = "TO_DATE('%s', 'YYYY-MM-DD')"
            elif isinstance(v, time_):
                v = v.strftime("%H:%M:%S")
                exprFmt = "TO_TIMESTAMP('%s', 'HH24:MI:SS')"
            else:
                v = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(int(v) - time.altzone))
        except ValueError:
            pass
        return exprFmt % v

    if type(v) in (type(1), type(1.2)):
        return str(v)
    elif type(v) == bool:
        return str(int(v))
    else:
        return "'" + sql_quote(v) + "'"


def isParametrized(col_type):
    return col_type in \
        (cx_Oracle.LOB, cx_Oracle.BLOB, cx_Oracle.CLOB, cx_Oracle.BFILE)


def getParamName(col_name):
    return ':%s' % col_name


def get_new_id():
    return dbquery('SELECT global_seq.nextval FROM dual').fetchone()[0]


class _FakeLOB(object):

    def __init__(self, data=None):
        self._io = StringIO()
        if data is not None:
            self.write(data)

    def close(self):
        pass

    def isopen(self):
        return not self._io.closed

    def open(self):
        pass

    def read(self, offset=1, amount=None):
        self._io.seek(offset - 1)
        return self._io.read(amount)

    def size(self):
        return len(self.read())

    def trim(self, newSize=0):
        return self._io.truncate(newSize)

    def getchunksize(self):
        return None

    def write(self, data, offset=1):
        self._io.seek(offset - 1)
        self._io.write(data)
        self._io.flush()

class LOBIO(object):

    _tableName = None
    _fieldName = None
    _keys = None

    def __init__(self, ob=None):
        if ob is None:
            ob = _FakeLOB()
        self.ob = ob

    def __getstate__(self):
        data = self.__dict__
        if isinstance(self.ob, (cx_Oracle.LOB, _FakeLOB)):
            data['ob'] = None
        return data

    def _loadOb(self, for_update=False):
        from dbop import selectFrom
        extra = ''
        if for_update:
            extra = '1=1 FOR UPDATE'
        c = selectFrom(self._tableName, whereDict=self._keys,
            selectExpr=self._fieldName, extra=extra)
        self.ob = c.fetchone()[0]

    def _chkOb(self):
        if isinstance(self.ob, _FakeLOB) or \
              self._tableName is None or self._fieldName is None or \
              self._keys is None:
            return
        if isinstance(self.ob, basestring):
            self.ob = _FakeLOB(self.ob)
            return
        if self.ob is None:
            self._loadOb()
        else:
            try:
                self.ob.isopen()
            except cx_Oracle.ProgrammingError:
                self._loadOb()

    def read(self, size=None):
        self._chkOb()
        if size is not None:
            return self.ob.read(amount=size)
        return self.ob.read()

    def write(self, data):
        self._chkOb()
        if not self.ob.isopen():
            self._loadOb(for_update=True)
            self.ob.open()
        chunkSize = self.ob.getchunksize()
        if chunkSize:
            data = StringIO(data)
            buf = data.read(chunkSize)
            offset = 1
            while buf:
                self.ob.write(buf, offset=offset)
                offset += len(buf)
                buf = data.read(chunkSize)
        else:
            self.ob.write(data)
        self.ob.close()
        return self.ob.size()

    def truncate(self, size=0):
        self._chkOb()
        if not self.ob.isopen():
            self._loadOb(for_update=True)
            self.ob.open()
        return self.ob.trim(size)

    def getSqlData(self):
        self._chkOb()
        if isinstance(self.ob, _FakeLOB):
            return cx_Oracle.Binary(self.ob.read())
        # return self.ob
        from rx.ormlite2.dbop import SKIPPED_VALUE
        return SKIPPED_VALUE
