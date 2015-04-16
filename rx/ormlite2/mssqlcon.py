# -*- coding: utf-8 -*-

"""MS SQL Server (pymssql) DB Adapter.

$Id: mssqlcon.py 643 2010-03-30 15:01:34Z vmozhaev $
"""

import pymssql
import transaction
import thread
from datetime import date, time, datetime

from zope.interface import implements
from zope.component import getUtility

from rx.ormlite2.interfaces import IDatabaseConfig


dbapi = pymssql
cons = {}


def printableDbName():
    config = getUtility(IDatabaseConfig)
    return "%s@%s/%s" % \
        (config.MSSQL_USER, config.MSSQL_HOST, config.MSSQL_DBNAME)


class Connection(object):
    def __init__(self):
        config = getUtility(IDatabaseConfig)
        c = pymssql.connect(host=config.MSSQL_HOST, user=config.MSSQL_USER,
            password=config.MSSQL_PW, database=config.MSSQL_DBNAME)
        self.c = c
        self._txn_registered = False

    def registerForTxn(self):
        'See IZopeConnection'
        if not self._txn_registered:
            dm = RDBConManager(self)
            if transaction.get().status == 'Active':
                transaction.get().join(dm)
            self._txn_registered = True

    def commit(self):
        self._txn_registered = False
        return self.c.commit()

    def rollback(self):
        self._txn_registered = False
        return self.c.rollback()

    def cursor(self):
        self.registerForTxn()
        return self.c.cursor()

    def close(self):
        self._txn_registered = False
        return self.c.close()


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
#    if u is None or not isinstance(u, unicode):
#        return u
#    return u.encode('utf-8')
    # Для MS SQL utf-8 не применим - нужен unicode
    if u is None or isinstance(u, unicode):
        return u
    return unicode(u, 'utf-8')


class CursorProxy(object):

    def _exec(self, *args):
        try:
            return self._cursor.execute(*args)
        except:
            raise

    def execute(self, statement, params=None, kw=None):
        assert not (params and kw)
        convStatement = fromUnicode(statement)
        from dbop import SqlParam
        if params:
            convParams = [ fromUnicode(SqlParam.getValue(v)) \
                for v in (params or ()) ]
            return self._exec(convStatement, convParams)
        if kw:
            convKw = dict([ (k, fromUnicode(SqlParam.getValue(v))) \
                for k, v in kw.items() ])
            return self._exec(convStatement, convKw)
        return self._exec(convStatement)

    def __init__(self, cursor):
        self._cursor = cursor

    def __iter__(self):
        return self._cursor

    def _converters(self):
        converters = []
        for colDescr in self.description or ():
            conv = colDescr[1] == pymssql.STRING and toUnicode or None
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
        for dbrow in self._cursor: #.fetchall():
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
        return printableDbName()


def dbquery(q, *params, **kw):

##    import inspect
##    stack_items = []
##    for frame in inspect.stack()[1:]:
##        filename = frame[1]
##        linenum = frame[2]
##        funcname = frame[3]
##        stack_items.append((funcname, filename, linenum,))
##        if filename.find('layout.py') >= 0:
##            break
#    print '------------------------------------------'
##    print '>>> Stack:'
##    for item in stack_items:
##        print "      %s() from '%s', Line %s" % item
#    print '>>> Query: %s' % repr(q)
#    print '>>> Params: %s' % repr(params)
#    print '>>> kwargs: %s' % repr(kw)
#    print '------------------------------------------\n'

    c = con().cursor()
    c.arraysize = 100
    c = CursorProxy(c)
    try:
        c.execute(q, params, kw)
    except pymssql.DatabaseError, v:
        raise pymssql.DatabaseError("%s\n%r\n%r\n%r" % (v, q, params, kw))

    return c


def begin_transaction():
    con().begin()

def commit_transaction():
    con().commit()

def rollback_transaction():
    con().rollback()

def close_connection():
    con().close()

def defer_constraints(defer=True):
    pass


_col_type_cache = {}


def getColTypes(tabName):
    if _col_type_cache.has_key(tabName):
        return _col_type_cache[tabName]
    else:
        col_types = {}
        c = dbquery("SELECT TOP 0 * FROM %s" % tabName)
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

    if col_type is dbapi.DATETIME or \
           isinstance(v, date) or isinstance(v, datetime):
        exprFmt = "CONVERT(datetime, '%s', 120)"
        # проверить если дата в POSIX int формате
        try:
            if isinstance(v, datetime):
                v = v.strftime("%Y-%m-%d %H:%M:%S")
            elif isinstance(v, date):
                v = v.strftime("%Y-%m-%d")
                exprFmt = "CONVERT(date, '%s', 120)"
            else:
                v = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(int(v) - time.altzone))
        except ValueError:
            pass
        return exprFmt % v

    if type(v) in (type(1), type(1.2)):
        if col_type in (None, dbapi.STRING):
            return "'%s'" % v
        return str(v)
    elif type(v) == bool:
        return str(int(v))
    else:
        return "'" + sql_quote(v) + "'"


def isParametrized(col_type):
    return False


def getParamName(col_name):
    return '%%(%s)s' % col_name


def get_new_id():
    return dbquery(
        "DECLARE @new_id integer; EXEC @new_id = global_seq_nextval; SELECT @new_id"
        ).fetchone()[0]
