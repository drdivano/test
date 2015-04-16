# -*- coding: utf-8 -*-

"""Microsoft SQL Server ODBC (pyodbc) DB Adapter

$Id: odbcmscon.py 643 2010-03-30 15:01:34Z vmozhaev $
"""

import time
from datetime import datetime, date, time as time_
from StringIO import StringIO
import thread
import pyodbc

from zope.component import getUtility

from rx.ormlite2.interfaces import IDatabaseConfig
from rx.ormlite2 import _odbccon


dbapi = pyodbc
cons = {}


def printableDbName():
    return getUtility(IDatabaseConfig).MS_ODBC_DSN


class RDBConManager(_odbccon.RDBConManager):
    _printableDbName = staticmethod(printableDbName)


class Connection(_odbccon.Connection):
    _RDBConManager = staticmethod(RDBConManager)

    def __init__(self):
        c = pyodbc.connect(getUtility(IDatabaseConfig).MS_ODBC_DSN)
        self.c = c
        self._txn_registered = False


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
    if isinstance(s, buffer):
        return s
    return unicode(s, 'utf-8')


def fromUnicode(u):
    # Для MS SQL utf-8 не применим - нужен unicode
    return toUnicode(u)
#    if u is None or not isinstance(u, unicode):
#        return u
#    return u.encode('utf-8')


class CursorProxy(_odbccon.CursorProxy):
    _fromUnicode = staticmethod(fromUnicode)


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
    c = CursorProxy(c)
    try:
        c.execute(q, params, kw)
    except pyodbc.DatabaseError, v:
        raise pyodbc.DatabaseError("%s\n%r\n%r\n%r" % (v, q, params, kw))

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

    if col_type is dbapi.DATETIME or isinstance(v, (date, time_, datetime)):
        exprFmt = "CONVERT(datetime, '%s', 120)"
        # проверить если дата в POSIX int формате
        try:
            if isinstance(v, datetime):
                v = v.strftime("%Y-%m-%d %H:%M:%S")
            elif isinstance(v, date):
                v = v.strftime("%Y-%m-%d")
                exprFmt = "CONVERT(date, '%s', 120)"
            elif isinstance(v, time_):
                v = v.strftime("%H:%M:%S")
                exprFmt = "CONVERT(time, '%s', 120)"
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
    return col_type in (pyodbc.BINARY,)


def getParamName(col_name):
    return '%%(%s)s' % col_name


def get_new_id():
    return dbquery(
        "DECLARE @new_id integer; EXEC @new_id = global_seq_nextval; SELECT @new_id"
        ).fetchone()[0]


class LOBIO(object):
    
    _tableName = None
    _fieldName = None
    _keys = None
    
    disableRecordLoading = True
    
    _io = None

    def __init__(self, data=None):
        if data is not None:
            self._io = StringIO(data)

    def _chkOb(self):
        if self._io is None:
            if self._tableName is not None and self._fieldName is not None \
              and self._keys is not None:
                from dbop import selectFrom
                c = selectFrom(self._tableName, whereDict=self._keys,
                    selectExpr=self._fieldName)
                self._io = StringIO(c.fetchone()[0])
            else:
                self._io = StringIO()

    def read(self, size=None):
        self._chkOb()
        if size is not None:
            return self._io.read(size)
        return self._io.getvalue()

    def write(self, data):
        self._chkOb()
        self._io.seek(0)
        self._io.write(data)
        return len(self._io.getvalue())

    def truncate(self, size=0):
        self._chkOb()
        return self._io.truncate(size)

    def getSqlData(self):
        self._chkOb()
        return pyodbc.Binary(self._io.getvalue())
