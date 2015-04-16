# -*- coding: utf-8 -*-

"""PostgreSQL ODBC (pyodbc) DB Adapter

$Id: odbcpgcon.py 643 2010-03-30 15:01:34Z vmozhaev $
"""

import time
from datetime import datetime, date, time as time_
import thread
import pyodbc

from zope.component import getUtility

from rx.ormlite2.interfaces import IDatabaseConfig
from rx.ormlite2 import _odbccon


dbapi = pyodbc
cons = {}


def printableDbName():
    return getUtility(IDatabaseConfig).PG_ODBC_DSN


class RDBConManager(_odbccon.RDBConManager):
    _printableDbName = staticmethod(printableDbName)


class Connection(_odbccon.Connection):
    _RDBConManager = staticmethod(RDBConManager)

    def __init__(self):
        c = pyodbc.connect(getUtility(IDatabaseConfig).PG_ODBC_DSN)
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
    return unicode(s, 'utf-8')


def fromUnicode(u):
    if u is None or not isinstance(u, unicode):
        return u
    return u.encode('utf-8')


class CursorProxy(_odbccon.CursorProxy):
    _fromUnicode = staticmethod(fromUnicode)


def dbquery(q, *params, **kw):

    """
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
    """

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
    con().cursor().execute(
        'SET CONSTRAINTS ALL %s' % ('DEFERRED' if defer else 'IMMEDIATE'))


_col_type_cache = {}


def getColTypes(tabName):
    if _col_type_cache.has_key(tabName):
        return _col_type_cache[tabName]
    else:
        col_types = {}
        c = dbquery("SELECT * FROM %s LIMIT 0" % tabName)
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
        exprFmt = "TO_TIMESTAMP('%s', 'YYYY-MM-DD HH24:MI:SS')"
        # проверить если дата в POSIX int формате
        try:
            if isinstance(v, datetime):
                v = v.strftime("%Y-%m-%d %H:%M:%S")
            elif isinstance(v, date):
                v = v.strftime("%Y-%m-%d")
                exprFmt = "TO_TIMESTAMP('%s', 'YYYY-MM-DD')"
            elif isinstance(v, time_):
                v = v.strftime("%H:%M:%S")
                exprFmt = "TO_TIMESTAMP('%s', 'HH24:MI:SS')"
            else:
                v = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(int(v) - time.altzone))
        except ValueError:
            pass
        return exprFmt % v

    if type(v) in (type(1), type(1.2)):
        if col_type in (None, dbapi.STRING, unicode):
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
    return dbquery("SELECT nextval('global_seq')").fetchone()[0]
