# -*- coding: utf-8 -*-

"""Universal ODBC (pyodbc) DB Adapter

$Id: _odbccon.py 643 2010-03-30 15:01:34Z vmozhaev $
"""

import re
import pyodbc
import transaction

from zope.interface import implements


dbapi = pyodbc
cons = {}


def printableDbName():
    raise NotImplementedError('Function should return printable DB name')


class RDBConManager(object):
    implements(transaction.interfaces.IDataManager)
    _printableDbName = staticmethod(printableDbName)

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
        return self._printableDbName()


class Connection(object):
    _RDBConManager = staticmethod(RDBConManager)
    
    def __init__(self):
        raise NotImplementedError('Initializer should create correct DB connection')
#        c = pyodbc.connect(getUtility(IDatabaseConfig).ODBC_DSN)
#        self.c = c
#        self._txn_registered = False

    def registerForTxn(self):
        'See IZopeConnection'
        if not self._txn_registered:
            dm = self._RDBConManager(self)
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
    raise NotImplementedError('Function should return transaction-bounded connection')
#    t_id = thread.get_ident()
#    try:
#        return cons[t_id]
#    except KeyError:
#        c = cons[t_id] = Connection()
#        return c


def toUnicode(s):
    if s is None or isinstance(s, unicode):
        return s
    return unicode(s, 'utf-8')

def fromUnicode(u):
    if u is None or not isinstance(u, unicode):
        return u
    return u.encode('utf-8')


class CursorProxy(object):
    _fromUnicode = staticmethod(fromUnicode)

    def _exec(self, *args):
        try:
            return self._cursor.execute(*args)
        except:
            raise

    def execute(self, statement, params=None, kw=None):
        assert not (params and kw)
        convStatement = self._fromUnicode(statement)
        from dbop import SqlParam
        if params:
            convParams = [ self._fromUnicode(SqlParam.getValue(v)) \
                for v in (params or ()) ]
            return self._exec(convStatement, convParams)
        if kw:
            # Сначала избавляемся от всех строковых значений
            r = re.compile('(\'[^\']+\')')
            newStatement = convStatement
            sStrings = r.findall(convStatement)
            dStrings = {}
            for i in range(len(sStrings)):
                k = '[__p%s]' % (i+1)
                newStatement = newStatement.replace(sStrings[i], k)
                dStrings[k] = sStrings[i]
            # Теперь заменяем pyformat-параметры на qmark-параметры
            params = []
            r = re.compile('\%\((\w+)\)s')
            params = r.findall(newStatement)
            newStatement = r.sub('?', newStatement)
            # Вычисляем соответствие pyformat-параметров и kw
            r = re.compile('(?<=\%\()(\w+)(?=\)s)')
            params = [ r.sub(r'\1', p) for p in params ]
            # В конце восстанавливаем строковые значения
            for k in dStrings:
                newStatement = newStatement.replace(k, dStrings[k])
            # И поехали
            convParams = [ self._fromUnicode(SqlParam.getValue(kw[p])) \
                for p in (params or ()) ]
            return self._exec(newStatement, convParams)
        return self._exec(convStatement)


    def __init__(self, cursor):
        self._cursor = cursor

    def __iter__(self):
        return self._cursor

    def _converters(self):
        converters = []
        for colDescr in self.description or ():
            conv = colDescr[1] == pyodbc.STRING and toUnicode or None
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


def dbquery(q, *params, **kw):
    raise NotImplementedError('Function should return result of query execution throw cursor')

#    """
#    import inspect
#    stack_items = []
#    for frame in inspect.stack()[1:]:
#        filename = frame[1]
#        linenum = frame[2]
#        funcname = frame[3]
#        stack_items.append((funcname, filename, linenum,))
#        if filename.find('layout.py') >= 0:
#            break
#    print '------------------------------------------'
#    print '>>> Stack:'
#    for item in stack_items:
#        print "      %s() from '%s', Line %s" % item
#    print '>>> Query: %s' % repr(q)
#    print '>>> Params: %s' % repr(params)
#    print '>>> kwargs: %s' % repr(kw)
#    print '------------------------------------------\n'
#    """
#
#    c = con().cursor()
#    c = CursorProxy(c)
#    try:
#        c.execute(q, params, kw)
#    except pyodbc.DatabaseError, v:
#        raise pyodbc.DatabaseError("%s\n%r\n%r\n%r" % (v, q, params, kw))
#
#    return c


def begin_transaction():
    con().begin()

def commit_transaction():
    con().commit()

def rollback_transaction():
    con().rollback()

def close_connection():
    con().close()


def defer_constraints(defer=True):
    raise NotImplementedError("Procedure should switch on/of 'deferred constraints' mode")


_col_type_cache = {}


def getColTypes(tabName):
    raise NotImplementedError('Function should return dictionary of column types')
#    if _col_type_cache.has_key(tabName):
#        return _col_type_cache[tabName]
#    else:
#        col_types = {}
#        c = dbquery("SELECT * FROM %s LIMIT 0" % tabName)
#        for (name, type_code, display_size, internal_size, precision,
#             scale, null_ok) in c.description:
#            col_types[name.upper()] = type_code
#        _col_type_cache[tabName] = col_types
#        return col_types


def sql_quote(text):
    if type(text) not in (type(""), type(u"")):
        text = unicode(text)
    return text.replace("'", "''")


def sql_repr(v, col_type=None):
    raise NotImplementedError('Function should return a sql-ready representation of data')


def isParametrized(col_type):
    return False


def getParamName(col_name):
    return '%%(%s)s' % col_name


def get_new_id():
    raise NotImplementedError('Function should return a new unique id')
