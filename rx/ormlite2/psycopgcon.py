# -*- coding: utf-8 -*-

"""PostgreSQL (psycopg2) DB Adapter.

$Id: psycopgcon.py 1002 2012-11-18 18:38:17Z anovgorodov $
"""

import time
from datetime import datetime, date
import thread
import traceback
import psycopg2
import psycopg2.extras
import transaction

from zope.interface import implements
from zope.component import getUtility

from rx.ormlite2.interfaces import IDatabaseConfig

psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

dbapi = psycopg2
cons = {}

class ClosingDatabaseError(psycopg2.DatabaseError):
    pass


def printableDbName():
    return getUtility(IDatabaseConfig).POSTGRES_DSN


def disconnect_on_error(method):
    ''' В случае ошибки связи с БД закрываем конекшн. Следующий запрос откроет его.'''
    def decorator(*args, **kw):
        try:
            return method(*args, **kw)
        except (psycopg2.OperationalError, psycopg2.InterfaceError, psycopg2.InternalError), v:
            print 'We have problems with DB connection:', repr(v)
            try:
                con().close()
            except:
                print "An exception has occured while closing the connection"
                traceback.print_exc()
            raise v
    return decorator


class Connection(object):
    closing = False
    
    def __init__(self, dsn=None):
        self._txn_registered = False
        self.dsn = dsn
        self._reconnect()

    def _reconnect(self):
        c = psycopg2.connect(self.dsn or getUtility(IDatabaseConfig).POSTGRES_DSN)
        self.c = c
        self.is_connected = True
        self._init_hstore()

    def _init_hstore(self):
        # инициализируем поддержку hstore
        cursor = self.c.cursor()
        self.hstore_oids = []
        self.hstore_array_oids = []
        cursor.execute("""SELECT t.oid, typarray FROM pg_type t JOIN pg_namespace ns ON typnamespace = ns.oid WHERE typname = 'hstore'""")
        for oids in cursor:
            self.hstore_oids.append(oids[0])
            self.hstore_array_oids.append(oids[1])

        if self.hstore_oids:
            psycopg2.extras.register_hstore(self.c, unicode=True)

    def registerForTxn(self):
        'See IZopeConnection'
        if not self._txn_registered:
            # Если конекшн сдох - пытаемся выполнить реконнект. Делать это мы имеем право только в начале транзакции
            if not self.is_connected:  
                print 'Attempting to restore DB connection'
                self._reconnect()
            dm = RDBConManager(self)
            if transaction.get().status == 'Active':
                transaction.get().join(dm)
            self._txn_registered = True

    @disconnect_on_error
    def commit(self):
        self._txn_registered = False
        return self.c.commit()

    @disconnect_on_error
    def rollback(self):
        self._txn_registered = False
        try:
            return self.c.rollback()
        except psycopg2.InterfaceError:  # connection already closed
            pass

    @disconnect_on_error
    def cursor(self):
        self.registerForTxn()
        return self.c.cursor()

    def close(self):
        self.is_connected = False
        self._txn_registered = False
        return self.c.close()


def con():
    if Connection.closing:
        raise ClosingDatabaseError('Closing database connections')
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
            raise

    def execute(self, statement, params=None, kw=None):
        assert not (params and kw)
        #logging.debug('(***) %s %r', statement, params or kw)
        #print '(***) %s %r' % (statement, params or kw)
        convStatement = fromUnicode(statement)
        from dbop import SqlParam
        if params or not (params or kw):
            #        ^ Hack to avoid ambiguous using of '%' in data
            convParams = [ fromUnicode(SqlParam.getValue(v)) \
                for v in (params or ()) ]
            return self._exec(convStatement, convParams)
            #return self._exec(convStatement, params)
        if kw:
            convKw = dict([ (k, fromUnicode(SqlParam.getValue(v)))
                for k, v in kw.items() ])
            return self._exec(convStatement, convKw)
            #return self._exec(convStatement, kw)
        return self._exec(convStatement)


    def __init__(self, cursor):
        self._cursor = cursor

    def __iter__(self):
        return self._cursor

    @property
    def rowcount(self):
        return self._cursor.rowcount

    @property
    def description(self):
        return self._cursor.description

    def fetchone(self):
        return self._cursor.fetchone()

    def fetchall(self):
        return self._cursor.fetchall()

    def _formatRowToDict(self, dbrow):
        if not dbrow:
            return dbrow
        row = {}
        desc = self.description
        for i, val in enumerate(dbrow):
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
        sort_key = printableDbName()
        return '2:psycopgcon:%s' % sort_key


@disconnect_on_error
def dbquery(q, *params, **kw):

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
    except psycopg2.DatabaseError, v:
        raise psycopg2.DatabaseError("%s\npgcode=%r\n%r\n%r\n%r" % (v, v.pgcode, q, params, kw))

    return c


def begin_transaction():
    con().begin()

def commit_transaction():
    con().commit()

def rollback_transaction():
    con().rollback()

def close_connection():
    con().close()

def close_all_connections(kill=False):
    print 'psycopgcon.close_all_connection'
    Connection.closing = True
    for t_id, con in cons.items():
        del cons[t_id]
        con.c.cancel()
        con.c.rollback()
        con.c.close()

@disconnect_on_error
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
    return text.replace("%", "%%").replace("'", "''").replace("\\", "\\\\")

def sql_repr(v, col_type=None):
    if v is None:
        return 'NULL'

    if type(v) in (type([]), type(())):
        return "(%s)" % ",".join(map(lambda e: sql_repr(e, col_type), v))

    if col_type is dbapi.DATETIME or \
           isinstance(v, date) or isinstance(v, datetime):
        exprFmt = "TO_TIMESTAMP('%s', 'YYYY-MM-DD HH24:MI:SS')"
        # проверить если дата в POSIX int формате
        try:
            if isinstance(v, datetime):
                v = v.strftime("%Y-%m-%d %H:%M:%S")
            elif isinstance(v, date):
                v = v.strftime("%Y-%m-%d")
                exprFmt = "TO_TIMESTAMP('%s', 'YYYY-MM-DD')"
            else:
                v = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(int(v) - time.altzone))
        except ValueError:
            pass
        return exprFmt % v
    if col_type is con().hstore_oids:
        return v
    if col_type in psycopg2.extensions.BOOLEAN.values:
        return bool(v) and "'1'" or "'0'"


    if type(v) in (type(1), type(1.2)):
        if col_type in (None, dbapi.STRING):
            return "'%s'" % v
        return str(v)
    elif type(v) == bool:
        return str(int(v))
    else:
        return "'%s'" % sql_quote(v)


def isParametrized(col_type):
    return False


def getParamName(col_name):
    return '%%(%s)s' % col_name


def get_new_id():
    return dbquery("SELECT nextval('global_seq')").fetchone()[0]


from psycopg2._psycopg import connection as _connection
if hasattr(_connection, 'lobject'):

    class LOBIO(object):
        def __init__(self, oid=None):
            self.oid = oid
            self.fd = None

        def __getstate__(self):
            d = dict(self.__dict__)
            d['fd'] = None
            return d
            
        def __mkOid(self, force_w=False):
            if self.oid is None:
                c = con().cursor()
                self.oid = con().c.lobject(0).oid
            if self.fd is None:
                mode = 'w' if force_w else 'rw'
                self.fd = con().c.lobject(self.oid, mode)
    
        def close(self):
            if self.fd is not None:
                self.fd.close()
                self.fd = None
    
        def open(self):
            self.close()
            self.__mkOid()
    
        def write(self, s):
            self.__mkOid()
            self.fd.seek(0)
            size = self.fd.write(s)
            self.close()
            return size
    
        def read(self, size=None):
            self.__mkOid()
            if size is None:
                size = -1
            self.fd.seek(0)
            data = self.fd.read(size)
            self.close()
            return data
    
        def truncate(self, size=0):
            self.__mkOid()
            if not size:
                size = self.fd.tell()
            self.fd.seek(0)
            data = self.read(size)
            self.unlink()
            self.write(data)
    
        def unlink(self):
            if self.fd:
                self.fd.unlink()
            self.oid = None
            self.fd = None
    
        def getSqlData(self):
            return self.oid

else:

    class LOBIO(object):
        
        chunkSize = 4096
        
        def __init__(self, oid=None):
            self.oid = oid
            self.fd = None

        def __getstate__(self):
            d = dict(self.__dict__)
            d['fd'] = None
            return d
    
        def __mkOid(self):
            if self.oid is None:
                #c = con().cursor()
                #c.execute("select lo_creat(-1)")
                #self.oid = c.fetchone()[0]
                self.oid = con().c.lo_create(0)
                self.fd = None
            if self.fd is None:
                self.fd = con().c.lo_open(self.oid, -1)
    
        def close(self):
            self.fd = None
    
        def open(self):
            self.close()
            self.__mkOid()
    
        def write(self, s):
            self.__mkOid()
            # FIXME: lo_write should be a method of connection
            size = 0
            con().c.lo_lseek(self.fd, 0, 0)
            if self.chunkSize:
                while s:
                    size += con().c.lo_write(self.fd, s[:self.chunkSize])
                    s = s[self.chunkSize:]
            if s:
                size += con().c.lo_write(self.fd, s)
            self.close()
            return size
    
        def read(self, size=None):
            self.__mkOid()
            if size is None:
                size = -1
            con().c.lo_lseek(self.fd, 0, 0)
            data = con().c.lo_read(self.fd, size)
            self.close()
            return data
    
        def truncate(self, size=0):
            self.__mkOid()
            con().c.lo_lseek(self.fd, 0, 0)
            con().c.lo_truncate(self.fd, size)
            self.close()
    
        def unlink(self):
            if self.oid:
                con().c.lo_unlink(self.oid)
            self.oid = None
            self.fd = None
    
        def getSqlData(self):
            return self.oid
