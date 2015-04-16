# -*- coding: utf-8 -*-
from interfaces import IDatabaseAdapter
from zope.component import getUtility

class Callable(object):
    def __init__(self, name):
        self._name = name

    def __call__(self, *params, **kwargs):
        conmod = getUtility(IDatabaseAdapter)
        return getattr(conmod, self._name)(*params, **kwargs)


class DbAdapter(object):
    def __new__(cls):
        return getUtility(IDatabaseAdapter)


class _SKIPPED_VALUE(object):
    u"""Маркер значения, которое следует исключить из INSERT/UPDATE"""
    def __repr__(self):
        return '<SKIPPED_VALUE>'

SKIPPED_VALUE = _SKIPPED_VALUE()


dbquery = Callable('dbquery')
sql_repr = Callable('sql_repr')
sql_quote = Callable('sql_quote')
get_new_id = Callable('get_new_id')
printableDbName = Callable('printableDbName')
begin = Callable('begin_transaction')
commit = Callable('commit_transaction')
rollback = Callable('rollback_transaction')
defer_constraints = Callable('defer_constraints')
close = Callable('close_all_connections')
getColTypes = Callable('getColTypes')
con = Callable('con')
isParametrized = Callable('isParametrized')
getParamName = Callable('getParamName')
LOBIO = Callable('LOBIO')


class _DbApiProxy(object):
    def __getattr__(self, name):
        conmod = getUtility(IDatabaseAdapter)
        return getattr(conmod.dbapi, name)

dbapi = _DbApiProxy()


class SqlParam(object):
    u"""Враппер для sql-параметров"""
    
    def __init__(self, name, type, value):
        self.name = name
        self.type = type
        self.value = value

    @classmethod
    def getValue(cls, p):
        if isinstance(p, cls):
            return p.value
        return p


def whereDictToSql(tabName, whereDict, preventAmbiguous=False):
    u"""Converts column/value dictionary to SQL statement's WHERE clause"""
    
    whereParts = []
    colTypes = getColTypes(tabName)

    for col, v in whereDict.items():
        colName = col
        indO = col.find('(')
        if indO != -1:
            indE = col.rfind(')')
            if indE != -1:
                colName = col[indO+1:indE]
        colType = colTypes[colName.upper()]
        if preventAmbiguous:
            col = '%s.%s' % (tabName, col)
        if v is None:
            whereParts.append("%s IS NULL" % (col))
        elif isinstance(v, (list, tuple)):
            whereParts.append("%s IN %s" % (col, sql_repr(v, colType)))
        else:
            whereParts.append("%s=%s" % (col, sql_repr(v, colType)))
    return " AND ".join(whereParts)


def selectFrom(tabName, whereDict, selectExpr="*", extra=""):
    whereSql = whereDictToSql(tabName, whereDict)
    selectSql = 'SELECT %s FROM %s' % (selectExpr, tabName)
    if whereSql or extra:
        whereSql = ' AND '.join([ s for s in (whereSql, extra) if s ])
        selectSql = '%s WHERE %s' % (selectSql, whereSql)
    return dbquery(selectSql)


def delete(tabName, whereDict):
    query = "DELETE FROM %s WHERE %s" % \
        (tabName, whereDictToSql(tabName, whereDict))

    #log.DEBUG(query)
    return dbquery(query)


def insert(tabName, columns, keyFields, replace=False):
    u"""Inserts or updates data in database"""
    
    colTypes = getColTypes(tabName)
    whereDict = {}  # WHERE clause
    valuesDict = {} # SET/VALUES or columns list clause
    
    whereParams = {}
    valuesParams = {}
    
    for cName in columns:
        colValue = columns[cName]
        if cName in keyFields:
            whereDict[cName] = colValue
        if colValue is SKIPPED_VALUE:
            continue
        colType = colTypes.get(cName.upper())
        if isParametrized(colType):
            paramValue = SqlParam(cName, colType, colValue)
            valuesParams[cName] = paramValue
            if cName in keyFields:
                whereParams[cName] = paramValue
            colValue = getParamName(cName)
        else:
            colValue = sql_repr(colValue, colType)
        valuesDict[cName] = colValue

    whereSql = whereDictToSql(tabName, whereDict)
    
    if replace and whereDict:
        selectSql = 'SELECT count(*) FROM %s WHERE %s' % (tabName, whereSql)
        hasRows = bool(dbquery(selectSql, **whereParams).fetchone()[0])
    else:
        hasRows = False
    
    if hasRows:
        # We should UPDATE
        valuesSql = ', '.join([ '%s=%s' % item for item in valuesDict.items() ])
        updateSql = 'UPDATE %s SET %s' % (tabName, valuesSql)
        if whereDict:
            updateSql = '%s WHERE %s' % (updateSql, whereSql)
        dbquery(updateSql, **valuesParams)
        return False
    else:
        # We should INSERT
        columnsSql = ', '.join(valuesDict.keys())
        valuesSql = ', '.join(valuesDict.values())
        insertSql = 'INSERT INTO %s (%s) VALUES (%s)' % \
            (tabName, columnsSql, valuesSql)
        dbquery(insertSql, **valuesParams)
        return True
