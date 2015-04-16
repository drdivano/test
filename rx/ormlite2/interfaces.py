# -*- coding: utf-8 -*-

"""ORM-related Interfaces.

$Id: interfaces.py 788 2010-07-01 12:53:16Z mbirin $
"""

from zope.interface import Interface
import zope.schema

from rx.ormlite2 import schema


# Database-related Interfaces --------------------------------------------------

class IDatabaseConfig(Interface):
    u""""""


class IDatabaseError(Interface):
    u""""""


class IDatabaseAdapter(Interface):
    
    DatabaseError = zope.schema.Object(IDatabaseError)

    def con():
        """ Return DB-API-compliant database connection """

    def get_new_id():
        """ Get unique id (integer value) """

    def dbquery(q, *params, **kw):
        """ Prepare and execute a database operation.
        Parameters may be provided as sequence or
        mapping and will be bound to variables in the operation.
        Returns a cursor object conforming to DB-API specification. """

    def printableDbName():
        """ Return user-readable name of database connection """

    def begin_transaction():
        """ Begin new transaction. """

    def commit_transaction():
        """ Commit any pending transaction to the database. """

    def rollback_transaction():
        """ Roll back to the start of any pending transaction. """

    def getColTypes(tabName):
        """ Get a mapping containing column names as keys and
        corresponding column types as values """

    def sql_quote(v):
        """ Quote a string to be used in SQL query """

    def sql_repr(v, col_type=None):
        """ Quote a string to be used in SQL query taking in account
        it's type. None maps to NULL, and so on.
        col_type must contain dbapi-compliant column type,
        e.g. STRING, DATETIME, etc. If col_type is None, then column type
        is assumed to be STRING."""



# Record-related Interfaces --------------------------------------------------

class IRecord(Interface):
    """ Implements object as database record """

    def load():
        """ Load object from the database identified by already set
        key attribute"""

    def differsFromDb():
        """ Check if the object has modifications comparing to its
        corresponding DB record """

    def copy():
        """ Create a shallow copy of object """

    def load_many(cursor):
        """ Load objects from cursor an return as dict """

    def load_all():
        """ Load objects from cursor an return as list """

    def primary_key(self):
        """ Get primary key of the record """


class IActiveRecord(IRecord):
    """ Implements object as database record with persistence layer """

    def save(replace=True):
        """ Save object to the database """

    def delete():
        """ Delete object from the database identified by already set
        key attribute"""

    def getNewId(klass):
        """ Generate Id for new object """

    #invariant(lambda ob: ob._validateRequiredFields())


class IChangeNotify(Interface):
    def p_subscribe(self, method, event):
        """ Register method to be called when the object changes """

    def p_unsubscribe(self, method, event):
        """ Unregister the registered method """


class IMutableElement(IChangeNotify):
    """ This object can be stored in MutableVocabulary """


class IReadOnly(Interface):
    """ This object cannot be directly modified """


class IMetadataCapable(Interface):
    u"""Модель, формирующая и хранящая данные о последних базовых операциях
    с объектом (создание/модификация)."""

    createdDate = schema.Datetime(db_column='created_date',
        title=u'Дата создания', required=True)
    createdBy = schema.Choice(db_column='created_by', source='users_all',
        title=u'Кем создано', required=False)

    modifiedDate = schema.Datetime(db_column='modified_date',
        title=u'Дата изменения', required=True)
    modifiedBy = schema.Choice(db_column='modified_by', source='users_all',
        title=u'Кем изменено', required=False)


class IObjectChangeLogger(Interface):
    u"""Модель журнала изменений объектов"""
    def log(self, added, changed, deleted):
        u"""Логгирование событий изменения объектов"""
    
    
