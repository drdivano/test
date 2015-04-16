# -*- coding: utf-8 -*-

"""Implements object data persistence with "Active Record".

$Id: record.py 953 2012-03-25 13:26:19Z anovgorodov $
"""

import zope.interface
import zope.schema
from zope.interface import implements

from rx.ormlite2 import dbop
from rx.ormlite2.interfaces import IRecord, IActiveRecord
from rx.ormlite2.schema import IORMField, IChoice, IReference
from rx.ormlite2.lob import ILOB
from rx.ormlite2.exc import PersistenceError, ModelError


# Set to enable compact record representation
COMPACT_RECORD_REPR = False

class MISSING_REPR:
    def __repr__(self): return '{MISSING}'
MISSING = MISSING_REPR()


class _MissingMarker(object): pass
missing_marker = _MissingMarker()


class ObjectRef(object):
    def __init__(self, owner, name, field, key=None):
        assert key is None or isinstance(key, tuple)
        self.owner = owner
        self.name = name
        self.field = field
        self.key = key
    
    def __str__(self):
        return str(self.key)

    def __unicode__(self):
        return unicode(self.key)

    def __repr__(self):
        return 'ObjectRef(%s{%s}, %r, %r)' % (self.owner.__class__.__name__, id(self.owner), self.field, self.key)

    def __call__(self):
        bound_field = self.field.bind(self.owner)
        return bound_field.vocabulary[self.key]

    def __eq__(self, other):
        return self.field is other.field and self.key == other.key

    def __ne__(self, ob):
        return not self.__eq__(ob)

    def set(self, value):
        if value is None:
            self.key = self.field.null()
        else:
            self.key = IRecord(value).primary_key


class OrmMetaClass(type):
    def __new__(cls, name, bases, attrs):
        new = super(OrmMetaClass, cls).__new__(cls, name, bases, attrs)
        cls.init_orm_metadata(new)
        return new

    @classmethod
    def init_orm_metadata(meta, cls):
#        if hasattr(cls, '__class_initialized_%s_%s' % \
#            (cls.__module__, cls.__name__)):
#                return

        # заставляем метакласс Zope отработать раньше нас
        if '__implements_advice_data__' in cls.__dict__:
            interfaces, classImplements = cls.__dict__['__implements_advice_data__']
            #del cls.__implements_advice_data__
            classImplements(cls, *interfaces)
    
        fields = {}
    
        cls.p_attr2col = {}
        cls.p_col2attr = {}
        cls.p_attr_seq = []
        cls.p_col_seq = []
        cls.p_keys = []
        cls.p_key_fields = []
    
        seen = {}
        impl_fields = []
        for iface in zope.interface.implementedBy(cls):
            for name, field in zope.schema.getFieldsInOrder(iface):
                if name not in seen:
                    seen[name] = 1
                    impl_fields.append((name, field))
    
        for name, field in impl_fields:
            if IORMField.providedBy(field):
                if field.db_column in cls.p_col_seq:
                    raise ModelError('DB column "%s" cannot be declared twice '
                        '(trying to declare for field "%s", already declared '
                        'for field "%s")' % (field.db_column, name,
                                             cls.p_col2attr[field.db_column]))
                cls.p_attr2col[name] = field.db_column
                cls.p_col2attr[field.db_column] = name
                cls.p_attr_seq.append(name)
                cls.p_col_seq.append(field.db_column)
                fields[name] = field
                if field.primary_key:
                    cls.p_keys.append(name)
                    cls.p_key_fields.append(field.db_column)
    
        cls.p_fields = fields
    
#        setattr(cls, '__class_initialized_%s_%s' % \
#                (cls.__module__, cls.__name__), True)


class Record(object):
    __metaclass__ = OrmMetaClass

    implements(IRecord)

    def __setstate__(self, dic):
        # unpickling support
        self.__class_init__()
        self.__dict__.update(dic)

    def __init__(self, **kw):
        self.__class_init__()
        super(Record, self).__init__()

        # Инициализация значений полей
        for name, field in self.p_fields.items():
            value = kw.get(name, field.default)
            if IChoice.providedBy(field):
                ref = self._object_ref(self, name, field)
                ref.set(value)
                setattr(self, name, ref)
                continue
            if value is None:
                value = field.null()
            setattr(self, name, value)

    @property
    def primary_key(self):
        return tuple([ getattr(self, name) for name in self.p_keys ])

    @classmethod
    def _object_ref(self, *args, **kw):
        return ObjectRef(*args, **kw)

    def __repr__(self):
        if COMPACT_RECORD_REPR:
            parts = [ (key, getattr(self, key, MISSING)) \
                      for key in self.p_keys ]
            r = '<%s.%s(%s) at 0x%X>' % (
                self.__class__.__module__,
                self.__class__.__name__,
                ', '.join(('%s=%r' % (key, value) for key, value in parts)),
                id(self))
            return r
        else:
            parts = [str(self.__class__) + ' {%s}' % id(self)]
            parts.append("  TableName = %s" % getattr(self, 'p_table_name', MISSING))
            for ob_attr, field in self.p_fields.items():
                if hasattr(self, ob_attr):
                    v = getattr(self, ob_attr)
                    if isinstance(v, ActiveRecord):
                        v = "%s{%s}" % (v.__class__.__name__, id(v))
                    else:
                        v = repr(v)
                else:
                    v = "{MISSING}"
                parts.append("  %s = %s = %s" % \
                              (ob_attr, field.db_column, v))
            return "\n".join(parts)

    @classmethod
    def _selectExpr(klass):
        columns = list(klass.p_col_seq)
        dba = dbop.DbAdapter()
        if hasattr(dba, 'LOBIO'):
            LOBIO = dba.LOBIO
            if getattr(LOBIO, 'disableRecordLoading', False):
                lob_columns = [ c for c in columns \
                    if ILOB.providedBy(klass.p_fields[klass.p_col2attr[c]]) ]
                fix = lambda c, lob_columns=lob_columns: \
                    'NULL as %s' % c if c in lob_columns else c
                columns = [ fix(c) for c in columns ]
        return ','.join(columns)

    def _fetchFromDb(self):
        where_dict = {}
#        for k in self.p_key_fields:
#            where_dict[k] = getattr(self, self.p_col2attr[k])

        #TODO: Нужны тесты на добавленную логику
        p_keys = self.p_keys
        key_field_items = [ (key, field) \
            for key, field in self.p_fields.items() if key in p_keys ]
        for ob_attr, field in key_field_items:
            bound_field = field.bind(self)
            db_column = field.db_column
            if IChoice.providedBy(bound_field):
                val = getattr(self, ob_attr).key
            else:
                val = getattr(self, ob_attr)
                if IReference.providedBy(bound_field) and val is not None:
                    assert len(val.p_keys) == 1
                    val.save()  # FIXME: зачем здесь save ?
                    bound_field = val.p_fields[val.p_keys[0]].bind(val)
                    val = getattr(val, val.p_keys[0])
                elif IORMField.providedBy(bound_field):
                    if val is not None:
                        val = bound_field.toDbType(val)
            where_dict[db_column] = val

        c = dbop.selectFrom(tabName = self.p_table_name,
                            selectExpr = self._selectExpr(),
                            whereDict = where_dict)

        return c.fetchone()

    @classmethod
    def load(klass, *args, **kw):
        assert issubclass(klass, Record)
        assert args or kw, "No argument supplied to load() - most likely this is an error"
        return klass(*args, **kw).reload()

    def reload(self):
        r = self._fetchFromDb()
        if not r:
            raise PersistenceError('No data')

        self.__dict__.update(self._fromSequence(r))
        return self

    def _update(self, data=None, **kw):
        if data is None:
            data = dict()
        elif IRecord.providedBy(data):
            data = dict([(k, getattr(data, k)) for k in data.p_fields])
        data.update(kw)
        for ob_attr, field in self.p_fields.items():
            if ob_attr in data and ob_attr not in self.p_keys:
                setattr(self, ob_attr, data[ob_attr])

    def differsFromDb(self):
        r = self._fetchFromDb()
        if not r:
            return True
        d = self._fromSequence(r)
        for attr, dbval in d.items():
            field = self.p_fields[attr]
            obval = getattr(self, attr)
            if IORMField.providedBy(field):
                if obval is not None:
                    obval = field.toDbType(obval)
            if obval != dbval:
                return True
        return False

    def _fromDict(self, r):
        seq = []
        for db_col in self.p_col_seq:
            seq.append(r.get(db_col) or r.get(db_col.upper()))

        return self._fromSequence(seq)

    def _fromSequence(self, r):
        d = {}
        attr2col = self.p_attr2col
        tableName = self.p_table_name
        attr_seq = self.p_attr_seq
        fields = self.p_fields
        fromDbType = self._fromDbType
        valuesToSetKeys = []
        for i in range(len(self.p_col_seq)):
            attr = attr_seq[i]
            field = fields[attr]
            val = fromDbType(r[i], attr, field)
            if val is not missing_marker:
                # Hack to support LOB reloading
                if ILOB.providedBy(field) and val is not None:
                    val._tableName = tableName
                    val._fieldName = attr2col[attr]
                    val._keys = {}
                    valuesToSetKeys.append(val)
                d[attr] = val
        # Hack to support LOB reloading
        for k in self.p_keys:
            for val in valuesToSetKeys:
                val._keys[attr2col[k]] = d[k]
        return d

    def _fromDbType(self, val, attr, field):
        if IChoice.providedBy(field):
            return self._object_ref(self, attr, field, (val,))
        elif IReference.providedBy(field):
            if val is not None:
                bound_field = field.bind(self)
                return bound_field.vocabulary[val]
        elif IORMField.providedBy(field):
            bound_field = field.bind(self)
            val = bound_field.fromDbType(val)
        return val

    def copy(self):
        clone = self.__class__.__new__(self.__class__)
        clone.__dict__.update(self.__dict__)
        for name, field in self.p_fields.items():
            v = clone.__dict__.get(name)
            if IChoice.providedBy(field):
                clone.__dict__[name] = self._object_ref(clone, name, field, v.key)
        return clone

    @classmethod
    def load_many(klass, cursor):
        #klass.__class_init__()
        col2attr = klass.p_col2attr
        attr2col = klass.p_attr2col

        colindex2attr = []
        for d in cursor.description:
            col_name = d[0].lower()
            if col2attr.has_key(col_name):
                colindex2attr.append(col2attr[col_name])
            else:
                colindex2attr.append(col_name)

        for row in cursor:
            if not row:
                break
            ob = klass()
            valuesToSetKeys = []
            for i in xrange(len(row)):
                attr = colindex2attr[i]
                field = ob.p_fields.get(attr)
                val = ob._fromDbType(row[i], attr, field)
                if val is not missing_marker:
                    # Hack to support LOB reloading
                    if ILOB.providedBy(field) and val is not None:
                        val._tableName = klass.p_table_name
                        val._fieldName = attr2col[attr]
                        val._keys = {}
                        valuesToSetKeys.append(val)
                    setattr(ob, attr, val)

            # Hack to support LOB reloading
            for k in ob.p_keys:
                for val in valuesToSetKeys:
                    val._keys[attr2col[k]] = getattr(ob, k)
            
            yield ob

    @classmethod
    def load_all(klass):
        cursor = dbop.dbquery("select * from %s" % klass.p_table)
        return klass.load_many(cursor)

    @classmethod
    def select(klass, where, *params, **kw):
        """ Выборка списка объектов из БД
            Пример: User.select("ROLE='ADMIN' or STATUS=%s", status)
        """
        assert not (params and kw)

        select_list = ','.join([ col for col in klass.p_col_seq ])
        where_clause = 'WHERE %s' % where if where else ''
        q = 'SELECT %s FROM %s %s' % (select_list, klass.p_table_name, where_clause)
        cursor = dbop.dbquery(q, *params, **kw)
        return klass.load_many(cursor)


    def __eq__(self, ob):
        if not isinstance(ob, Record):
            return False
        if self.p_attr_seq != ob.p_attr_seq:
            #print 'self.p_attr_seq != ob.p_attr_seq'
            return False
        for name in self.p_attr_seq:
            if getattr(self, name) is not getattr(ob, name) and \
               getattr(self, name) != getattr(ob, name):
                #print '%r != %r' % (getattr(self, name), getattr(ob, name))
                return False
        return True

    def __ne__(self, ob):
        return not self.__eq__(ob)


class ActiveRecord(Record):
    
    implements(IActiveRecord)

    def save(self, replace=True):
        columns = {}
        for ob_attr, field in self.p_fields.items():
            bound_field = field.bind(self)
            db_column = field.db_column
            if IChoice.providedBy(bound_field):
                val = getattr(self, ob_attr).key
            else:
                val = getattr(self, ob_attr)
                if IReference.providedBy(bound_field) and val is not None:
                    assert len(val.p_keys) == 1
                    val.save()
                    bound_field = val.p_fields[val.p_keys[0]].bind(val)
                    val = getattr(val, val.p_keys[0])
                elif IORMField.providedBy(bound_field):
                    if val is not None:
                        val = bound_field.toDbType(val)

            columns[db_column] = val

        return dbop.insert(tabName = self.p_table_name,
                           columns = columns,
                           keyFields = self.p_key_fields,
                           replace = replace)

    def delete(self):
        where_dict = {}
#        for k in self.p_key_fields:
#            where_dict[k] = getattr(self, self.p_col2attr[k])

        #TODO: Нужны тесты на добавленную логику
        p_keys = self.p_keys
        key_field_items = [ (key, field) \
            for key, field in self.p_fields.items() if key in p_keys ]
        for ob_attr, field in key_field_items:
            bound_field = field.bind(self)
            db_column = field.db_column
            if IChoice.providedBy(bound_field):
                val = getattr(self, ob_attr).key
            else:
                val = getattr(self, ob_attr)
                if IReference.providedBy(bound_field) and val is not None:
                    assert len(val.p_keys) == 1
                    val.save()
                    bound_field = val.p_fields[val.p_keys[0]].bind(val)
                    val = getattr(val, val.p_keys[0])
                elif IORMField.providedBy(bound_field):
                    if val is not None:
                        val = bound_field.toDbType(val)
            where_dict[db_column] = val

        dbop.delete(tabName = self.p_table_name,
                    whereDict = where_dict
                    )

    @classmethod
    def getNewId(klass):
        return dbop.get_new_id()
