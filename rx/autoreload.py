# -*- coding: utf-8 -*-

"""
$Id: $
Возможность автоматической перезагрузки вокабов при изменений БД.
Перезагрузку вызывают только изменения, произведенные другими пользователями.

Synopsis:

class MyVocabulary(MvccVocabulary):
    makeVocabularyRegisterable('my_vocab')
    def reload(self):
        cursor = dbquery("select * from my_table")
        self.replace_all(MyRecord.bulkLoadList(cursor))
    factory = ReloadOnDbChange(MvccVocabulary.factory, reload, 'my_table')


Для работы механизма требуется PostgreSQL 9.x
Необходимо создать в БД функцию, и тригер для каждой из таблиц:

create or replace function _change_notify()
returns trigger as $$
  begin
    execute 'NOTIFY ' || tg_table_name || ', ' || quote_literal(current_user);
    return null;
  end;
$$ language plpgsql;

create trigger _change_notify_tg after insert or update or delete
on %(table)s
execute procedure _change_notify();
"""

import logging
import transaction
from zope.interface import implements
from pyramid.ormlite import dbop


class _ReloadRollbackManager(object):
    implements(transaction.interfaces.IDataManager)

    def __init__(self, changed_tables, table_name):
        self.changed_tables = changed_tables
        self.table_name = table_name

    def tpc_begin(self, t): pass
    def tpc_vote(self, t): pass
    def tpc_finish(self, t): pass
    def tpc_abort(self, t): pass
    def commit(self, t): pass

    def abort(self, t):
        self.changed_tables.add(self.table_name)

    def sortKey(self):
        return ''


class ReloadOnDbChange(object):
    def __init__(self, factory_class, reload_method, table_name):
        self.__listening = False
        self.changed_tables = set()
        self.factory_class = factory_class
        self.reload = reload_method
        self.table_name = table_name
        self.current_db_user = None

    def start(self):
        dbop.con().listen(self.table_name)
        self.current_db_user = dbop.dbquery("select current_user").fetchone()[0]

    def poll(self):
        notifies = dbop.con().poll()
        while notifies:
            try:
                notify = notifies.pop()
            except IndexError:  # кто-то уже вытащил это событие
                break
            if notify.payload == self.current_db_user:
                # мы сами сгенерировали это событие, игнорируем его
                continue
            logging.info('Got NOTIFY:', notify.pid, notify.channel, notify.payload)
            self.changed_tables.add(notify.channel)

    # Получение factory. Метод вызывается на этапе регистрации вокаба.
    def __call__(self, *args, **kw):
        self.start()
        factory = self.factory_class(*args, **kw)
        return _VocabFactoryProxy(self, factory)


# подменяет метод __call__ у factory
class _VocabFactoryProxy(object):
    def __init__(self, reloader, vocab_factory):
        self.__dict__['reloader'] = reloader
        self.__dict__['vocab_factory'] = vocab_factory

    # Получение вокаба в этой factory
    def __call__(self, *args, **kw):
        vocab = self.vocab_factory(*args, **kw)
        reloader = self.reloader
        reloader.poll()
        tab_name = reloader.table_name
        if tab_name in reloader.changed_tables:
            logging.debug('Reload %r due to database notify' % self.vocab_factory.vocabClass.regName)
            vocab.reload()
            # в случае transaction.abort возвращаем таблицу обратно в список измененных
            dm = _ReloadRollbackManager(reloader.changed_tables, tab_name)
            transaction.get().join(dm)
            reloader.changed_tables.discard(tab_name)
        return vocab

    def __getattr__(self, name):
        return getattr(self.vocab_factory, name)

    def __setattr__(self, name, value):
        setattr(self.vocab_factory, name, value)
