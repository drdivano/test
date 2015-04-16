# -*- coding: utf-8 -*-

"""
$Id: $

Получение справочников через PBus
"""


from zope.schema.interfaces import ITokenizedTerm
from pyramid.ormlite import dbquery


def group_by_class(json_obs):
    r = {}
    for ob in json_obs:
        r.setdefault(ob['class'], []).append(ob)
    return r


# deprecated
def process_message(msg_data, event_handlers):
    if isinstance(msg_data, list):
        events = msg_data
    elif isinstance(msg_data, dict) and 'event' in msg_data:  # ответ на запрос (например, command get_all)
        events = [msg_data]
    else:
        events = []

    for event in events:
        event_type = event.get('event')
        for cls_name, json_obs in group_by_class(event['objects']).items():
            try:
                handler = event_handlers[cls_name]
            except KeyError:
                continue
            handler(event_type, json_obs)

    return ''


REPLACE_ALL_MARKER = object()

class VocabularyEvent(object):
    """ Событие обновления справочника
    """
    # Зазор между стадиями decode и update_vocab позволяет выполнять обработку в определенном порядке, а также
    # вставлять необходимые групповые операции (например, индексирование).
    def __init__(self, event_type, event_objects):
        self.type = event_type
        self.objects = event_objects

    @classmethod
    def from_json(cls, msg_data):
        if isinstance(msg_data, list):
            events = msg_data
        elif isinstance(msg_data, dict) and 'event' in msg_data:  # ответ на запрос (например, command get_all)
            events = [msg_data]
        else:
            # we don't understand this message
            events = []
        for event_json in events:
            yield cls(event_json['event'], event_json['objects'])


    def decode(self, decoders):
        for cls_name, json_obs in group_by_class(self.objects).items():
            objects = []
            try:
                decode = decoders[cls_name]
            except KeyError:
                continue
            for json_ob in json_obs:
                ob = decode(json_ob)
                if ob is None:
                    continue
                objects.append(ob)
            yield cls_name, objects


    def update_vocabulary(self, objects, vocab):
        changes = []
        if self.type in ('add', 'change', 'replace_all'):
            seen = set()
            for ob in objects:
                k = ITokenizedTerm(ob).token
                seen.add(k)
                old_ob = vocab.get(k)
                if old_ob == ob:
                    continue
                vocab.add(ob)  # добавление или замена элемента вокаба
                changes.append([old_ob, ob])
            if self.type == 'replace_all':
                for k in vocab.keys():
                    if k not in seen:
                        old_ob = vocab.get(k)
                        del vocab[k]
                        old_ob.delete()
                        changes.append([old_ob, None])
        elif self.type == 'delete':
            for ob in objects:
                k = ITokenizedTerm(ob).token
                try:
                    old_ob = vocab.get(k)
                    del vocab[k]
                    old_ob.delete()
                    changes.append([old_ob, None])
                except KeyError:
                    pass
        return changes


    # Более компактный и эффективный алгоритм для MvccVocabulary
    # семантика changes отличается от метода update_vocabulary
    def update_mvcc(self, objects, vocab, db_table_name):
        changes = []
        if self.type in ('add', 'change', 'replace_all'):
            if self.type == 'replace_all':
                if db_table_name is not None:
                    dbquery("delete from %s" % db_table_name)
                vocab.replace_all(objects)
                changes.append(REPLACE_ALL_MARKER)
            else:
                vocab.update_many(objects)
            changes += objects
            if db_table_name is not None:
                for ob in objects:
                    ob.save()
        elif self.type == 'delete':
            delete_tokens = []
            for ob in objects:
                if db_table_name is not None:
                    ob.delete()
                delete_tokens.append(ITokenizedTerm(ob).token)
                changes.append(ob)
            vocab.delete_many(delete_tokens)

        return changes
