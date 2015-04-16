# -*- coding: utf-8 -*-
"""Прокладка для использования Django-форм
$Id: $
"""

import django.forms
from django.forms import *
from django.core import validators
from pyramid.vocabulary import getV

from rx.utils import enumerate_fields

def ob_to_dict(ob, exclude=[], include=[]):
    r = {}
    for name, field in enumerate_fields(ob, exclude, include):
        r[name] = field.get(ob)
    return r

def dict_to_ob(ob, data, exclude=[], include=[]):
    for name, field in enumerate_fields(ob, exclude, include):
        if name in data:
            field.set(ob, data[name])

def vocab_to_choices(vocab_name, initial=None, sort_key=None, restricted_keys=[], default_item_label=''):
    vocabulary = getV(vocab_name)
    terms = [ vocabulary.getTerm(v) for v in vocabulary ]
    if sort_key:
        # TODO: для сортировки использовать collator
        terms.sort(key=sort_key)
    choices = [ (term.token, term.title or term.value) for term in terms
                if (term.token not in restricted_keys or term.token == initial)]
    if '' not in vocabulary:
        choices.insert(0, ('', default_item_label))
    if initial is not None and initial not in dict(choices):
        choices.insert(1, (initial, initial))
    return choices

    
       

if __name__ == '__main__':

    from pyramid.app.auth import SimpleUser
    u = SimpleUser()
    u.identity = 1
    u.firstName = u'John'
    u.middleName = u'X.'
    u.lastName = u'Doe'
    u.email = 'test@example.com'
    u.role = 'manager'
    
    d = ob_to_dict(u)
    assert d['lastName'] == u'Doe'

    class CommentForm(Form):
        firstName = CharField(initial='First name')
        lastName = CharField(initial='Last name')

    f = CommentForm(auto_id=False, data=d)
    print f

    dict_to_ob(u, {'firstName': 'Sam', 'lastName': 'Smith'})
    assert u.lastName == 'Smith'
    assert u.firstName == 'Sam'
