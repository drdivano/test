# -*- coding: utf-8 -*-

"""
$Id: $
"""

import unittest
import testoob
import mock

from pyramid.tests import testlib
from pyramid.registry import registerFromModule, registerVocabulary, unregisterVocabulary
from pyramid.registry.interfaces import IRegisterableVocabulary
from pyramid.vocabulary import getV
import pyramid.vocabulary.mvcc
import pyramid.i18n
from zope.interface.declarations import implements
from zope.schema.interfaces import ITokenizedTerm

import rx.pbus
from rx.pbus.vocabulary import group_by_class, VocabularyEvent

class _TestVocab(object):
    def __init__(self):
        self.adding = []
        self.updating = []
        self.deleting = []
        self.clearing = False
        self.saved = False

    def add_many(self, obs):
        self.adding += obs

    def update_many(self, obs):
        self.updating += obs

    def delete_many(self, obs):
        self.deleting += obs

    def replace_all(self, obs):
        self.clearing = True
        self.add_many(obs)


class Ob(object):
    implements(ITokenizedTerm)

    def __init__(self, k, v, *args, **kw):
        super(Ob, self).__init__()
        self.value = self
        self.token = self.k = k
        self.v = v
        self.saving = self.deleting = False

    def save(self):
        self.saving = True

    def delete(self):
        self.deleting = True

def decode_ob(data):
    if data['k'] == -999: return None
    return Ob(data['k'], data['v'])

DECODERS = {'Ob': decode_ob}


class TestModifyVocabs(unittest.TestCase):
    DEFAULT_NEGOTIATOR_LANG = 'ru'

    def setUp(self):
        super(TestModifyVocabs, self).setUp()

    def tearDown(self):
        super(TestModifyVocabs, self).tearDown()


    def test_group_by_class(self):
        data = [{'class': 'Ob1', 'k': 1, 'v': 'a'}, {'class': 'Ob1', 'k': 2, 'v': 'b'},
                {'class': 'Ob2', 'k': 3, 'v': 'c'}]
        grouped = group_by_class(data)
        self.assertEqual(len(grouped), 2)
        self.assertEqual(len(grouped['Ob1']), 2)
        self.assertEqual(len(grouped['Ob2']), 1)


    def test_decode_msg(self):
        msg = {'event': 'replace_all',
               'objects': [{'class': 'Ob', 'k': 1, 'v': 'a'},
                           {'class': 'Ob', 'k': -999, 'v': 'xxx'}]}
        events = list(VocabularyEvent.from_json(msg))
        self.assertEqual(len(events), 1)

        event = events[0]
        self.assertEqual(event.type, 'replace_all')

        decoded = list(event.decode(DECODERS))
        self.assertEqual(len(decoded), 1)
        cls, obs = decoded[0]
        self.assertEqual(cls, 'Ob')
        self.assertEqual(len(obs), 1)


    def test_modify_vocab(self):
        event = VocabularyEvent('replace_all', [{'class': 'Ob', 'k': 1, 'v': 'a'}])
        decoded = list(event.decode({'Ob': decode_ob}))
        cls, obs = decoded[0]

        vocab = _TestVocab()
        with mock.patch('rx.pbus.vocabulary.dbquery') as dbquery:
            event.update_mvcc(obs, vocab, 'table')
            self.assertTrue(vocab.clearing)
            #self.assertTrue(dbquery.called)
            dbquery.assert_has_any_call('delete from table')
            self.assertEqual(len(vocab.adding), 1)

        event.type = 'add'
        vocab = _TestVocab()
        with mock.patch('rx.pbus.vocabulary.dbquery') as dbquery:
            event.update_mvcc(obs, vocab, 'table')
            self.assertFalse(vocab.clearing)
            self.assertTrue(obs[0].saving)
            self.assertEqual(len(vocab.updating), 1)

        event.type = 'delete'
        vocab = _TestVocab()
        with mock.patch('rx.pbus.vocabulary.dbquery') as dbquery:
            event.update_mvcc(obs, vocab, 'table')
            self.assertFalse(vocab.clearing)
            self.assertTrue(obs[0].deleting)
            self.assertEqual(len(vocab.deleting), 1)



if __name__ == "__main__":
    testoob.main()
