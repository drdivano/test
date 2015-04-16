# -*- coding: utf-8 -*-

import unittest
import testoob
import datetime
from pyramid.tests import testlib

from zope.component import provideUtility, queryUtility
from pyramid.ormlite import dbquery
from pyramid.i18n import INegotiator
import rx.i18n.translation

class TestNegotiator:
    LANG = 'ru'
    def getLanguage(self, langs, env):
        if TestNegotiator.LANG in langs:
            return TestNegotiator.LANG
        else:
            return langs[0]

negotiator = TestNegotiator()

class Test(unittest.TestCase):
    def setUp(self):
        super(Test, self).setUp()
        self._old_negotiator = queryUtility(TestNegotiator(), INegotiator)

        provideUtility(TestNegotiator(), INegotiator)
        from pyramid.i18n.negotiator import LanguageAvailability
        provideUtility(LanguageAvailability(['ru', 'en']))
        rx.i18n.translation.init_i18n()

    def tearDown(self):
        super(Test, self).tearDown()
        provideUtility(self._old_negotiator, INegotiator)

    def test_bilingual(self):
        m = rx.i18n.translation.bilingual(u'Санкт-Тестербург', u'Saint-Testerburg')
        TestNegotiator.LANG = 'ru'
        self.assertEqual(m, u'Санкт-Тестербург')
        TestNegotiator.LANG = 'en'
        self.assertEqual(m, 'Saint-Testerburg')

    def test_self_translated(self):
        m = rx.i18n.translation.self_translated(ru=u'Москва', en=u'Moscow', fr=u'Moscou')
        TestNegotiator.LANG = 'ru'
        self.assertEqual(m, u'Москва')
        TestNegotiator.LANG = 'en'
        self.assertEqual(m, 'Moscow')
        TestNegotiator.LANG = 'fr'
        self.assertNotEqual(m, 'Moscou')

        from pyramid.i18n.negotiator import LanguageAvailability
        provideUtility(LanguageAvailability(['ru', 'en', 'fr']))

        self.assertEqual(m, 'Moscou')


        m2 = rx.i18n.translation.self_translated()
        self.assertEqual(m2, '')
        
        m3 = rx.i18n.translation.self_translated(en=u'Moscow')
        TestNegotiator.LANG = 'ru'
        self.assertEqual(m3, u'Moscow')


if __name__ == "__main__":
    testoob.main()
