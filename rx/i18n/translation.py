# -*- coding: utf-8 -*-

"""
$Id: $
"""

from zope.component import getUtility, provideUtility
from zope.component.interfaces import ComponentLookupError
from zope.i18n.interfaces import ITranslationDomain, INegotiator
from zope.i18n import interpolate
from zope.interface import implements
from pyramid.i18n.message import Message
import config


class BilingualTranslationDomain(object):
    u'''Переводчик, работающий без каталога переводов, с сообщениями, содержащими внутри себя свои переводы.
        Текущая версия поддерживает работу с 2 языками - ru и en'''
    implements(ITranslationDomain)

    def translate(self, msgid, mapping=None, context=None,
                  target_language=None, default=None):
        '''See interface ITranslationDomain'''
        if target_language is None:
            try:
                negotiator = getUtility(INegotiator)
            except ComponentLookupError:
                target_language = config.FALLBACK_LANG
            else:
                target_language = negotiator.getLanguage(['ru', 'en'], context)
        if target_language == 'ru':
            text = msgid
        else:
            text = default
        if text is None:
            text = default
        return interpolate(text, mapping)

def bilingual(ru, en):
    return Message(ru, default=en, domain='bilingual_translate')

# TODO: поддержка имен локалей (en_US, en_GB), см. zope/i18n/negotiator.py
class SelfTranslationDomain(object):
    u'''Переводчик, работающий без каталога переводов, с сообщениями, содержащими внутри себя свои переводы.
        Msgid сообщения является объектом типа dict, ключ - код языка, значение - текст на этом языке'''

    implements(ITranslationDomain)

    def translate(self, text_by_lang, mapping=None, context=None,
                  target_language=None, default=None):
        '''See interface ITranslationDomain'''
        if not text_by_lang:
            return u''
        if target_language is None:
            negotiator = getUtility(INegotiator)
            target_language = negotiator.getLanguage(text_by_lang.keys(), context)
        try:
            text = text_by_lang[target_language]
        except KeyError:
            for lang in config.FALLBACK_LANGUAGES:
                if lang in text_by_lang:
                    text = text_by_lang[lang]
                    break
                text = text_by_lang.values()[0]  # хоть какое-то сообщение
        return interpolate(text, mapping)

def self_translated(**text_by_lang):
    return Message(text_by_lang, domain='self_translate')


def init_i18n():
    provideUtility(BilingualTranslationDomain(), name='bilingual_translate')
    provideUtility(SelfTranslationDomain(), name='self_translate')

