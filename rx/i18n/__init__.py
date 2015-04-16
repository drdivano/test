# -*- coding: utf-8 -*-
"""
$Id: $
"""

from zope.component import queryUtility, getUtility
from zope.i18n.interfaces import INegotiator, ILanguageAvailability
from pyramid.i18n import INegotiator
from pyramid.i18n.message import MessageFactory
import pyramid.i18n
import config

_ = MessageFactory('messages')
translate = lambda s: pyramid.i18n.translate(s, domain='messages')

__ = unicode # __ маркирует строку для будущего перевода, в тех случаях, когда в текущей версии перевод не требуется

def date_format():
    negotiator = queryUtility(INegotiator)
    if negotiator and negotiator.getLanguage(['ru', 'en'], None) == 'en':
        return '%m/%d/%Y'
    return '%d.%m.%Y'
        
def format_datetime(dt, omit_seconds=False):
    # TODO: выводить часовой пояс
    return dt.strftime((omit_seconds and '%H:%M ' or '%H:%M:%S ') + date_format())

def format_date(dt):
    return dt.strftime(date_format())

def get_current_lang(available=None):
    negotiator = queryUtility(INegotiator)
    if negotiator is not None:
        if not available:
            available = [ elem[0] for elem in \
                getUtility(ILanguageAvailability).getAvailableLanguages() ]
        lang = negotiator.getLanguage(available, None)
        if lang:
            return lang
    return config.FALLBACK_LANGUAGES[0]
