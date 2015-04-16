# -*- coding: utf-8 -*-

"""ORMLite Exceptions.

$Id: exc.py 774 2010-06-24 12:28:47Z anovgorodov $
"""

from zope.interface import Invalid
from pyramid.exc import PyramidException
from pyramid.i18n.message import MessageFactory
_ = MessageFactory('pyramid')

# TODO: Это ошибка выборки данных из БД. Придумать более подходящее название и переименовать
class PersistenceError(PyramidException, LookupError):
    msg = _(u'Не найдены данные по заданному ключу')

class ModelError(PyramidException, Invalid):
    msg = _(u'Ошибка данных модели')

class RequiredFieldsError(ModelError):
    msg = _(u'Не заполнены все обязательные поля')
