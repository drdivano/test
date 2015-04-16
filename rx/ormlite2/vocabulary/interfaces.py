# -*- coding: utf-8 -*-

"""DB Vocabulary Interfaces.

$Id$
"""

import zope.schema

from pyramid.vocabulary.interfaces import IObjectVocabulary


class IRDBVocabulary(IObjectVocabulary):
    u"""Словарь с поддержкой загрузки объектов из БД."""
