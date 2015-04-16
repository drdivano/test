# -*- coding: utf-8 -*-

"""Simple DB Vocabularies

$Id: simple.py 535 2009-12-01 11:31:27Z vmozhaev $
"""

from pyramid.vocabulary.simple import SimpleVocabulary
from rx.ormlite2.dbop import dbquery


class SimpleDBVocabulary(SimpleVocabulary):
    u"""Класс для создания простых словарей на основе данных из БД"""
    select = None
    def __init__(self, *args, **kwargs):
        assert self.select is not None
        items = dbquery(self.select).fetchall()
        super(SimpleDBVocabulary, self).__init__(items, *args, **kwargs)

