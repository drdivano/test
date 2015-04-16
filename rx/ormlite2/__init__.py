# -*- coding: utf-8 -*-

"""ORM-related modules.

$Id: __init__.py 535 2009-12-01 11:31:27Z vmozhaev $
"""

from rx.ormlite2.exc import PersistenceError
from rx.ormlite2.record import Record, ActiveRecord
from rx.ormlite2 import record
from rx.ormlite2.dbop import dbquery
from rx.ormlite2.sql import sql_repr

from rx.ormlite2.vocabulary.adapters import Record2TermAdapter

