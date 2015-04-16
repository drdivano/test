# -*- coding: utf-8 -*-

"""
$Id: $
"""

import sys
import re

import transaction
from pyramid.ormlite import dbquery
from pyramid.vocabulary import getV
from test_autoreload import SyncUnitsVocabulary
from pyramid.app import initializer as _initializer
import pyramid.vocabulary.mvcc

POSTGRES_USER, POSTGRES_PW = sys.argv[1:]  # для коннекта к базе используем отдельного юзера

import config
config.POSTGRES_DSN = re.sub(r"(password|user)='[^']*'", '', config.POSTGRES_DSN)
config.POSTGRES_DSN += " user='%s' password='%s'" % (POSTGRES_USER, POSTGRES_PW)


def main():
    _initializer.initDBConnection()
    pyramid.vocabulary.mvcc.register()
    SyncUnitsVocabulary.register()

    transaction.begin()
    assert len(getV('units')) == 0
    transaction.commit()

    print '--- go ahead $',
    sys.stdout.flush()

    sys.stdin.read(1)

    transaction.begin()
    assert len(getV('units')) == 1
    dbquery("update ut__orm_units set name='Директорат' where unit_id=-1")
    transaction.commit()


if __name__ == '__main__':
    main()

