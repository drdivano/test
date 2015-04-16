# -*- coding: utf-8 -*-

#import config
from pyramid.utils import Config as config
from zope.interface import implements, interface
from rx.ormlite2 import schema
from rx.ormlite2.interfaces import IActiveRecord
from rx.ormlite2 import psycopgcon
from rx.ormlite2.dbop import dbquery
from rx.ormlite2.record import ActiveRecord

from zope.component import provideUtility
from rx.ormlite2.interfaces import IDatabaseAdapter, IDatabaseConfig
provideUtility(psycopgcon, IDatabaseAdapter)
provideUtility(config, IDatabaseConfig)


def testLOB():

    dbquery("create table _ut_lob_test (k varchar(16), data_lob oid)")

    c = dbquery("select * from pg_largeobject")
    
    from rx.ormlite2.psycopgcon import con
    from rx.ormlite2 import lob
    
    class IOb(IActiveRecord):
        recId = schema.Int(db_column="k",
                           primary_key=True, readonly=True)
        data = schema.LOB(db_column="data_lob")

    class Ob(ActiveRecord):
        implements(IOb)
        p_table_name = '_ut_lob_test'


    largestr = '1234567890' * 100

    ob = Ob(k=55)

    import time
    start_t = time.time()
    for n in range(1000):
        ob.data.write(largestr)

    ob.save()

    print "Write time:", time.time() - start_t

    ob2 = Ob(k=55).load()

    print "len=", ob2.data.seek(0, 2)

    ob2.data.seek(0)
    
    start_t = time.time()
    d = ob2.data.read()
    print "Read time:", time.time() - start_t
    print "len=", len(d)
    

if __name__ == "__main__":
    testLOB()

