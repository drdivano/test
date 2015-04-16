# -*- coding: utf-8 -*-

from zope.interface import implements, interface
from ORMLite import schema, ActiveRecord

class IHoursRecord(interface.Interface):
    recId = schema.Int(db_column="rec_id", primary_key=True)
    userId = schema.Int(db_column="user_id")
    projectId = schema.Int(db_column="project_id")
    recDate = schema.Datetime(db_column="rec_date")
    amount = schema.Float(db_column="amount")
    description = schema.Text(db_column="description")
    origProjectId = schema.Int(db_column="orig_project_id")


class HoursRecord(ActiveRecord):
    implements(IHoursRecord)
    p_table_name = "hours"



if __name__ == "__main__":
    import time, dbop
    start_t = time.time()
    hours = HoursRecord.bulkLoad(dbop.dbquery("select * from hours"))
    print "bulkLoad: %s records in %s sec" % (len(hours), time.time() - start_t)
    start_t = time.time()
    for ob in hours.values():
        ob2 = HoursRecord()
        ob2.recId = ob.recId
        ob2.load()

    print "ob.load(): %s records in %s sec" % (len(hours), time.time() - start_t)
    
