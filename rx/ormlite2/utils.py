# -*- coding: utf-8 -*-
import time, re
from datetime import datetime, date, time as datetime_time 


def now():
    #return int(time.time())
    return datetime.now()

try:
    strptime = time.strptime
except AttributeError:
    from strptime import strptime


def parseTime(s):
    m = re.search(r'\b(\d{1,2}:\d{1,2})\b', s)
    if m:
        return datetime_time(*strptime(m.expand(r'\1'), "%H:%M")[3:6])
    m = re.search(r'\b(\d{1,2}:\d{1,2}:\d{1,2})\b', s)
    if m:
        return datetime_time(*strptime(m.expand(r'\1'), "%H:%M:%S")[3:6])
    raise ValueError(s)

#    s = s.strip()
#    c = s.count(":")
#    if not c:
#        return datetime_time()
#    fmt = "%H:%M"
#    if c > 1:
#        fmt = "%H:%M:%S"
#    return datetime_time(*strptime(s, fmt)[3:6])
#
#    return dt


def parseDateTime(s):
    s = s.strip()
    sc = s.count(":")
    if not s:
        return datetime.now()
    if sc == 0:
        return parseDate(s)
    p1, p2 = s.split(" ")
    if p1.count(":"):
        t = p1
        d = p2
    else:
        t = p2
        d = p1

    return datetime.combine(parseDate(d), parseTime(t))
#    s = "%s %s" % (t, d)
#
#    year = d.split(".")[-1]
#    if len(year) == 4:
#        year_fmt = "%Y"
#    else:
#        year_fmt = "%y"
#
#    if sc == 1:
#        return datetime(*strptime(s, "%H:%M %d.%m." + year_fmt)[:5])
#    elif sc == 2:
#        return datetime(*strptime(s, "%H:%M:%S %d.%m." + year_fmt)[:6])
#
#    return dt
        

def parseDate(s):
    
    m = re.search(r'\b(\d{4}-\d{1,2}-\d{1,2})\b', s)
    if m:
        return date(*strptime(m.expand(r'\1'), "%Y-%m-%d")[:3])
    m = re.search(r'\b(\d{1,2}\.\d{1,2}\.\d{4})\b', s)
    if m:
        return date(*strptime(m.expand(r'\1'), "%d.%m.%Y")[:3])
    m = re.search(r'\b(\d{1,2}\.\d{1,2}\.\d{1,2})\b', s)
    if m:
        return date(*strptime(m.expand(r'\1'), "%d.%m.%y")[:3])
#    if re.match("\d{4}-\d{1,2}-\d{1,2}", s):
#        return date(*strptime(s, "%Y-%m-%d")[:3])
#    if re.match("\d{1,2}\.\d{1,2}\.\d{4}", s):
#        return date(*strptime(s, "%d.%m.%Y")[:3])
#    if re.match("\d{1,2}\.\d{1,2}\.\d{1,2}", s):
#        return date(*strptime(s, "%d.%m.%y")[:3])

    raise ValueError(s)


def timing(func):
    def wrapper(*args, **kw):
        start_t = time.time()
        r = func(*args, **kw)
        print "%s: %s elapsed" % (func.__name__, time.time() - start_t)
        return r    

    wrapper.__name__ = func.__name__
    return wrapper


def removeSQLComments(lines):
    u"""Убирает строки с sql-комментариями"""
    for line in lines:
        line = line.strip()
        if not line: continue
        if line.startswith('--'): continue
        yield line

def runSQL(sql_text, skip_errors=False, **kw):
    u"""Выполняет sql-скрипт"""
    from rx.ormlite2.dbop import dbquery, dbapi
    sql_text = '\n'.join(removeSQLComments(sql_text.split('\n')))
    for sql_statement in sql_text.split(';'):
        if not sql_statement: continue
        try:
            dbquery(sql_statement, **kw)
        except dbapi.DatabaseError:
            if skip_errors:
                continue
            else:
                raise
