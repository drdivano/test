# -*- coding: utf-8 -*-

"""
$Id: $
"""

import time


fn = 'test1.txt'

start_t = time.time()
for n in xrange(10000):
    with open(fn, 'a') as f:
        print >>f, ('some text ' * 10)

print 'elapsed: %.03f' % (time.time() - start_t)

fn = 'test2.txt'

start_t = time.time()
with open(fn, 'a') as f:
    for n in xrange(10000):
        print >>f, ('some text ' * 10)

print 'elapsed: %.03f' % (time.time() - start_t)
