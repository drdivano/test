# -*- coding: utf-8 -*-

import os
import unittest
import testoob
#import testlib


if __name__ == "__main__":

    testdir = os.path.split(__file__)[0]
    testfiles = [ f[:-3] for f in os.listdir(testdir or '.') \
                 if f.startswith('test_') and f.endswith('.py') ]
    modules = [ __import__(file) for file in testfiles ]
    test_loader = unittest.TestLoader()
    tests = [ test_loader.loadTestsFromModule(module) for module in modules ]
    testoob.main(unittest.TestSuite(tests))
