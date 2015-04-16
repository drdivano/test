# -*- coding: utf-8 -*-

"""
$Id: $
"""

import os
import time
import tempfile
import unittest
import testoob
import logging
import re
import cStringIO
from rx.log.mplogging import MPTimedRotatingFileHandler

class BaseTest(unittest.TestCase):

    """Base class for logging tests."""

    log_format = "%(name)s -> %(levelname)s: %(message)s"
    expected_log_pat = r"^([\w.]+) -> ([\w]+): ([\d]+)$"
    message_num = 0

    def setUp(self):
        """Setup the default logging stream to an internal StringIO instance,
        so that we can examine log output as we want."""
        logger_dict = logging.getLogger().manager.loggerDict
        logging._acquireLock()
        try:
            self.saved_handlers = logging._handlers.copy()
            self.saved_handler_list = logging._handlerList[:]
            self.saved_loggers = logger_dict.copy()
            self.saved_level_names = logging._levelNames.copy()
        finally:
            logging._releaseLock()

        # Set two unused loggers: one non-ASCII and one Unicode.
        # This is to test correct operation when sorting existing
        # loggers in the configuration code. See issue 8201.
        logging.getLogger("\xab\xd7\xbb")
        logging.getLogger(u"\u013f\u00d6\u0047")

        self.root_logger = logging.getLogger("")
        self.original_logging_level = self.root_logger.getEffectiveLevel()

        self.stream = cStringIO.StringIO()
        self.root_logger.setLevel(logging.DEBUG)
        self.root_hdlr = logging.StreamHandler(self.stream)
        self.root_formatter = logging.Formatter(self.log_format)
        self.root_hdlr.setFormatter(self.root_formatter)
        self.root_logger.addHandler(self.root_hdlr)

    def tearDown(self):
        """Remove our logging stream, and restore the original logging
        level."""
        self.stream.close()
        self.root_logger.removeHandler(self.root_hdlr)
        while self.root_logger.handlers:
            h = self.root_logger.handlers[0]
            self.root_logger.removeHandler(h)
            h.close()
        self.root_logger.setLevel(self.original_logging_level)
        logging._acquireLock()
        try:
            logging._levelNames.clear()
            logging._levelNames.update(self.saved_level_names)
            logging._handlers.clear()
            logging._handlers.update(self.saved_handlers)
            logging._handlerList[:] = self.saved_handler_list
            loggerDict = logging.getLogger().manager.loggerDict
            loggerDict.clear()
            loggerDict.update(self.saved_loggers)
        finally:
            logging._releaseLock()

    def assert_log_lines(self, expected_values, stream=None):
        """Match the collected log lines against the regular expression
        self.expected_log_pat, and compare the extracted group values to
        the expected_values list of tuples."""
        stream = stream or self.stream
        pat = re.compile(self.expected_log_pat)
        try:
            stream.reset()
            actual_lines = stream.readlines()
        except AttributeError:
            # StringIO.StringIO lacks a reset() method.
            actual_lines = stream.getvalue().splitlines()
        self.assertEqual(len(actual_lines), len(expected_values))
        for actual, expected in zip(actual_lines, expected_values):
            match = pat.search(actual)
            if not match:
                self.fail("Log line does not match expected pattern:\n" +
                            actual)
            self.assertEqual(tuple(match.groups()), expected)
        s = stream.read()
        if s:
            self.fail("Remaining output at end of log stream:\n" + s)

    def next_message(self):
        """Generate a message consisting solely of an auto-incrementing
        integer."""
        self.message_num += 1
        return "%d" % self.message_num


class MPLoggingTest(BaseTest):
    def setUp(self):
        super(MPLoggingTest, self).setUp()
        fd, self.log_file_name = tempfile.mkstemp('.log')
        os.close(fd)

        self.mp_handler = MPTimedRotatingFileHandler(self.log_file_name, when='D')
        self.mp_logger = logging.getLogger('mplog')
        self.mp_logger.propagate = 0
        self.mp_logger.addHandler(self.mp_handler)

    def tearDown(self):
        super(MPLoggingTest, self).tearDown()
        self.mp_handler.close()

        d = os.path.dirname(self.log_file_name)
        for fn in self._log_file_names():
            os.unlink(d + os.sep + fn)
        os.unlink(self.mp_handler.file_lock.f.name)

    def _log_file_names(self):
        return [fn for fn in os.listdir(os.path.dirname(self.log_file_name))
                if fn.startswith(os.path.basename(self.log_file_name))]

    def test_rollover(self):
        record = logging.LogRecord(None, None, "", 0, "test", (), None, None)
        self.mp_handler.emit(record)
        self.mp_handler.rolloverAt = time.time()
        self.mp_handler.emit(record)

        file_names = self._log_file_names()
        self.assertEqual(len(file_names), 2)

    def _make_record(self, text):
        return logging.LogRecord(None, None, "", 0, str(text), (), None, None)

    def test_concurrent_rollover(self):
        _48h_ago = time.time() - 48 * 3600
        yesterday = time.time() - 24 * 3600
        open(self.log_file_name, 'w').write('1\n2\n')

        mp_handler2 = MPTimedRotatingFileHandler(self.log_file_name, when='D')
        self.mp_handler.lastRolloverTime = mp_handler2.lastRolloverTime = _48h_ago
        self.mp_handler.rolloverAt = mp_handler2.rolloverAt = time.time()
        os.utime(self.log_file_name, (yesterday, yesterday))
        mp_handler2.emit(self._make_record(3))  # performs rollover
        self.mp_handler.emit(self._make_record(4))  # no rollover

        file_names = self._log_file_names()
        self.assertEqual(len(file_names), 2)
        self.assertEqual(open(self.log_file_name).read(), '3\n4\n')


if __name__ == "__main__":
    testoob.main()
