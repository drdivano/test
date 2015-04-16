# -*- coding: utf-8 -*-

"""
$Id: $
Log handler with support for multiple processes writing to the log file.
"""

import os
import re
from stat import ST_MTIME, ST_CTIME
import time
import logging
import portalocker

_MIDNIGHT = 24 * 60 * 60  # number of seconds in a day


class FileLock(object):
    def __init__(self, f):
        self.f = f

    def __enter__(self):
        portalocker.lock(self.f, portalocker.LOCK_EX)

    def __exit__(self, exc_type, exc_val, exc_tb):
        portalocker.unlock(self.f)

    def close(self):
        if not self.f.closed:
            self.f.close()


class MPTimedRotatingFileHandler(logging.Handler):
    """
    Handler for logging to a file, rotating the log file at certain timed
    intervals.

    If backupCount is > 0, when rollover is done, no more than backupCount
    files are kept - the oldest ones are deleted.
    """
    # Parameter delay is not supported (makes no sense for this type of logs)
    def __init__(self, filename, when='h', interval=1, backupCount=0, encoding=None, utc=False):
        super(MPTimedRotatingFileHandler, self).__init__()
        self.baseFilename = os.path.abspath(filename)
        self.encoding = encoding
        self.when = when.upper()
        self.backupCount = backupCount
        self.utc = utc
        # Calculate the real rollover interval, which is just the number of
        # seconds between rollovers.  Also set the filename suffix used when
        # a rollover occurs.  Current 'when' events supported:
        # S - Seconds
        # M - Minutes
        # H - Hours
        # D - Days
        # midnight - roll over at midnight
        # W{0-6} - roll over on a certain day; 0 - Monday
        #
        # Case of the 'when' specifier is not important; lower or upper case
        # will work.
        if self.when == 'S':
            self.interval = 1 # one second
            self.suffix = "%Y-%m-%d_%H-%M-%S"
            extMatch = r"^\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}$"
        elif self.when == 'M':
            self.interval = 60 # one minute
            self.suffix = "%Y-%m-%d_%H-%M"
            extMatch = r"^\d{4}-\d{2}-\d{2}_\d{2}-\d{2}$"
        elif self.when == 'H':
            self.interval = 60 * 60 # one hour
            self.suffix = "%Y-%m-%d_%H"
            extMatch = r"^\d{4}-\d{2}-\d{2}_\d{2}$"
        elif self.when == 'D' or self.when == 'MIDNIGHT':
            self.interval = 60 * 60 * 24 # one day
            self.suffix = "%Y-%m-%d"
            extMatch = r"^\d{4}-\d{2}-\d{2}$"
        elif self.when.startswith('W'):
            self.interval = 60 * 60 * 24 * 7 # one week
            if len(self.when) != 2:
                raise ValueError("You must specify a day for weekly rollover from 0 to 6 (0 is Monday): %s" % self.when)
            if self.when[1] < '0' or self.when[1] > '6':
                raise ValueError("Invalid day specified for weekly rollover: %s" % self.when)
            self.dayOfWeek = int(self.when[1])
            self.suffix = "%Y-%m-%d"
            extMatch = r"^\d{4}-\d{2}-\d{2}$"
        else:
            raise ValueError("Invalid rollover interval specified: %s" % self.when)

        self.extMatch = re.compile(extMatch)
        self.interval = self.interval * interval # multiply by units requested

        self.file_lock = FileLock(self._open_lockfile(self.baseFilename))

        if os.path.exists(filename):
            stat_result = os.stat(filename)
            self.lastRolloverTime = stat_result[ST_CTIME]
            t = stat_result[ST_MTIME]
        else:
            t = time.time()
        self.rolloverAt = self.computeRollover(t)

    def _open_lockfile(self, file_name):
        # Use 'file.lock' and not 'file.log.lock' (Only handles the normal "*.log" case.)
        if file_name.endswith(".log"):
            lock_file_name = file_name[:-4]
        else:
            lock_file_name = file_name
        lock_file_name += ".lock"
        return open(lock_file_name, "w")

    def close(self):
        self.file_lock.close()


    def computeRollover(self, currentTime):
        """
        Work out the rollover time based on the specified time.
        """
        result = currentTime + self.interval
        # If we are rolling over at midnight or weekly, then the interval is already known.
        # What we need to figure out is WHEN the next interval is.  In other words,
        # if you are rolling over at midnight, then your base interval is 1 day,
        # but you want to start that one day clock at midnight, not now.  So, we
        # have to fudge the rolloverAt value in order to trigger the first rollover
        # at the right time.  After that, the regular interval will take care of
        # the rest.  Note that this code doesn't care about leap seconds. :)
        if self.when == 'MIDNIGHT' or self.when.startswith('W'):
            # This could be done with less code, but I wanted it to be clear
            if self.utc:
                t = time.gmtime(currentTime)
            else:
                t = time.localtime(currentTime)
            currentHour = t[3]
            currentMinute = t[4]
            currentSecond = t[5]
            # r is the number of seconds left between now and midnight
            r = _MIDNIGHT - ((currentHour * 60 + currentMinute) * 60 +
                    currentSecond)
            result = currentTime + r
            # If we are rolling over on a certain day, add in the number of days until
            # the next rollover, but offset by 1 since we just calculated the time
            # until the next day starts.  There are three cases:
            # Case 1) The day to rollover is today; in this case, do nothing
            # Case 2) The day to rollover is further in the interval (i.e., today is
            #         day 2 (Wednesday) and rollover is on day 6 (Sunday).  Days to
            #         next rollover is simply 6 - 2 - 1, or 3.
            # Case 3) The day to rollover is behind us in the interval (i.e., today
            #         is day 5 (Saturday) and rollover is on day 3 (Thursday).
            #         Days to rollover is 6 - 5 + 3, or 4.  In this case, it's the
            #         number of days left in the current week (1) plus the number
            #         of days in the next week until the rollover day (3).
            # The calculations described in 2) and 3) above need to have a day added.
            # This is because the above time calculation takes us to midnight on this
            # day, i.e. the start of the next day.
            if self.when.startswith('W'):
                day = t[6] # 0 is Monday
                if day != self.dayOfWeek:
                    if day < self.dayOfWeek:
                        daysToWait = self.dayOfWeek - day
                    else:
                        daysToWait = 6 - day + self.dayOfWeek + 1
                    newRolloverAt = result + (daysToWait * (60 * 60 * 24))
                    if not self.utc:
                        dstNow = t[-1]
                        dstAtRollover = time.localtime(newRolloverAt)[-1]
                        if dstNow != dstAtRollover:
                            if not dstNow:  # DST kicks in before next rollover, so we need to deduct an hour
                                addend = -3600
                            else:           # DST bows out before next rollover, so we need to add an hour
                                addend = 3600
                            newRolloverAt += addend
                    result = newRolloverAt
        return result

    def shouldRollover(self, record):
        """
        Determine if rollover should occur.

        record is not used, as we are just comparing times, but it is needed so
        the method signatures are the same
        """
        if not os.path.exists(self.baseFilename):
            return False
        stat_result = os.stat(self.baseFilename)
        if self.lastRolloverTime != stat_result[ST_CTIME]:
            self.lastRolloverTime = stat_result[ST_CTIME]
            self.rolloverAt = self.computeRollover(stat_result[ST_MTIME])
        return time.time() >= self.rolloverAt

    def getFilesToDelete(self):
        """
        Determine the files to delete when rolling over.

        More specific than the earlier method, which just used glob.glob().
        """
        dirName, baseName = os.path.split(self.baseFilename)
        fileNames = os.listdir(dirName)
        result = []
        prefix = baseName + "."
        plen = len(prefix)
        for fileName in fileNames:
            if fileName[:plen] == prefix:
                suffix = fileName[plen:]
                if self.extMatch.match(suffix):
                    result.append(os.path.join(dirName, fileName))
        result.sort()
        if len(result) < self.backupCount:
            result = []
        else:
            result = result[:len(result) - self.backupCount]
        return result

    def doRollover(self):
        """
        do a rollover; in this case, a date/time stamp is appended to the filename
        when the rollover happens.  However, you want the file to be named for the
        start of the interval, not the current time.  If there is a backup count,
        then we have to get a list of matching filenames, sort them and remove
        the one with the oldest suffix.
        """
        # get the time that this sequence started at and make it a TimeTuple
        currentTime = int(time.time())
        dstNow = time.localtime(currentTime)[-1]
        t = self.rolloverAt - self.interval
        if self.utc:
            timeTuple = time.gmtime(t)
        else:
            timeTuple = time.localtime(t)
            dstThen = timeTuple[-1]
            if dstNow != dstThen:
                if dstNow:
                    addend = 3600
                else:
                    addend = -3600
                timeTuple = time.localtime(t + addend)
        dfn = self.baseFilename + "." + time.strftime(self.suffix, timeTuple)
        if os.path.exists(dfn):
            os.remove(dfn)
        # Issue 18940: A file may not have been created if delay is True.
        if os.path.exists(self.baseFilename):
            os.rename(self.baseFilename, dfn)
        if self.backupCount > 0:
            for s in self.getFilesToDelete():
                os.remove(s)
        newRolloverAt = self.computeRollover(currentTime)
        while newRolloverAt <= currentTime:
            newRolloverAt = newRolloverAt + self.interval
        #If DST changes and midnight or weekly rollover, adjust for this.
        if (self.when == 'MIDNIGHT' or self.when.startswith('W')) and not self.utc:
            dstAtRollover = time.localtime(newRolloverAt)[-1]
            if dstNow != dstAtRollover:
                if not dstNow:  # DST kicks in before next rollover, so we need to deduct an hour
                    addend = -3600
                else:           # DST bows out before next rollover, so we need to add an hour
                    addend = 3600
                newRolloverAt += addend
        self.rolloverAt = newRolloverAt

    def emit(self, record):
        try:
            msg = self.format(record)
            with self.file_lock:
                if self.shouldRollover(record):
                    self.doRollover()
                self._write(msg, open(self.baseFilename, 'a'))
                self.flush()
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception:
            self.handleError(record)

    def _write(self, msg, stream):
        fs = "%s\n"
        try:
            if isinstance(msg, unicode) and getattr(stream, 'encoding', None):
                ufs = u'%s\n'
                try:
                    stream.write(ufs % msg)
                except UnicodeEncodeError:
                    #Printing to terminals sometimes fails. For example,
                    #with an encoding of 'cp1251', the above write will
                    #work if written to a stream opened or wrapped by
                    #the codecs module, but fail when writing to a
                    #terminal even when the codepage is set to cp1251.
                    #An extra encoding step seems to be needed.
                    stream.write((ufs % msg).encode(stream.encoding))
            else:
                stream.write(fs % msg)
        except UnicodeError:
            stream.write(fs % msg.encode("utf-8"))

