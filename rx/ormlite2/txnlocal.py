# -*- coding: utf-8 -*-

"""Transaction-local storage
$Id: $
"""

from zope.interface import implements
import transaction
import transaction.interfaces


class TxnLocalDataSynch(object):
    """ Creates and deletes transaction-local data """
    implements(transaction.interfaces.ISynchronizer)

    def __init__(self, dataObjectFactory):
        self.data = {}
        self.dataObjectFactory = dataObjectFactory

    def beforeCompletion(self, txn):
        pass

    def afterCompletion(self, txn):
        try:
            del self.data[id(txn)]
        except KeyError:
            pass

    def newTransaction(self, txn):
        pass

    def getData(self):
        try:
            return self.data[id(transaction.get())]
        except KeyError:
            self.data[id(transaction.get())] = self.dataObjectFactory()
            return self.data[id(transaction.get())]

class TxnLocalData(object):
    def __init__(self):
        self.registeredHook = False
        self.added_objects = []
        self.deleted_objects = set()
        self.bigVocabCache = {}
        self.localData = {}

txnLocalDataSynch = TxnLocalDataSynch(TxnLocalData)
getTxnLocalData = txnLocalDataSynch.getData
