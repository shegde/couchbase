"""
Script to create and delete key-value pairs from the default bucket in Couchbase cluster.
It uses the Python CB client library to talk to the CB cluster

Useful links:
Need pip installed to install the python sdk, which may require easy_install.
http://docs.couchbase.com/developer/c-2.4/download-install.html
Client library - http://docs.couchbase.com/couchbase-sdk-python-1.2/#introduction
CB console - http://10.5.107.219:8091/index.html#sec=overview

"""

from couchbase import Couchbase
from couchbase.exceptions import CouchbaseError
from multiprocessing import Process
import uuid
import time

keyCount = 5000
cb_host = '10.5.107.217'
timeOut = 60
loopCount = 10
tokenSleep = 0
stepSleep = 0.1
sp_tokens = []
prefix = "Test.token."
value1 = {}
value1['partitionName'] = "resTest"

# create a connection to the cluster
c1 = Couchbase.connect(bucket='default', host=cb_host)
c1.timeout = timeOut

def createKeys():
    print "START: Number of tokens: %d" % (len(sp_tokens))
    for n in range(keyCount):
        id = uuid.uuid4().hex
        token = prefix + id
        sp_tokens.append(token)

    for token in sp_tokens:
        try:
            c1.set(token, value1)
            time.sleep(tokenSleep)
        except CouchbaseError as e:
            print e
    print "END: Number of tokens: %d" % (len(sp_tokens))

def getKeys():
    for t in sp_tokens:
        c1.get(t)

def deleteKeys():
    print "DEL START: Deleting %d keys" % len(sp_tokens)
    for t in sp_tokens[:]:
            try:
            c1.delete(t)
            sp_tokens.remove(t)
        except NotFoundError as e:
            print "Token does not exist %s" % (t)
            print e
    print "DEL END: Number of tokens: %d" % (len(sp_tokens))

if __name__ == '__main__':
    for i in range(loopCount):
        print "ITERATION: %d" % (i+1)
        createKeys()
        time.sleep(stepSleep)
    getKeys()
    deleteKeys()
    print "DONE"
