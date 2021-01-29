#
# Test multi-threading for the xcrpc code.
#
import pytest

import xcalar.external.client
from xcalar.external.exceptions import XDPException
import threading

from xcalar.compute.services.Echo_xcrpc import Echo, EchoRequest, EchoErrorRequest

client = None
serv = None

class EchoThread(threading.Thread):
    def __init__(self, thread_id):
        threading.Thread.__init__(self)
        self.thread_id = thread_id

    def run(self):
        for i in range(1000):
            echoStr = "Echo #" + str(i) + " from " + str(self.thread_id)
            echo = EchoRequest()
            echo.echo = echoStr
            resp = serv.echoMessage(echo)
            #print("Echoed=" + str(echoStr) + " resp.echo=" + resp.echoed)
            assert resp.echoed == echoStr

def testEcho():
    global client
    global serv

    client = xcalar.external.client.Client(bypass_proxy=True)
    serv = Echo(client)

    threads = {}
    for i in range(100):
        threads[i] = EchoThread(i)
    for thread in threads.values():
        thread.start()
    for thread in threads.values():
        thread.join()
