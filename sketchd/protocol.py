import re

from twisted.internet.protocol import DatagramProtocol
from twisted.internet.protocol import Factory
from twisted.protocols.basic import LineReceiver

from twisted.python import log


class MessageProcessor(object):
    """Parses messages in Sketch protocoll and forwards to dispatcher. """

    pattern = re.compile(
        r"^([^:]+):(\"[^\"]+\"|[^\|]+)\|([\w]+)(\|[\w\|]+)?\s?$")

    def __init__(self, dispatcher):
        self.dispatcher = dispatcher

    def process(self, data):
        """Parses input ``data`` using regular expression. """
        m = self.pattern.match(data)
        if not m or len(m.groups()) != 4:
            log.err("Cannot parse data '%s'" % data)
        else:
            key, value, method, params = m.groups()
            if params:
                params = params.split("|")
            else:
                params = []
            self.dispatcher.dispatch(method, key, value, *params)


class SketchUDPServerProtocol(DatagramProtocol):
    """Implementation of the Sketch protocol via UDP. """

    def __init__(self, processor):
        print("generating SketchUDPServerProtocol")
        self.processor = processor

    def datagramReceived(self, data, (host, port)):
        """Process received data. """
        print "received %r from %s:%d" % (data, host, port)
        self.processor.process(data)

    def connectionRefused(self):
        print "No one listening"


class SketchTCPServerProtocol(LineReceiver):
    """Implementation of the Sketch protocol via TCP. """

    def __init__(self, processor):
        self.processor = processor

    def lineReceived(self, data):
        """Process received data. """
        self.processor.process(data)


class SketchTCPServerFactory(Factory):
    """Factory for Sketch TCP protocol. """

    def __init__(self, processor):
        self.processor = processor

    def buildProtocol(self, addr):
        return SketchTCPServerProtocol(self.processor,
            self.monitor_message, self.monitor_response)
