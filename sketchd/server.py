"""
"""
from twisted.application.service import Application
from twisted.application.internet import UDPServer
from twisted.application.service import MultiService
from twisted.python import log

from sketch.protocol import SketchUDPServerProtocol
from sketch.protocol import MessageProcessor
from sketch.algorithm.heavyhitters import SpaceSaving


LISTEN_PORT = 4007


class Dispatcher(object):

    factory_table = {}

    def register(self, method, factory):
        self.factory_table[method] = factory

    def dispatch(self, method, key, value, *args):
        if method not in self.factory_table:
            log.err("Method '%s' not registered. " % method)
        else:
            factory = self.factory_table[method]
            factory[key].update(value, *args)


class SketchFactory(object):

    def __init__(self, clazz, *args, **kargs):
        self.clazz = clazz
        self.args = args
        self.kargs = kargs
        self.table = {}

    def __getitem__(self, key):
        if key not in self.table:
            log.msg("Create: '%s' [%s]" % (key, self.clazz.__name__))
            self.table[key] = self.clazz(key, *self.args, **self.kargs)

        return self.table[key]


dispatcher = Dispatcher()
dispatcher.register("hh", SketchFactory(SpaceSaving))


sketch_udp_protocol = SketchUDPServerProtocol(MessageProcessor(dispatcher))

server = UDPServer(LISTEN_PORT, sketch_udp_protocol)

application = Application("sketch")

server.setServiceParent(application)
