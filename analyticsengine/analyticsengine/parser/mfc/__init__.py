__author__ = 'sarink'

from system import System
from glbl import Glbl
from services import Services
from analyticsengine.parser.mfc.common import Serialize


class MfcResponse:
    def __init__(self, header=None, data=None):
        self.header = header
        self.data = data

    def __repr__(self):
        return "MfcResponse: MFC Response Object"

    def startElement(self, name, attrs):
        if name == 'header':
            self.header = Header()
            return self.header
        elif name == 'data':
            self.data = Data()
            return self.data
        else:
            return None

    def endElement(self, name, value):
        return None

    def to_json(self):
        return Serialize.to_json(self)

class Header:
    def __init__(self, status_code=None, status_msg=None):
        self.status_code = status_code
        self.status_msg = status_msg

    def startElement(self, name, attrs):
        return None

    def endElement(self, name, value):
        if name == 'code':
            try:
                self.status_code = int(value)
            except ValueError:
                self.status_code = value
        elif name == 'message':
            self.status_msg = value
        else:
            setattr(self, name, value)


class Data:
    def __init__(self, system=None, glbl=None, services=None):
        self.timestamp = None
        self.start_time = None
        self.system = system
        self.glbl = glbl
        self.services = services

    def startElement(self, name, attrs):
        if name == 'system':
            self.system = System()
            return self.system
        elif name == 'global':
            self.glbl = Glbl()
            return self.glbl
        elif name == 'services':
            self.services = Services()
            return self.services
        else:
            return None

    def endElement(self, name, value):
        if name == 'timestamp':
            try:
                self.timestamp = int(value)
            except ValueError:
                self.timestamp = value
        elif name == 'start-time':
            try:
                self.start_time = int(value)
            except ValueError:
                self.start_time = value
        elif name != 'mfc-stats' and name != 'mfc-all-stats':
            print "im adding..", name
            setattr(self, name, value)
        else:
            return
