__author__ = 'sarink'
from common import Requests, Storage


class NSBytes(Storage):
    def __init__(self, *args, **kwargs):
        Storage.__init__(self, *args, **kwargs)


class NSRequests(Requests):
    def __init__(self, *args, **kwargs):
        Requests.__init__(self, *args, **kwargs)


class Namespace:
    def __init__(self, name=None, bytes=None, requests=None):
        self.name = name
        self.bytes = bytes
        self.requests = requests

    def startElement(self, name, attrs):
        if name == 'bytes':
            self.bytes = NSBytes()
            return self.bytes
        elif name == 'requests':
            self.requests = NSRequests()
            return self.requests
        else:
            return None

    def endElement(self, name, value):
        if name == 'sname':
            self.name = value
        else:
            setattr(self, name, value)


class HTTP:
    def __init__(self, namespaces=None):
        self.namespaces = []
        if namespaces:
            self.namespaces = namespaces

    def startElement(self, name, attrs):
        if name == 'namespace':
            ns = Namespace()
            self.namespaces.append(ns)
            return ns
        else:
            return None

    def endElement(self, name, value):
        setattr(self, name, value)


class Services:
    def __init__(self, http=None):
        self.http = http

    def startElement(self, name, attrs):
        if name == 'http':
            self.http = HTTP()
            return self.http
        else:
            return None

    def endElement(self, name, value):
        setattr(self, name, value)