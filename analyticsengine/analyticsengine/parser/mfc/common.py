__author__ = 'sarink'
import json


class Serialize(object):
    @staticmethod
    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True)


class Usage:
    def __init__(self, total=None, used=None):
        self.total = total
        self.used = used

    def startElement(self, name, attrs):
        return None

    def endElement(self, name, value):
        if name == 'total':
            try:
                self.total = int(value)
            except ValueError:
                self.total = value
        elif name == 'used':
            try:
                self.used = int(value)
            except ValueError:
                self.used = value
        else:
            setattr(self, name, value)


class Requests:
    def __init__(self, total=None, active=None, cache_hit=None):
        self.total = total
        self.active = active
        self.cache_hit = cache_hit

    def startElement(self, name, attrs):
        return None

    def endElement(self, name, value):
        if name == 'total':
            try:
                self.total = int(value)
            except ValueError:
                self.total = value
        elif name == 'active':
            try:
                self.active = int(value)
            except ValueError:
                self.active = value
        elif name == 'cache-hit':
            try:
                self.cache_hit = int(value)
            except ValueError:
                self.cache_hit = value
        else:
            setattr(self, name, value)


class Storage:
    def __init__(self, ram=None, disk=None, nfs=None, origin=None):
        self.ram = ram
        self.disk = disk
        self.nfs = nfs
        self.origin = origin

    def startElement(self, name, attrs):
        return None

    def endElement(self, name, value):
        if name == 'ram':
            try:
                self.ram = int(value)
            except ValueError:
                self.ram = value
        elif name == 'disk':
            try:
                self.disk = int(value)
            except ValueError:
                self.disk = value
        elif name == 'nfs':
            try:
                self.nfs = int(value)
            except ValueError:
                self.nfs = value
        elif name == 'origin':
            try:
                self.origin = int(value)
            except ValueError:
                self.origin = value
        else:
            setattr(self, name, value)
