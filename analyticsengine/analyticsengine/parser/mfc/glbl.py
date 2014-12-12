__author__ = 'sarink'
from common import Usage, Requests, Storage


class Bytes(Storage):
    def __init__(self, *args, **kwargs):
        Storage.__init__(self, *args, **kwargs)


class GlblRequests(Requests):
    def __init__(self, *args, **kwargs):
        Requests.__init__(self, *args, **kwargs)


class RamCache(Usage):
    def __init__(self, *args, **kwargs):
        Usage.__init__(self, *args, **kwargs)


class DiskSpace(Usage):
    def __init__(self, *args, **kwargs):
        Usage.__init__(self, *args, **kwargs)


class Tier:
    def __init__(self, provider=None, read=None, write=None, evict=None):
        self.provider = provider
        self.read = read
        self.write = write
        self.evict = evict

    def startElement(self, name, attrs):
        return None

    def endElement(self, name, value):
        if name == 'read':
            try:
                self.read = int(value)
            except ValueError:
                self.read = value
        elif name == 'write':
            try:
                self.write = int(value)
            except ValueError:
                self.write = value
        elif name == 'evict':
            try:
                self.evict = int(value)
            except ValueError:
                self.evict = value
        elif name == 'provider':
            self.provider = value
        else:
            setattr(self, name, value)


class Glbl:
    def __init__(self, bytes=None, requests=None, ram_cache=None, tiers=None, disk_space=None):
        self.bytes = bytes
        self.requests = requests
        self.ram_cache = ram_cache
        self.tiers = []
        if tiers:
            self.tiers = tiers
        self.disk_space = disk_space

    def startElement(self, name, attrs):
        if name == 'bytes':
            self.bytes = Bytes()
            return self.bytes
        elif name == 'requests':
            self.requests = GlblRequests()
            return self.requests
        elif name == 'ram-cache':
            self.ram_cache = RamCache()
            return self.ram_cache
        elif name == 'tier':
            tier = Tier()
            self.tiers.append(tier)
            return tier
        elif name == 'diskspace':
            self.disk_space = DiskSpace()
            return self.disk_space
        else:
            return None

    def endElement(self, name, value):
        setattr(self, name, value)


