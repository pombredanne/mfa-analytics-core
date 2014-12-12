from common import Usage


class CPU:
    def __init__(self, load1=None, load5=None, load15=None):
        self.load1 = load1
        self.load5 = load5
        self.load15 = load15

    def startElement(self, name, attrs):
        return None

    def endElement(self, name, value):
        if name == 'load1':
            try:
                self.load1 = int(value)
            except ValueError:
                self.load1 = value
        elif name == 'load5':
            try:
                self.load5 = int(value)
            except ValueError:
                self.load5 = value
        elif name == 'load15':
            try:
                self.load15 = int(value)
            except ValueError:
                self.load15 = value
        else:
            setattr(self, name, value)


class Memory(Usage):
    def __init__(self, *args, **kwargs):
        Usage.__init__(self, *args, **kwargs)


class System:
    def __init__(self, cpu=None, memory=None):
        self.cpu = cpu
        self.memory = memory

    def startElement(self, name, attrs):
        if name == 'system-cpu':
            self.cpu = CPU()
            return self.cpu
        elif name == 'system-memory':
            self.memory = Memory()
            return self.memory
        else:
            return None

    def endElement(self, name, value):
        setattr(self, name, value)
