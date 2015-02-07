__author__ = 'sarink'

import re
from system import System
from glbl import Glbl
from services import Services
from analyticsengine.parser.mfc.common import Serialize
from analyticsengine.logging import LOG


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
            setattr(self, name, value)
        else:
            return


class Config:
    def __init__(self, hostname=None, host_id=None, version=None, licenses=None, ifcfg=None,
                 network=None, pm=None, namespaces=None, cache=None):
        self.hostname = hostname
        self.host_id = host_id
        self.version = version
        self.licenses = []
        if licenses:
            self.licenses = licenses
        self.ifcfg = ifcfg
        self.network = network
        self.pm = pm
        self.namespaces = {}
        if namespaces:
            self.namespaces = namespaces
        self.cache = cache
        self.dump = None


class ConfigData:
    def __init__(self, config=None):
        self.config = Config()
        self.config.dump = config

    def startElement(self, name, attrs):
        return None

    def endElement(self, name, value):
        if name == 'config':
            self.config.dump = value
            self.start_parsing()
        else:
            return

    def start_parsing(self):
        hostid_pattern = re.compile(r"Host ID: ([\w\d-]+)")
        self.config.host_id = hostid_pattern.search(self.config.dump).group(1)
        LOG.debug("Host ID: " + self.config.host_id)

        hostname_pattern = re.compile(r"Hostname: ([\w\d-]+)")
        hostname = hostname_pattern.search(self.config.dump)
        if hostname:
            hostname = hostname.group(1)
            self.config.hostname = hostname
            LOG.debug("Hostname: " + hostname)

        version_pattern = re.compile(r"Version: ([-\w\d .]+)")
        self.config.version = version_pattern.search(self.config.dump).group(1)
        LOG.debug("Version: " + self.config.version)

        lic_pattern = re.compile(r"license install ([-\w\d \".]+)", re.M)
        lic_match = lic_pattern.findall(self.config.dump)
        for lic in lic_match:
            self.config.licenses.append(lic)
            LOG.debug("License: " + lic)

        if_pattern = re.compile(r"[no ]* interface ([\w\d]+) ([-\w\d \".]+)", re.M)
        if_match = if_pattern.findall(self.config.dump)
        self.config.ifcfg = if_match
        LOG.debug("Interface Config: ")
        LOG.debug(self.config.ifcfg)

        nw_pattern = re.compile(r"[no ]* network ([\w]*) ([-\w\d \".]*)", re.M)
        nw_match = nw_pattern.findall(self.config.dump)
        self.config.network = nw_match
        LOG.debug("Network: ")
        LOG.debug(self.config.network)

        ns_pattern = re.compile(r"namespace ([\w]+) ([-\w\d \"./]+)", re.M)
        ns_match = ns_pattern.findall(self.config.dump)
        for (ns, conf) in ns_match:
            if ns not in self.config.namespaces.keys():
                self.config.namespaces[ns] = []
                LOG.debug("Namespace: " + ns)
            else:
                self.config.namespaces[ns].append(conf)


class MfcConfigResponse(MfcResponse):
    def __init__(self, *args, **kwargs):
        MfcResponse.__init__(self, *args, **kwargs)

    def __repr__(self):
        return "MfcResponse: MFC Response Config Object"

    def startElement(self, name, attrs):
        if name == 'header':
            self.header = Header()
            return self.header
        elif name == 'data':
            self.data = ConfigData()
            return self.data
        else:
            return None
