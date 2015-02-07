__author__ = 'sarink'

from geventhttpclient import httplib
httplib.patch()

from urllib2 import Request, urlopen, URLError, HTTPError
from analyticsengine.logging import LOG


class MfcHttpConnection:
    def __init__(self, ip=None, port=None, uri=None):
        self.conn = None
        self.request = None
        if ip is None:
            ip = '127.0.0.1'
        self.ip = ip
        if port is None:
            port = '8080'
        self.port = port
        if uri is None:
            uri = '/admin/agentd_comm'
        self.uri = uri
        self.url = None
        self.headers = None
        self.data = None
        self.response = None

    def set_header(self, header, val):
        if self.headers is None:
            self.headers = {}
        self.headers[header] = val

    def build_req(self, uri=None, ip=None, port=None, data=None):
        if uri is not None:
            self.uri = uri
        if ip is not None:
            self.ip = ip
        if port is not None:
            self.port = port
        if data is not None:
            self.data = data
            self.set_header("Content-Type", "application/x-www-form-urlencoded")
            self.set_header("Content-Length", "%d" %len(data))
            self.set_header("Accept", "*/*")

        self.url = 'http://' + self.ip + ':' + self.port + self.uri

        self.request = Request(self.url, self.data, self.headers)

    def send_request(self, data=None):
        self.build_req(data=data)
        if self.request is not None:
            try:
                LOG.info("sending request to MFC agentd - " + self.ip)
                req_open = urlopen(self.request)
            except HTTPError, e:
                LOG.critical("Error code: %s Error Message: %s"%(e.code, e.msg))
                raise e
            except URLError, e:
                if hasattr(e, 'code'):
                    LOG.critical('ERROR code: ', e.code)
                elif hasattr(e, 'reason') :
                    LOG.critical('URL ERROR: ' + str(e.reason))
                else:
                    LOG.debug("No HTTP errors.." + str(e))
                raise e
            else:
                LOG.debug("reading MFC agentd response - " + self.ip)
                self.response = req_open.read()
                return self.response
