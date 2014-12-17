# -*- coding: utf-8 -*-

__author__ = 'Juniper Networks'

from analyticsengine.parser.handlers import XmlHandler
from analyticsengine.parser.mfc import MfcResponse
import xml.sax


class Parser(object):
    @staticmethod
    def parse_mfc_counters(ip, raw_xml):
        mfc_resp_obj = MfcResponse()
        MfcResponse.ip = property(lambda self: None)
        h = XmlHandler(mfc_resp_obj)
        xml.sax.parseString(raw_xml, h)
        mfc_resp_obj.ip = ip
        return mfc_resp_obj

    @staticmethod
    def parse_mfc_config(raw_xml):
        pass
