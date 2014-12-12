# -*- coding: utf-8 -*-

__author__ = 'Juniper Networks'

from analyticsengine.parser.handlers import XmlHandler
from analyticsengine.parser.mfc import MfcResponse
import xml.sax


class Parser(object):
    @staticmethod
    def parse_mfc_counters(raw_xml):
        mfc_resp_obj = MfcResponse()
        h = XmlHandler(mfc_resp_obj)
        xml.sax.parseString(raw_xml, h)
        return mfc_resp_obj

    @staticmethod
    def parse_mfc_config(raw_xml):
        pass
