# -*- coding: utf-8 -*-

__author__ = 'Juniper Networks'

from analyticsengine.parser.handlers import XmlHandler
from analyticsengine.parser.mfc import MfcResponse, MfcConfigResponse
import xml.sax


class Parser(object):
    @staticmethod
    def parse_mfc_counters(device, sample_id, raw_xml):
        mfc_resp_obj = MfcResponse()
        MfcResponse.device_id = property(lambda self: None)
        MfcResponse.name = property(lambda self: None)
        MfcResponse.ip = property(lambda self: None)
        MfcResponse.sample_id = property(lambda self: None)
        h = XmlHandler(mfc_resp_obj)
        xml.sax.parseString(raw_xml, h)
        mfc_resp_obj.device_id = device[0]
        mfc_resp_obj.name = device[1]
        mfc_resp_obj.ip = device[2]
        mfc_resp_obj.sample_id = sample_id
        return mfc_resp_obj

    @staticmethod
    def parse_mfc_config(device, raw_xml):
        mfc_conf_resp_obj = MfcConfigResponse()
        MfcConfigResponse.name = property(lambda self: None)
        MfcConfigResponse.ip = property(lambda self: None)
        MfcConfigResponse.device_id = property(lambda self: None)
        h = XmlHandler(mfc_conf_resp_obj)
        xml.sax.parseString(raw_xml, h)
        mfc_conf_resp_obj.device_id = device[0]
        mfc_conf_resp_obj.name = device[1]
        mfc_conf_resp_obj.ip = device[2]
        return mfc_conf_resp_obj
