# -*- coding: utf-8 -*-

__author__ = 'Juniper Networks'

import os
import ConfigParser


base_install_path = os.environ['MF_AE_PATH']
conf_base_path = base_install_path + 'conf/'

config = ConfigParser.RawConfigParser()
config.read(conf_base_path + 'ae.conf')
config.add_section('constants')
config.set('constants', 'REDIS_XML_QUEUE_KEY', 'mfc_xml_queue')
config.set('constants', 'REDIS_PARSER_QUEUE_KEY', 'mfc_parser_queue')
config.set('constants', 'REDIS_BUCKET_QUEUE_KEY', 'mfc_bucket_queue')