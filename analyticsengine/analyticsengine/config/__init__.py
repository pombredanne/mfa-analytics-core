# -*- coding: utf-8 -*-

__author__ = 'Juniper Networks'

import os
import ConfigParser


base_install_path = os.environ['MF_AE_PATH']
conf_base_path = base_install_path + '/conf/'

config = ConfigParser.RawConfigParser()
config.read(conf_base_path + 'ae.conf')

config.set('logger', 'fh_log_level', 'logging.NOTSET')
config.set('logger', 'log_file_size', int(config.get('logger', 'file_size'))*1024*1024)

#-----
# adding constants to config
#-----
config.add_section('constants')
config.set('constants', 'BASE_INSTALL_PATH', base_install_path)
config.set('constants', 'CONF_BASE_PATH', conf_base_path)
config.set('constants', 'IP_LIST_FILENAME', 'ip_list')
config.set('constants', 'REDIS_XML_QUEUE_KEY', 'mfc_xml_queue')
config.set('constants', 'REDIS_PARSER_QUEUE_KEY', 'mfc_parser_queue')
config.set('constants', 'REDIS_BUCKET_QUEUE_KEY', 'mfc_bucket_queue')
