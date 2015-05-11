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
# Task Queues
config.set('constants', 'BASE_INSTALL_PATH', base_install_path)
config.set('constants', 'CONF_BASE_PATH', conf_base_path)
config.set('constants', 'IP_LIST_FILENAME', 'ip_list')
config.set('constants', 'REDIS_XML_QUEUE_KEY', 'mfc_xml_result_q')
config.set('constants', 'REDIS_CONFIG_XML_QUEUE_KEY', 'mfc_conf_xml_q')
config.set('constants', 'REDIS_PARSER_QUEUE_KEY', 'mfc_parser_result_q')
config.set('constants', 'REDIS_MFC_BUCKET_QUEUE_KEY', 'mfc_bucket_result_q')
config.set('constants', 'REDIS_CLUSTER_BUCKET_QUEUE_KEY', 'cluster_bucket_result_q')
config.set('constants', 'REDIS_MFC_STORE_QUEUE_KEY', 'mfc_store_q')
config.set('constants', 'REDIS_CLUSTER_STORE_QUEUE_KEY', 'cluster_store_q')
config.set('constants', 'REDIS_CONFIG_STORE_QUEUE_KEY', 'mfc_conf_store_q')
# Global DS
config.set('constants', 'REDIS_MFC_UUID_HASH_KEY', 'mfc_uuid_hash')
config.set('constants', 'REDIS_DEV_LIST_KEY', 'dev_list')
config.set('constants', 'REDIS_SYNC_DEV_LIST_KEY', 'sync_dev_list')
config.set('constants', 'REDIS_UNSYNC_DEV_LIST_KEY', 'unsync_dev_list')
config.set('constants', 'REDIS_NEW_FOUND_DEV_LIST_KEY', 'new_found_dev_list')
config.set('constants', 'REDIS_NEW_SYNC_DEV_LIST_KEY', 'new_sync_dev_list')
config.set('constants', 'REDIS_MFC_CUR_THRPT_KEY', 'mfc_cur_thrpt')
# MySQL
config.set('constants', 'MYSQL_CONNECT_TIMEOUT', 60)
# DB Query
config.set('constants', 'MFC_DEV_LIST_QUERY', 'select id, name, ipAddr from mfa_dev_mf')

cluster_cum_timeout = 20 \
    if int(config.get('collector', 'MFC_REQUEST_FREQUENCY')) <= 30 else \
    int(config.get('collector', 'MFC_REQUEST_FREQUENCY')) - 10
config.set('constants', 'CLUSTER_SAMPLE_TIMEOUT', cluster_cum_timeout)