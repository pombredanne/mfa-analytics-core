# -*- coding: utf-8 -*-

__author__ = 'Juniper Networks'

import os
import ConfigParser


base_install_path = os.environ['MF_AE_PATH']
conf_base_path = base_install_path + 'conf/'

config = ConfigParser.RawConfigParser()
config.read(conf_base_path + 'ae.conf')
