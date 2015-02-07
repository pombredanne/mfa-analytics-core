# -*- coding: utf-8 -*-

__author__ = 'Juniper Networks'

import logging
import logging.handlers as handlers
import os
import errno
from analyticsengine.config import config

class AELog(logging.Logger):
    def __init__(self, name):
        self.aelog_level = None
        self.aelog_path = None
        self.aelog_file_name = None
        self.aelog_fh = None
        self.aelog_fh_level = None
        self.aelog_format = None
        self.aelog_file_size = None
        self.aelog_file_count = None
    
        logging.Logger.__init__(self, name, logging.DEBUG)

    def set_level(self, level=logging.DEBUG):
        self.aelog_level = level
        self.setLevel(self.aelog_level)

    def create_fh(self, file_path='/var/log/mfa', file_name='ae.log',
                  file_size=104857600, file_level=logging.NOTSET, file_count=10):
        self.aelog_file_name = file_name
        self.aelog_file_size = file_size
        self.aelog_fh_level = file_level
        self.aelog_file_count = file_count

        #check for trailing '/'
        if file_path[-1:] == '/':
            self.aelog_path = file_path
            file_path = file_path[:-1]
        else:
            self.aelog_path = file_path + '/'

        #---------------------------
        # Check if the directory exist
        # makedirs errors if it has trailing '/'
        # so using log_path
        #--------------------------
        if not os.path.exists(file_path):
            print 'file path not found. creating log directory: ' + file_path
            try:
                os.makedirs(file_path)
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise

        #AELOG rotating file handler
        self.aelog_fh = handlers.RotatingFileHandler(self.aelog_path + self.aelog_file_name,
                                                     maxBytes=self.aelog_file_size,
                                                     backupCount=self.aelog_file_count)
        self.aelog_fh.setLevel(self.aelog_fh_level)

    # Logging format
    def set_format(self, log_format):
        if log_format is None:
            log_format = '%(asctime)s - %(funcName)s - %(levelname)s - %(message)s'
        self.aelog_format = logging.Formatter(log_format)
        self.aelog_fh.setFormatter(self.aelog_format)

    def set_fh(self, fh=None):
        if fh is not None:
            self.aelog_fh = fh
        if self.aelog_fh is not None:
            self.addHandler(self.aelog_fh)

logging.setLoggerClass(AELog)
LOG = logging.getLogger('MF-AE-LOG')
LOG.setLevel(eval('logging.' + config.get('logger', 'level')))
LOG.create_fh(config.get('logger', 'file_path'), config.get('logger', 'file_name'),
              eval(config.get('logger', 'fh_log_level')))
LOG.set_format(config.get('logger', 'format'))
LOG.set_fh()