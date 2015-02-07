from __future__ import absolute_import
import sys,os
sys.path.append(os.environ['MF_AE_PATH'])
from analyticsengine.config import celeryconfig
from celery import Celery

celery = Celery(include=[
    'analyticsengine.collector.tasks'
])

celery.config_from_object(celeryconfig)

if __name__ == '__main__':
    celery.start()
