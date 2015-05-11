__author__ = 'sarink'

import signal
import gevent
import gevent.subprocess
from gevent import monkey
monkey.patch_all()
from analyticsengine.collector.tasks import (redis_flush_keys, get_device_list, run_request_fetch, run_request_parser,
                                             run_process_counters, run_store_ingestion, run_scheduler)
from analyticsengine.logging import LOG
from analyticsengine.dbmanager.mfc import create_cluster_tables, create_daily_tables


def run_collector():
    LOG.info("Starting collector..")
    gevent.signal(signal.SIGQUIT, gevent.kill)
    redis_flush_keys()
    get_device_list()
    create_cluster_tables()
    create_daily_tables()
    try:
        tasks = [run_scheduler, run_store_ingestion, run_process_counters, run_request_parser, run_request_fetch]
        g_thread_pool = [gevent.spawn(task) for task in tasks]
        gevent.joinall(g_thread_pool)
    except KeyboardInterrupt:
        print "Keyboard interrupt.."
    finally:
        #pass
        gevent.killall(g_thread_pool, exception=gevent.GreenletExit)

if __name__ == '__main__':
    run_collector()