__author__ = 'sarink'

import gevent
from gevent import monkey
monkey.patch_all()
from analyticsengine.collector.tasks import run_request_fetch, run_request_parser, run_process_counters
from analyticsengine.logging import LOG


def run_collector():
    LOG.info("Starting collector..")
    gevent.joinall([
        gevent.spawn(run_process_counters),
        gevent.spawn(run_request_parser),
        gevent.spawn(run_request_fetch)
    ])

if __name__ == '__main__':
    run_collector()