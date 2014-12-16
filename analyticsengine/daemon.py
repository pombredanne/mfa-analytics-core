__author__ = 'sarink'

from analyticsengine.collector.tasks import run_request_fetch, run_request_parser, run_process_counters
import gevent


def run_collector():
    gevent.joinall([
        gevent.spawn(run_process_counters),
        gevent.spawn(run_request_parser),
        gevent.spawn(run_request_fetch)
    ])

if __name__ == '__main__':
    run_collector()