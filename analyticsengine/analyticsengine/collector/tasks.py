from analyticsengine.celeryapp.celery import celery
from redis import Redis
from pickle import loads, dumps
from http import HttpConnection
from analyticsengine.config import config
from analyticsengine.logging import LOG
from analyticsengine.parser import Parser
from analyticsengine.parser.mfc.common import Serialize
import json
import time

redis = Redis()

###
# sample task
###
@celery.task
def generate_prime(val):
    multiples = []
    result = []
    for i in xrange(2, val+1):
        if i not in multiples:
            result.append(i)
            for j in xrange(i*i, val+1, i):
                multiples.append(j)
    return result

@celery.task
def request_mfc(ip, data=None):
    if data is None:
        data = """<mfc-request><header><type>GET</type></header><data>stats mfc-cluster mfc</data></mfc-request>"""
    mfc_con = HttpConnection(ip)
    resp = mfc_con.send_request(data)
    redis.rpush(config.get('constants', 'REDIS_XML_QUEUE_KEY'), [ip, resp])
    return resp

@celery.task
def parse_counters(data=None):
    if data is None:
        data = redis.blpop(config.get('constants', 'REDIS_XML_QUEUE_KEY'))
    data = eval(data[1])
    p_obj = Parser.parse_mfc_counters(data[0], data[1])
    redis.rpush(config.get('constants', 'REDIS_PARSER_QUEUE_KEY'), Serialize.to_json(p_obj))
    return p_obj

@celery.task
def process_counters(data=None):
    if data is None:
        data = redis.blpop(config.get('constants', 'REDIS_PARSER_QUEUE_KEY'))

    redis.rpush(config.get('constants', 'REDIS_BUCKET_QUEUE_KEY'), data[1])
    return json.loads(data[1])

@celery.task
def run_request_fetch(ip_list=None):
    LOG.info("Starting request fetch task")
    if ip_list is None:
        with open(config.get('constants', 'CONF_BASE_PATH') + config.get('constants', 'IP_LIST_FILENAME'), 'r') as fp:
            ip_list = fp.readlines()

    tick = lambda x: time.time() - x
    t1 = time.time()
    initial = True

    while True:
        if tick(t1) >= int(config.get('collector', 'MFC_REQUEST_FREQUENCY')) or initial:
            for ip in ip_list:
                LOG.debug("Sending request to MFC - " + ip)
                request_mfc.delay(ip.strip())
            t1 = time.time()
            initial = False

@celery.task
def run_request_parser():
    LOG.info("Starting request parser task")
    while redis.llen(config.get('constants', 'REDIS_XML_QUEUE_KEY')) > 0:
        parse_counters.delay()

@celery.task
def run_process_counters():
    LOG.info("Starting process counter task")
    while redis.llen(config.get('constants', 'REDIS_PARSER_QUEUE_KEY')) > 0:
        process_counters.delay()
