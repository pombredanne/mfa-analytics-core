from analyticsengine.celeryapp.celery import celery
from redis import Redis
from pickle import loads, dumps
from http import HttpConnection
from analyticsengine.config import config
from analyticsengine.parser import MfcCounterParser, Parser
from analyticsengine.parser.mfc.common import Serialize
import json

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
    redis.rpush(config.get('constants', 'REDIS_XML_QUEUE_KEY'), resp)
    return resp

@celery.task
def parse_counters(data=None):
    if data is None:
        data = redis.blpop(config.get('constants', 'REDIS_XML_QUEUE_KEY'))
    p_obj = Parser.parse_mfc_counters(data[1])
    redis.rpush(config.get('constants', 'REDIS_PARSER_QUEUE_KEY'), Serialize.to_json(p_obj))
    return p_obj

@celery.task
def process_counters(data=None):
    if data is None:
        data = redis.blpop(config.get('constants', 'REDIS_PARSER_QUEUE_KEY'))

    redis.rpush(config.get('constants', 'REDIS_BUCKET_QUEUE_KEY'), data[1])
    return json.loads(data[1])
