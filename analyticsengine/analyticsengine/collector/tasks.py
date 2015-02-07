import redis
import redis.connection
from redis_collections import Dict, List
import json
import uuid
import re
import time
import signal
import gevent.pool
import gevent.socket
from geventhttpclient import HTTPClient, URL
from celery import chain
import decimal

from analyticsengine.celeryapp.celery import celery
from analyticsengine.collector.http import MfcHttpConnection
from analyticsengine.config import config
from analyticsengine.logging import LOG
from analyticsengine.parser import Parser
from analyticsengine.parser.mfc.common import Serialize

redis.connection.socket = gevent.socket
r = redis.Redis()
keys = [config.get('constants', 'REDIS_DEV_LIST_KEY'),
        config.get('constants', 'REDIS_SYNC_DEV_LIST_KEY'),
        config.get('constants', 'REDIS_MFC_UUID_HASH_KEY')]
for key in keys:
    if r.exists(key):
        r.delete(key)
mfa_dev_list = List(key=keys[0], redis=r)
sync_dev_list = List(key=keys[1], redis=r)
mfc_uuid = Dict(key=keys[2], redis=r)


"""Cluster Request Tasks"""

@celery.task(rate_limit=2)
def request_config_mfc(ip, data=None):
    if data is None:
        data = """<mfc-request><header><type>GET</type></header>
        <data>running-config mfc-cluster mfc</data></mfc-request>"""
    mfc_con = MfcHttpConnection(ip)
    resp = mfc_con.send_request(data)
    redis.rpush(config.get('constants', 'REDIS_CONFIG_XML_QUEUE_KEY'), [ip, resp])
    return resp


@celery.task()
def request_stats_mfc(ip, data=None):
    cleanup_pattern = re.compile(r"\n\s*")
    if data is None:
        data = """<mfc-request><header><type>GET</type></header><data>stats mfc-cluster mfc</data></mfc-request>"""
    mfc_con = MfcHttpConnection(ip)
    resp = mfc_con.send_request(data)
    resp = cleanup_pattern.sub("", resp)
    if ip not in mfc_uuid.keys():
        mfc_uuid[ip] = str(uuid.uuid1())
    r.rpush(config.get('constants', 'REDIS_XML_QUEUE_KEY'), [mfc_uuid[ip + '_uuid'], ip, resp])
    return resp


def request_config_mfc_cb(client, device, q_key):
    req_uri = '/admin/agentd_comm'
    req_body = """<mfc-request><header><type>GET</type></header>
        <data>running-config mfc-cluster mfc</data></mfc-request>"""
    req_headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Content-Length": len(req_body),
        "Accept": "*/*",
        "connection": "Keep-Alive"
    }

    LOG.info("Sending request to agentd " + device[2])
    agentd_resp = client.post(req_uri, body=req_body, headers=req_headers)
    resp = agentd_resp.read()
    r.rpush(q_key, [device, resp])
    return


def request_stats_mfc_cb(client, device, sample_id, q_key):
    req_uri = '/admin/agentd_comm'
    req_body = """<mfc-request><header><type>GET</type></header>
        <data>stats mfc-cluster mfc</data></mfc-request>"""
    req_headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Content-Length": len(req_body),
        "Accept": "*/*",
        "connection": "Keep-Alive",
    }

    cleanup_pattern = re.compile(r"\n\s*")
    LOG.info("Sending request to agentd " + device[2])
    agentd_resp = client.post(req_uri, body=req_body, headers=req_headers)
    resp = agentd_resp.read()
    resp = cleanup_pattern.sub("", resp)
    r.rpush(q_key, [device, sample_id, resp])
    return


@celery.task(queue='fetch', routing_key='fetch.config')
def request_cluster_config(dev_list):
    req_uri = '/admin/agentd_comm'
    conf_q = config.get('constants', 'REDIS_CONFIG_XML_QUEUE_KEY')
    mfc_count = len(dev_list)
    g_pool = gevent.pool.Pool(size=mfc_count)

    LOG.debug("Creating Config request clients")
    conf_clients = []
    for device in dev_list:
        url = URL('http://' + device[2] + ':8080' + req_uri)
        conf_clients.append(HTTPClient.from_url(url, concurrency=1, headers_type=dict))

    LOG.info("Starting to request Config from MFC")
    for i in xrange(mfc_count):
        g_pool.spawn(request_config_mfc_cb, conf_clients[i], dev_list[i], conf_q)
    g_pool.join()
    LOG.info("Finished collecting Config from MFC")

    for i in xrange(mfc_count):
        conf_clients[i].close()

    """Parse and store the config.

    mfc_uuid is a global hashmap(redis Dict) with ip as key and UUID as value
    parse_config_mfc will update the sync_dev_list, mfc_uuid for each XML response.
    """
    LOG.info("Parsing and building the UUID hash. will push the config to db")
    q_len = r.llen(conf_q)
    g_pool = gevent.pool.Pool(size=q_len)
    for _ in xrange(q_len):
        data = r.blpop(conf_q)
        g_pool.spawn(parse_config_mfc, data)
    g_pool.join()

    """Return list of MFCs which was able to communicate."""
    #keys = mfc_uuid.keys()
    #return list(set(map(lambda val: val.strip("_hostname _uuid"), keys)))
    sync_list = List(key=config.get('constants', 'REDIS_SYNC_DEV_LIST_KEY'), redis=r)
    return list(sync_list)

@celery.task(queue='fetch', routing_key='fetch.stats')
def request_cluster_stats(sync_mfcs, interval=20):
    req_uri = '/admin/agentd_comm'
    xml_q = config.get('constants', 'REDIS_XML_QUEUE_KEY')
    signal.signal(signal.SIGQUIT, gevent.kill)

    """Request to synced MFCs

    will get the IP list from mfc_uuid
    """
    sync_mfcs_count = len(sync_mfcs)
    stat_clients = []

    LOG.info("Synced MFCs: ")
    for device_id, name, ip in sync_mfcs:
        LOG.info("%s %s %s" % (device_id, name, ip))

    LOG.debug("Creating Stats request clients")
    for device_id, name, ip in sync_mfcs:
        url = URL('http://' + ip + ':8080' + req_uri)
        stat_clients.append(HTTPClient.from_url(url, concurrency=1, headers_type=dict))

    g_req_pool = gevent.pool.Pool(size=sync_mfcs_count)
    LOG.info("Starting to request stats from MFC")
    while True:
        # Commented following : time based check hogs CPU cycles.
        '''
        if tick(t1) >= interval or initial:
            t1 = time.time()
            initial = False
        '''
        sample_id = str(uuid.uuid1())
        for i in xrange(sync_mfcs_count):
            g_req_pool.spawn(request_stats_mfc_cb, stat_clients[i], sync_mfcs[i], sample_id, xml_q)
        g_req_pool.join()
        gevent.sleep(interval)

    for i in xrange(sync_mfcs_count):
        stat_clients[i].close()

"""Cluster Parse Tasks."""

@celery.task
def parse_config_mfc(data=None):
    sync_list = List(key=config.get('constants', 'REDIS_SYNC_DEV_LIST_KEY'), redis=r)
    if data is None:
        data = r.blpop(config.get('constants', 'REDIS_CONFIG_XML_QUEUE_KEY'))
    data = eval(data[1])
    device = data[0]
    xml = data[1]
    p_obj = Parser.parse_mfc_config(device, xml)

    """ Update the gloabl DS

    extend the sync_dev_list with the device tuple
    Store the UUID in a global hashmap. will be retrieved using IP key.
    """
    try:
        sync_list.extend((device,))
        mfc_uuid.update({device[2] + '_uuid': p_obj.data.config.host_id})
        mfc_uuid.update({device[2] + '_hostname': p_obj.data.config.hostname})
    except AttributeError:
        LOG.error("Unable to get config from MFC: " + device[2])
        LOG.error("Status Code: %s Message: %s" % (p_obj.header.status_code, p_obj.header.status_msg))
        LOG.error("Check MFC state make sure Agentd is working fine.")
    finally:
        r.rpush(config.get('constants', 'REDIS_CONFIG_STORE_QUEUE_KEY'), Serialize.to_json(p_obj))
        return p_obj


@celery.task
def parse_counters(data=None):
    if data is None:
        data = r.blpop(config.get('constants', 'REDIS_XML_QUEUE_KEY'))
    data = eval(data[1])
    p_obj = Parser.parse_mfc_counters(data[0], data[1], data[2])
    print p_obj
    r.rpush(config.get('constants', 'REDIS_PARSER_QUEUE_KEY'), Serialize.to_json(p_obj))
    return p_obj


@celery.task
def parse_cluster_stats():
    while True:
        data = r.blpop('mfc_xml_result_q')  #config.get('constants', 'REDIS_XML_QUEUE_KEY'))
        parse_counters.apply_async(args=[data], queue='parse', routing_key='parse.stats')


"""Cluster Process Tasks."""

@celery.task
def process_mfc_counters(counters=None, data=None):
    decimal.getcontext().prec = 6

    if counters is None:
        data = r.blpop(config.get('constants', 'REDIS_PARSER_QUEUE_KEY'))
        counters = json.loads(data[1])

    # MFC CHR
    tot_bytes = counters['data']['glbl']['bytes']['ram'] + \
                counters['data']['glbl']['bytes']['disk'] + \
                counters['data']['glbl']['bytes']['nfs'] + \
                counters['data']['glbl']['bytes']['origin']
    tot_cache = tot_bytes - counters['data']['glbl']['bytes']['origin']
    counters['data']['chr'] = float((decimal.Decimal(tot_cache) / decimal.Decimal(tot_bytes)) * 100)

    r.rpush(config.get('constants', 'REDIS_MFC_STORE_QUEUE_KEY'), json.dumps(counters))

    return counters


@celery.task
def process_cluster_counters(counters):
    r.rpush(config.get('constants', 'REDIS_CLUSTER_BUCKET_QUEUE_KEY'), counters)


@celery.task
def process_cluster_stats():
    from collections import defaultdict, Counter

    def multi_dict_counter(level):
        if level < 1:
            return Counter()
        return defaultdict(lambda: multi_dict_counter(level-1))

    """ Creating a 2D dictionary to hold the cluster wide counter

    counters from across MFCs will be aggregated based on the sample ID.
    cluster[<Sample ID>][<Counter Name>] = Counter(<Dict of counter values>)
    """
    cluster = multi_dict_counter(2)  # 2 Level dictionary of Counter

    tick = lambda x: time.time() - x
    item_cnt = 0
    cur_sample = None
    mfc_hash = Dict(key=config.get('constants', 'REDIS_MFC_UUID_HASH_KEY'), redis=r)
    cluster_sample_timeout = config.get('constants', 'CLUSTER_SAMPLE_TIMEOUT')
    store_q = config.get('constants', 'REDIS_CLUSTER_STORE_QUEUE_KEY')

    while True:
        data = r.blpop(config.get('constants', 'REDIS_PARSER_QUEUE_KEY'))
        counters = json.loads(data[1])

        """Process each MFC counter."""
        process_mfc_counters.apply_async(args=[counters], queue='process', routing_key='process.stat')

        """Process Cluster wide cumulative data for same sample ID."""
        item_cnt += 1

        # Requests
        cluster[counters['sample_id']]['requests'].update(counters['data']['glbl']['requests'])

        # Bytes
        cluster[counters['sample_id']]['bytes'].update(counters['data']['glbl']['bytes'])

        try:
            cluster[counters['sample_id']]['ip_list'].append(counters['ip'])  # Preserve the IP
        except AttributeError:
            cluster[counters['sample_id']]['ip_list'] = list()
            cluster[counters['sample_id']]['ip_list'].append(counters['ip'])

        if cur_sample is not None and cur_sample != counters['sample_id']:
            # new sample has arrived
            if item_cnt > len(mfc_hash.keys()) or tick(init_sample_ts) >= cluster_sample_timeout:
                # 1st case: record from all the Sync'd MFCs received. Store and remove the sample from cluster DS.
                # or 2nd case: some data still left to be received but hit sample time out.
                r.rpush(store_q, (cur_sample, dict(cluster[cur_sample])))

                del cluster[cur_sample]
                item_cnt = 1
                cur_sample = counters['sample_id']
                init_sample_ts = time.time()
            else:
                LOG.info("New sample as started and waiting for old sample to arrive until pushed out")

        if cur_sample is None:
            cur_sample = counters['sample_id']
            init_sample_ts = time.time()

"""Cluster Store Tasks."""


@celery.task
def store_mfc_stats():
    from analyticsengine import dbmanager
    from analyticsengine.dbmanager.mfc.schema import MFC_STATS_TABLE_NAME, MFC_SUMMARY_TABLE_NAME

    db_connection = dbmanager.connect_cassandra()

    DAILY_TABLE_INSERT = "INSERT INTO " + MFC_STATS_TABLE_NAME + """ (mfcid, hostname, ip, ts, type, name, value)
                        VALUES (%(mfcid)s, %(hostname)s, %(ip)s, %(ts)s, %(type)s, %(name)s, %(value)s)
                        """
    DAILY_TABLE_SUMMARY_INSERT = "INSERT INTO " + MFC_SUMMARY_TABLE_NAME + """ (hostname, ip, sample_id, ts, value)
                        VALUES (%(hostname)s, %(ip)s, %(sample_id)s, %(ts)s, %(value)s)
                        """

    while True:
        data = r.blpop(config.get('constants', 'REDIS_MFC_STORE_QUEUE_KEY'))
        counters = json.loads(data[1])

        pk = glbl_bytes = glbl_ds = glbl_ram = glbl_req = {}

        """Primary Key."""
        pk['mfcid'] = counters['device_id']
        pk['hostname'] = counters['name']
        pk['ip'] = counters['ip']
        pk['ts'] = counters['data']['timestamp'] * 1000

        """Global Bytes."""
        glbl_bytes.update(pk)
        glbl_bytes['type'] = 'global'
        glbl_bytes['name'] = 'bytes'
        glbl_bytes['value'] = counters['data']['glbl']['bytes']

        #ingest_to_db.apply_async(args=[DailyCounters, session], kwargs=glbl_bytes, queue='store',
        #                         routing_key='store.stats')
        #DailyMFCCounters.create(**glbl_bytes)
        db_connection.execute(DAILY_TABLE_INSERT, glbl_bytes)

        """Global Disk Space."""
        glbl_ds.update(pk)
        glbl_ds['type'] = 'global'
        glbl_ds['name'] = 'disk_space'
        glbl_ds['value'] = counters['data']['glbl']['disk_space']

        #ingest_to_db.apply_async(args=[DailyCounters, session], kwargs=glbl_ds, queue='store',
        #                         routing_key='store.stats')
        #DailyMFCCounters.create(**glbl_ds)
        db_connection.execute(DAILY_TABLE_INSERT, glbl_ds)

        """Global Ram Cache."""
        glbl_ram.update(pk)
        glbl_ram['type'] = 'global'
        glbl_ram['name'] = 'ram_cache'
        glbl_ram['value'] = counters['data']['glbl']['ram_cache']

        #ingest_to_db.apply_async(args=[DailyCounters, session], kwargs=glbl_ram, queue='store',
        #                         routing_key='store.stats')
        #DailyMFCCounters.create(**glbl_ram)
        db_connection.execute(DAILY_TABLE_INSERT, glbl_ram)

        """Global Requests."""
        glbl_req.update(pk)
        glbl_req['type'] = 'global'
        glbl_req['name'] = 'requests'
        glbl_req['value'] = counters['data']['glbl']['requests']

        #ingest_to_db.apply_async(args=[DailyCounters, session], kwargs=glbl_req, queue='store',
        #                         routing_key='store.stats')
        #DailyMFCCounters.create(**glbl_req)
        db_connection.execute(DAILY_TABLE_INSERT, glbl_req)

        """MFC Summary Stats"""
        sum_stats = dict()
        sum_stats['hostname'] = counters['name']
        sum_stats['ip'] = counters['ip']
        sum_stats['sample_id'] = counters['sample_id']
        sum_stats['ts'] = counters['data']['timestamp'] * 1000
        sum_stats['value'] = {}
        for k, v in counters['data']['glbl']['requests'].items():
            sum_stats['value'].update({'req_' + k: str(v)})
        sum_stats['value'].update({'chr': str(counters['data']['chr'])})
        db_connection.execute(DAILY_TABLE_SUMMARY_INSERT, sum_stats)

@celery.task
def store_cluster_stats():
    from analyticsengine.dbmanager.mfc.schema import CLUSTER_SUMMARY_TABLE_NAME, CLUSTER_SAMPLE_MAP_TABLE_NAME
    from analyticsengine import dbmanager
    from collections import Counter

    db_connection = dbmanager.connect_cassandra()
    DAILY_TABLE_INSERT = "INSERT INTO " + CLUSTER_SUMMARY_TABLE_NAME + """ (name, ts, sample_id, value)
                        VALUES (%(name)s, %(ts)s, %(sample_id)s, %(value)s)
                        """
    SAMPLE_MAP_INSERT = "INSERT INTO " + CLUSTER_SAMPLE_MAP_TABLE_NAME + """ (sample_id, ts, ip_list)
                        VALUES (%(sample_id)s, %(ts)s, %(ip_list)s)
                        """
    while True:
        data = r.blpop(config.get('constants', 'REDIS_CLUSTER_STORE_QUEUE_KEY'))
        sample_id, counters = eval(data[1])

        pk = glbl_req = glbl_bytes = sample_map = dict()
        pk['ts'] = int(time.time())

        glbl_req.update(pk)
        glbl_req['name'] = 'gl_requests'
        glbl_req['value'] = dict(counters['requests'])
        glbl_req['sample_id'] = sample_id
        #DailyClusterCounters.create(**glbl_req)
        db_connection.execute(DAILY_TABLE_INSERT, glbl_req)

        glbl_bytes.update(pk)
        glbl_bytes['name'] = 'gl_bytes'
        glbl_bytes['value'] = dict(counters['bytes'])
        glbl_bytes['sample_id'] = sample_id
        #DailyClusterCounters.create(**glbl_bytes)
        db_connection.execute(DAILY_TABLE_INSERT, glbl_bytes)

        sample_map.update(pk)
        sample_map['sample_id'] = sample_id
        sample_map['ip_list'] = counters['ip_list']
        db_connection.execute(SAMPLE_MAP_INSERT, sample_map)

@celery.task
def ingest_to_db(model, session, **kwargs):
    if not session:
        from cqlengine import connection
        LOG.info("No c* session found.. creating one.")
        connection.setup([config.get('cassandra', 'db_host')], config.get('cassandra', 'keyspace'))
    model.create(**kwargs)


def terminate_task(task):
    LOG.info("Exiting the task: ")
    task.revoke(terminate=True)


"""Task Runner."""


def run_request_fetch(dev_list=None, get_dev_from_file=False):
    from analyticsengine import dbmanager

    LOG.info("Starting request fetch task")
    if dev_list is None and get_dev_from_file:
        with open(config.get('constants', 'CONF_BASE_PATH') + config.get('constants', 'IP_LIST_FILENAME'), 'r') as fp:
            dev_list = fp.readlines()
            mfa_dev_list.extend(dev_list)
    else:
        LOG.info("Querying MySQL to get the list of devices..")
        mysql_db = dbmanager.connect_mysql()
        mysql_cur = mysql_db.cursor()
        mysql_cur.execute(config.get('constants', 'MFC_DEV_LIST_QUERY'))
        rows = mysql_cur.fetchmany(500)
        while len(rows) > 0:
            mfa_dev_list.extend(rows)
            rows = mysql_cur.fetchmany(500)
        mysql_db.close()

    req_interval = int(config.get('collector', 'MFC_REQUEST_FREQUENCY'))

    """Will split fetch job in to chain with two tasks.

    First request the config for all the MFCs.
    Prepare the list of MFCs that can be accessed. A hashmap of IP with UUID is created
    pass the SYNCd MFCs to fetch the stats.
    """
    collector_task = chain(request_cluster_config.s(list(mfa_dev_list)),
                           request_cluster_stats.s(interval=req_interval),
                           )
    collector_task.apply_async()

    '''
    mfc_cnt = len(ip_list)

    req_cluster_task1 = request_cluster.apply_async(args=[ip_list[:mfc_cnt/2],
                                                          int(config.get('collector', 'MFC_REQUEST_FREQUENCY'))],
                                                    queue='fetch', routing_key='fetch.stats')
    LOG.info("Fetch task1 with task ID: " + req_cluster_task1.task_id)

    req_cluster_task2 = request_cluster.apply_async(args=[ip_list[mfc_cnt/2:],
                                                          int(config.get('collector', 'MFC_REQUEST_FREQUENCY'))],
                                                    queue='fetch', routing_key='fetch.stats')
    LOG.info("Fetch task2 with task ID: " + req_cluster_task2.task_id)
    '''


def run_request_parser():
    LOG.info("Starting request parser task")
    parse_cluster_task = parse_cluster_stats.apply_async(args=[])
    LOG.info("Parse task runner with task ID: " + parse_cluster_task.task_id)


def run_process_counters():
    LOG.info("Starting process counter task")
    process_cluster_task = process_cluster_stats.apply_async(args=[])
    LOG.info("Process task runner with task ID: " + process_cluster_task.task_id)


def run_store_ingestion():
    LOG.info("Starting Storage task")
    store_mfc_task = store_mfc_stats.apply_async(args=[], queue='store', routing_key='store.mfc_stats')
    store_cluster_task = store_cluster_stats.apply_async(args=[], queue='store', routing_key='store.cluster_stats')
    LOG.info("Store MFC stats task runner with task ID: " + store_mfc_task.task_id)
    LOG.info("Store MFC stats task runner with task ID: " + store_cluster_task.task_id)
