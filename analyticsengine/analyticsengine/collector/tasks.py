import sys
import redis
import redis.connection
from redis_collections import Dict, List
from collections import defaultdict, Counter
import json
import uuid
import re
import time
import signal
import gevent.pool
import gevent.socket
from celery import chain
import decimal

from analyticsengine.celeryapp.celery import celery
from analyticsengine.collector.http import MfcHttpConnection
from analyticsengine.config import config
from analyticsengine.logging import LOG
from analyticsengine.parser import Parser
from analyticsengine.parser.mfc.common import Serialize

from geventhttpclient import httplib
httplib.patch()
from geventhttpclient import HTTPClient, URL

#redis.connection.socket = gevent.socket
r = redis.Redis(host=config.get('redis', 'db_host'), port=config.get('redis', 'db_port'), db=config.get('redis', 'db'))
r_keys = {
    'dev_list': config.get('constants', 'REDIS_DEV_LIST_KEY'),
    'sync_dev_list': config.get('constants', 'REDIS_SYNC_DEV_LIST_KEY'),
    'mfc_uuid': config.get('constants', 'REDIS_MFC_UUID_HASH_KEY'),
    'cur_thrpt': config.get('constants', 'REDIS_MFC_CUR_THRPT_KEY'),
    'unsync_dev_list': config.get('constants', 'REDIS_UNSYNC_DEV_LIST_KEY'),
    'new_found_dev_list': config.get('constants', 'REDIS_NEW_FOUND_DEV_LIST_KEY')
}
mfa_dev_list = List(key=r_keys['dev_list'], redis=r)
sync_dev_list = List(key=r_keys['sync_dev_list'], redis=r)
mfc_uuid = Dict(key=r_keys['mfc_uuid'], redis=r)


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
    #Default response.
    resp = """<?xml version="1.0"?><mfc-response><header><status><code>504</code>
    <message>No Response</message></status></header></mfc-response>"""

    try:
        LOG.info("Sending config sync request to agentd device: %s %s %s " % device)
        agentd_resp = client.post(req_uri, body=req_body, headers=req_headers)
        resp = agentd_resp.read()
    except:
        LOG.error("Config sync request timed out for device: %s %s %s " % device)
    finally:
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
    #Default response.
    resp = """<?xml version="1.0"?><mfc-response><header><status><code>504</code>
    <message>No Response</message></status></header></mfc-response>"""

    try:
        LOG.info("Sending stat request to agentd %s %s %s " % device)
        agentd_resp = client.post(req_uri, body=req_body, headers=req_headers)
        resp = agentd_resp.read()
        resp = cleanup_pattern.sub("", resp)
    except:
        LOG.error("Stat request timedout for device: %s %s %s " % device)
    finally:
        r.rpush(q_key, [device, sample_id, resp])
    return

"""Request device config for the list of devices
 if the device list is from unsync_list, set the unsync_list flag to True. default is False.
 when unsync_list is set to True, parse_config_and_update will be set to update new_sync_dev_list instead of sync_list.
"""
@celery.task(queue='fetch', routing_key='fetch.config')
def request_cluster_config(dev_list, unsync_list=False):
    req_uri = '/admin/agentd_comm'
    conf_q = config.get('constants', 'REDIS_CONFIG_XML_QUEUE_KEY')
    mfc_count = len(dev_list)
    g_pool = gevent.pool.Pool(size=mfc_count)
    sync_flag = True
    if unsync_list:
        sync_flag = False

    LOG.debug("Creating Config request clients")
    conf_clients = []
    for device in dev_list:
        url = URL('http://' + device[2] + ':8080' + req_uri)
        conf_clients.append(HTTPClient.from_url(url, concurrency=1, headers_type=dict))

    LOG.debug("Starting to request Config from MFC")
    for i in xrange(mfc_count):
        g_pool.spawn(request_config_mfc_cb, conf_clients[i], dev_list[i], conf_q)
    g_pool.join()
    LOG.debug("Finished collecting Config from MFC")

    for i in xrange(mfc_count):
        conf_clients[i].close()

    """Parse and store the config.

    mfc_uuid is a global hashmap(redis Dict) with ip as key and UUID as value
    parse_config_and_sync will update the sync_dev_list, mfc_uuid for each XML response.
    """
    LOG.debug("Parsing config request output and building the UUID hash.")
    q_len = r.llen(conf_q)
    g_pool = gevent.pool.Pool(size=q_len)
    for _ in xrange(q_len):
        data = r.blpop(conf_q)
        g_pool.spawn(parse_config_and_sync, data, sync_flag)
    g_pool.join()

    """Return list of MFCs which was able to communicate."""
    sync_list = List(key=config.get('constants', 'REDIS_SYNC_DEV_LIST_KEY'), redis=r)
    return list(sync_list)

@celery.task(queue='fetch', routing_key='fetch.stats')
def request_cluster_stats(sync_mfcs, interval=20):
    req_uri = '/admin/agentd_comm'
    xml_q = config.get('constants', 'REDIS_XML_QUEUE_KEY')
    new_dev_list_key = config.get('constants', 'REDIS_NEW_FOUND_DEV_LIST_KEY')
    new_sync_dev_list_key = config.get('constants', 'REDIS_NEW_SYNC_DEV_LIST_KEY')
    sync_mfcs_key = config.get('constants', 'REDIS_SYNC_DEV_LIST_KEY')
    signal.signal(signal.SIGQUIT, gevent.kill)
    stat_clients = []

    """Request to synced MFCs

    will get the IP list from mfc_uuid
    """
    sync_mfcs_count = len(sync_mfcs)

    LOG.info("Synced MFCs: ")
    for device_id, name, ip in sync_mfcs:
        LOG.info("%s %s %s" % (device_id, name, ip))

    def create_stat_clients():
        LOG.info("Creating Stats request clients")
        for device_id, name, ip in sync_mfcs:
            url = URL('http://' + ip + ':8080' + req_uri)
            stat_clients.append(HTTPClient.from_url(url, concurrency=1, headers_type=dict))

    def close_stat_clients():
        for c in xrange(sync_mfcs_count):
            stat_clients[c].close()

    create_stat_clients()
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
        g_req_pool.join(timeout=interval)
        gevent.sleep(interval)

        if r.exists(new_sync_dev_list_key):
            LOG.info("New MFCs added to the Sync list- updating stat request clients")
            close_stat_clients()
            stat_clients = []
            LOG.info("Newly Synced MFCs: ")
            new_sync_mfcs = list(List(key=new_sync_dev_list_key, redis=r))
            for device_id, name, ip in new_sync_mfcs:
                LOG.info("%s %s %s" % (device_id, name, ip))
            r.delete(new_dev_list_key)
            #Get the current synced list and extend with newly synced list
            sync_mfcs = List(key=sync_mfcs_key, redis=r)
            sync_mfcs.extend(new_sync_mfcs)
            sync_mfcs = list(sync_mfcs)
            sync_mfcs_count = len(sync_mfcs)
            create_stat_clients()

    close_stat_clients()

"""Cluster Parse Tasks.
 parse XML config data and add the device to sync or unsync depending on the parsed obj.
 default sync list the sync_dev_list.
 in the case of recheck on the unsync devices, sync_list will be set to False. And upon successful response obj,
 the device will be added to new_sync_dev_list which will be checked inside the request_cluster_stats task.
"""

@celery.task
def parse_config_and_sync(data=None, sync_list=True):
    if sync_list:
        sync_list = List(key=config.get('constants', 'REDIS_SYNC_DEV_LIST_KEY'), redis=r)
    else:
        sync_list = List(key=config.get('constants', 'REDIS_NEW_SYNC_DEV_LIST_KEY'), redis=r)

    unsync_list = List(key=config.get('constants', 'REDIS_UNSYNC_DEV_LIST_KEY'), redis=r)
    if data is None:
        data = r.blpop(config.get('constants', 'REDIS_CONFIG_XML_QUEUE_KEY'))
    data = eval(data[1])
    device = data[0]
    xml = data[1]
    p_obj = Parser.parse_mfc_config(device, xml)
    if p_obj.header.status_code == 0:
        """ Update the gloabl DS

        extend the sync_dev_list with the device tuple
        Store the UUID in a global hashmap. will be retrieved using IP key.
        """
        try:
            mfc_uuid.update({device[2] + '_uuid': p_obj.data.config.host_id})
            mfc_uuid.update({device[2] + '_hostname': p_obj.data.config.hostname})
            #Update to the sync list if its able to retrieve the data attributes
            sync_list.extend((device,))
        except AttributeError:
            LOG.error("Something wrong with the Config data from MFC: " + device[2])
            LOG.error("Restart agentd or make sure the config data is valid.")
            unsync_list.extend((device,))
        finally:
            r.rpush(config.get('constants', 'REDIS_CONFIG_STORE_QUEUE_KEY'), Serialize.to_json(p_obj))
    else:
        LOG.error("Unable to get config from MFC: " + device[2])
        LOG.error("Status Code: %s Message: %s" % (p_obj.header.status_code, p_obj.header.status_msg))
        LOG.error("Check MFC state make sure Agentd is working fine.")
        unsync_list.extend((device,))

    return p_obj

@celery.task
def parse_counters(data=None):
    if data is None:
        data = r.blpop(config.get('constants', 'REDIS_XML_QUEUE_KEY'))
    LOG.debug(data)
    data = eval(data[1])
    p_obj = Parser.parse_mfc_counters(data[0], data[1], data[2])
    r.rpush(config.get('constants', 'REDIS_PARSER_QUEUE_KEY'), Serialize.to_json(p_obj))
    return p_obj


@celery.task
def parse_cluster_stats():
    xml_q = config.get('constants', 'REDIS_XML_QUEUE_KEY')
    while True:
        data = r.blpop(xml_q)
        parse_counters.apply_async(args=[data], queue='parse', routing_key='parse.stats')


"""Cluster Process Tasks."""

@celery.task
def process_mfc_counters(counters=None, data=None):
    decimal.getcontext().prec = 6

    if counters is None:
        data = r.blpop(config.get('constants', 'REDIS_PARSER_QUEUE_KEY'))
        counters = json.loads(data[1])

    if counters['data'] is None:
        LOG.critical("Device: %s, %s IP: %s" % (counters['device_id'], counters['name'], counters['ip']))
        LOG.critical("MFC response doesn't have any counter data. skipping sample: %s" % (counters['sample_id']))
    else:
        gl_bytes = Counter(counters['data']['glbl']['bytes'])
        # MFC CHR
        tot_bytes = sum(gl_bytes.values())
        tot_cache = counters['data']['glbl']['bytes']['ram'] + counters['data']['glbl']['bytes']['disk']
        # Handle Zero condition. Cumulative sum could be 0
        if tot_bytes == 0:
            counters['data']['chr'] = 0
        else:
            counters['data']['chr'] = float((decimal.Decimal(tot_cache) / decimal.Decimal(tot_bytes)) * 100)

        #Calculate current throughput
        mfcs_cur_thrpt = Dict(key=config.get('constants', 'REDIS_MFC_CUR_THRPT_KEY'), redis=r)
        try:
            counters['data']['cur_thrpt'] = gl_bytes - mfcs_cur_thrpt[counters['device_id']]
            counters['data']['cur_thrpt']['total'] = sum(counters['data']['cur_thrpt'].values())
            counters['data']['cur_thrpt']['cache'] = counters['data']['cur_thrpt']['ram'] + \
                                                     counters['data']['cur_thrpt']['disk']
            mfcs_cur_thrpt[counters['device_id']] = gl_bytes
        except KeyError:
            LOG.debug("current throughput hashmap - Initial update for " + str(counters['device_id']))
            counters['data']['cur_thrpt'] = mfcs_cur_thrpt[counters['device_id']] = gl_bytes
            counters['data']['cur_thrpt']['total'] = counters['data']['cur_thrpt']['cache'] = 0

        r.rpush(config.get('constants', 'REDIS_MFC_STORE_QUEUE_KEY'), json.dumps(counters))

    return counters


@celery.task
def process_cluster_counters(counters):
    r.rpush(config.get('constants', 'REDIS_CLUSTER_BUCKET_QUEUE_KEY'), counters)


@celery.task
def process_cluster_stats():

    def multi_dict_counter(level):
        if level < 1:
            return Counter()
        return defaultdict(lambda: multi_dict_counter(level-1))

    """ Creating a 2D dictionary to hold the cluster wide counter

    counters from across MFCs will be aggregated based on the sample ID.
    cluster[<Sample ID>][<Counter Name>] = Counter(<Dict of counter values>)

    cluster['cumulative'][<Counter Name>] will be used to keep track of the cumulative of last sample
    Delta will be calculated using above counter.
    """
    cluster = multi_dict_counter(2)  # 2 Level dictionary of Counter

    tick = lambda x: time.time() - x
    item_cnt = 0
    cur_sample = None
    #mfc_hash = Dict(key=config.get('constants', 'REDIS_MFC_UUID_HASH_KEY'), redis=r)
    sync_list = List(key=config.get('constants', 'REDIS_SYNC_DEV_LIST_KEY'), redis=r)
    cluster_sample_timeout = config.get('constants', 'CLUSTER_SAMPLE_TIMEOUT')
    store_q = config.get('constants', 'REDIS_CLUSTER_STORE_QUEUE_KEY')
    req_interval = int(config.get('collector', 'MFC_REQUEST_FREQUENCY'))
    sample_q = []

    while True:
        data = r.blpop(config.get('constants', 'REDIS_PARSER_QUEUE_KEY'))
        counters = json.loads(data[1])

        #Check if data exist for the parsed response. Agentd response can be empty
        if counters['data'] is not None:
            """Process each MFC counter."""
            process_mfc_counters.apply_async(args=[counters], queue='process', routing_key='process.stat')

            """Process Cluster wide cumulative data for same sample ID."""
            item_cnt += 1

            # Requests
            cluster[counters['sample_id']]['requests'].update(counters['data']['glbl']['requests'])

            #Cumulative Bytes
            cluster[counters['sample_id']]['bytes'].update(counters['data']['glbl']['bytes'])

            #Timestamp
            cluster[counters['sample_id']]['timestamp'] = counters['data']['timestamp']

            try:
                cluster[counters['sample_id']]['ip_list'].append(counters['ip'])  # Preserve the IP
            except AttributeError:
                cluster[counters['sample_id']]['ip_list'] = list()
                cluster[counters['sample_id']]['ip_list'].append(counters['ip'])

            if cur_sample is not None and cur_sample != counters['sample_id']:
                # new sample has arrived
                if item_cnt > len(sync_list) or tick(init_sample_ts) >= cluster_sample_timeout:
                    # 1st case: record from all the Sync'd MFCs received. Store and remove the sample from cluster DS.
                    # or 2nd case: some data still left to be received but hit sample time out.

                    #Calculate cumulative Delta.
                    cluster[cur_sample]['cur_thrpt'] = cluster[cur_sample]['bytes'] - cluster['cumulative']['bytes']
                    cluster[cur_sample]['cur_thrpt']['total'] = sum(cluster[cur_sample]['cur_thrpt'].values())
                    cluster[cur_sample]['cur_thrpt']['cache'] = cluster[cur_sample]['cur_thrpt']['ram'] + \
                                                                cluster[cur_sample]['cur_thrpt']['disk']

                    #Preserve the cumulative for next sample set
                    cluster['cumulative']['bytes'] = cluster[cur_sample]['bytes']

                    #Push to store the data
                    r.rpush(store_q, (cur_sample, dict(cluster[cur_sample])))

                    del cluster[cur_sample]
                    item_cnt = 1
                    cur_sample = sample_q.pop(0) if(len(sample_q) > 0) else counters['sample_id']
                    init_sample_ts = time.time()
                else:
                    LOG.info("Got new sample ID: %s. Need to wait for current sample(%s) to arrive until pushed out" %
                             (counters['sample_id'], cur_sample))
                    LOG.info("Adding sample ID to the waiting list.")
                    if counters['sample_id'] not in sample_q:
                        sample_q.append(counters['sample_id'])

            if cur_sample is None:
                cur_sample = counters['sample_id']
                init_sample_ts = time.time()
        else:
            LOG.critical("Device: %s, %s IP: %s" % (counters['device_id'], counters['name'], counters['ip']))
            LOG.critical("MFC response doesn't have any counter data. skipping sample: %s" % (counters['sample_id']))


"""Cluster Store Tasks."""


@celery.task
def store_mfc_stats():
    from datetime import date
    from analyticsengine import dbmanager
    from analyticsengine.dbmanager.mfc.schema import MFC_STATS_TABLE_NAME, MFC_SUMMARY_TABLE_NAME

    db_connection = dbmanager.connect_cassandra()
    date_strf = lambda dt: dt.strftime('%m%d%Y')

    req_interval = int(config.get('collector', 'MFC_REQUEST_FREQUENCY'))

    while True:
        data = r.blpop(config.get('constants', 'REDIS_MFC_STORE_QUEUE_KEY'))
        counters = json.loads(data[1])

        """ CF date suffix

        CF date suffix are calculated based on the timestamp in the response.
        data can be buffered in queue and may be of different date. Instead of calculating based on current date,
        its best to stick to the timestamp in the payload as it will then get stored in right CF.
        This will address the case when queue has buffered data of different date and can recover on a app crash.
        """
        date_str = date_strf(date.fromtimestamp(counters['data']['timestamp']))
        DAILY_TABLE_INSERT = "INSERT INTO " + MFC_STATS_TABLE_NAME + date_str + \
                             """ (mfcid, hostname, ip, ts, type, name, value)
                             VALUES (%(mfcid)s, %(hostname)s, %(ip)s, %(ts)s, %(type)s, %(name)s, %(value)s)
                             """
        DAILY_SUMMARY_INSERT = "INSERT INTO " + MFC_SUMMARY_TABLE_NAME + date_str + \
                               """ (mfcid, hostname, ip, sample_id, ts, value)
                               VALUES (%(mfcid)s, %(hostname)s, %(ip)s, %(sample_id)s, %(ts)s, %(value)s)
                               """

        pk = glbl_bytes = glbl_ds = glbl_ram = glbl_req = glbl_tier = http_ns = sys_stat = dict()

        """Global stats."""
        #Primary Key.
        pk['mfcid'] = str(counters['device_id'])
        pk['hostname'] = counters['name']
        pk['ip'] = counters['ip']
        pk['ts'] = counters['data']['timestamp'] * 1000

        #Global Bytes.
        glbl_bytes.update(pk)
        glbl_bytes['type'] = 'global'
        glbl_bytes['name'] = 'bytes'
        glbl_bytes['value'] = counters['data']['glbl']['bytes']

        #ingest_to_db.apply_async(args=[DailyCounters, session], kwargs=glbl_bytes, queue='store',
        #                         routing_key='store.stats')
        #DailyMFCCounters.create(**glbl_bytes)
        db_connection.execute(DAILY_TABLE_INSERT, glbl_bytes)

        #Global Disk Space.
        glbl_ds.update(pk)
        glbl_ds['type'] = 'global'
        glbl_ds['name'] = 'disk_space'
        #Disk space is in MB. Converting to Bytes
        for k, v in counters['data']['glbl']['disk_space'].items():
            counters['data']['glbl']['disk_space'][k] = v * 1024 * 1024
        glbl_ds['value'] = counters['data']['glbl']['disk_space']

        #ingest_to_db.apply_async(args=[DailyCounters, session], kwargs=glbl_ds, queue='store',
        #                         routing_key='store.stats')
        #DailyMFCCounters.create(**glbl_ds)
        db_connection.execute(DAILY_TABLE_INSERT, glbl_ds)

        #Global Ram Cache.
        glbl_ram.update(pk)
        glbl_ram['type'] = 'global'
        glbl_ram['name'] = 'ram_cache'
        glbl_ram['value'] = counters['data']['glbl']['ram_cache']

        #ingest_to_db.apply_async(args=[DailyCounters, session], kwargs=glbl_ram, queue='store',
        #                         routing_key='store.stats')
        #DailyMFCCounters.create(**glbl_ram)
        db_connection.execute(DAILY_TABLE_INSERT, glbl_ram)

        #Global Requests.
        glbl_req.update(pk)
        glbl_req['type'] = 'global'
        glbl_req['name'] = 'requests'
        glbl_req['value'] = counters['data']['glbl']['requests']

        #ingest_to_db.apply_async(args=[DailyCounters, session], kwargs=glbl_req, queue='store',
        #                         routing_key='store.stats')
        #DailyMFCCounters.create(**glbl_req)
        db_connection.execute(DAILY_TABLE_INSERT, glbl_req)

        #Global Tiers.
        glbl_tier.update(pk)
        glbl_tier['type'] = 'global'
        for tier in counters['data']['glbl']['tiers']:
            glbl_tier['name'] = tier['provider']
            tier.pop('provider')
            glbl_tier['value'] = tier
            db_connection.execute(DAILY_TABLE_INSERT, glbl_tier)

        """Namespace Stats."""
        http_ns.update(pk)
        http_ns['type'] = 'http_ns'
        for ns in counters['data']['services']['http']['namespaces']:
            http_ns['name'] = ns['name'] + ':requests'
            http_ns['value'] = ns['requests']
            db_connection.execute(DAILY_TABLE_INSERT, http_ns)

            http_ns['name'] = ns['name'] + ':bytes'
            http_ns['value'] = ns['bytes']
            db_connection.execute(DAILY_TABLE_INSERT, http_ns)

        """System Stats."""
        sys_stat.update(pk)
        sys_stat['type'] = 'system'

        sys_stat['name'] = 'cpu'
        sys_stat['value'] = counters['data']['system']['cpu']
        db_connection.execute(DAILY_TABLE_INSERT, sys_stat)

        sys_stat['name'] = 'memory'
        #System Memory is in KB. Converting to Bytes
        for k, v in counters['data']['system']['memory'].items():
            counters['data']['system']['memory'][k] = v * 1024
        sys_stat['value'] = counters['data']['system']['memory']
        db_connection.execute(DAILY_TABLE_INSERT, sys_stat)

        """MFC Summary Stats"""
        sum_stats = dict()
        sum_stats['mfcid'] = str(counters['device_id'])
        sum_stats['hostname'] = counters['name']
        sum_stats['ip'] = counters['ip']
        sum_stats['sample_id'] = counters['sample_id']
        sum_stats['ts'] = counters['data']['timestamp'] * 1000
        sum_stats['value'] = {}
        for k, v in counters['data']['glbl']['requests'].items():
            sum_stats['value'].update({'req_' + k: str(v)})
        for k, v in counters['data']['cur_thrpt'].items():
            sum_stats['value'].update({'cur_' + k: str(v/req_interval)})
        sum_stats['value'].update({'chr': str(counters['data']['chr'])})
        db_connection.execute(DAILY_SUMMARY_INSERT, sum_stats)

@celery.task
def store_cluster_stats():
    from datetime import date
    from analyticsengine.dbmanager.mfc.schema import (CLUSTER_STATS_TABLE_NAME, CLUSTER_SUMMARY_TABLE_NAME,
                                                      CLUSTER_SAMPLE_MAP_TABLE_NAME)
    from analyticsengine import dbmanager
    from collections import Counter

    db_connection = dbmanager.connect_cassandra()
    date_strf = lambda dt: dt.strftime('%m%d%Y')

    while True:
        data = r.blpop(config.get('constants', 'REDIS_CLUSTER_STORE_QUEUE_KEY'))
        sample_id, counters = eval(data[1])

        #CF date suffix calculation based on the current timestamp in the payload. Read above for more info.
        date_str = date_strf(date.fromtimestamp(counters['timestamp']))
        DAILY_TABLE_INSERT = "INSERT INTO " + CLUSTER_STATS_TABLE_NAME + date_str + \
                             """
                             (name, ts, sample_id, value) VALUES (%(name)s, %(ts)s, %(sample_id)s, %(value)s)
                             """
        DAILY_SUMMARY_TABLE_INSERT = "INSERT INTO " + CLUSTER_SUMMARY_TABLE_NAME + date_str + \
                                     """
                                     (name, ts, sample_id, value) VALUES (%(name)s, %(ts)s, %(sample_id)s, %(value)s)
                                     """

        SAMPLE_MAP_INSERT = "INSERT INTO " + CLUSTER_SAMPLE_MAP_TABLE_NAME + date_str + \
                            """
                            (sample_id, ts, ip_list) VALUES (%(sample_id)s, %(ts)s, %(ip_list)s)
                            """

        pk = glbl_req = glbl_bytes = sample_map = dict()
        pk['ts'] = counters['timestamp'] * 1000

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

        #Update clusterwide summary
        cluster_sum = dict()
        cluster_sum['name'] = 'cur_thrpt'
        cluster_sum['ts'] = pk['ts']
        cluster_sum['sample_id'] = sample_id
        cluster_sum['value'] = dict(counters['cur_thrpt'])
        db_connection.execute(DAILY_SUMMARY_TABLE_INSERT, cluster_sum)

        cluster_sum['name'] = 'cum_bytes'
        cluster_sum['value'] = dict(counters['bytes'])
        db_connection.execute(DAILY_SUMMARY_TABLE_INSERT, cluster_sum)

        cluster_sum['name'] = 'requests'
        cluster_sum['value'] = dict(counters['requests'])
        db_connection.execute(DAILY_SUMMARY_TABLE_INSERT, cluster_sum)

@celery.task
def store_mfc_config():
    from analyticsengine.dbmanager.mfc.schema import MFC_CONFIG_TABLE_NAME
    from analyticsengine import dbmanager
    from datetime import datetime

    db_connection = dbmanager.connect_cassandra()
    CONFIG_TABLE_INSERT = "INSERT INTO " + MFC_CONFIG_TABLE_NAME + """ (mfcid, hostname, ip, ts, type, value)
                        VALUES (%(mfcid)s, %(hostname)s, %(ip)s, %(ts)s, %(type)s, %(value)s)
                        """
    timestamp = lambda dt: long((dt - datetime.fromtimestamp(0)).total_seconds() * 1000)

    while True:
        data = r.blpop(config.get('constants', 'REDIS_CONFIG_STORE_QUEUE_KEY'))
        conf_data = json.loads(data[1])

        pk = conf = raw = dict()
        #Primary Key.
        pk['mfcid'] = str(conf_data['device_id'])
        pk['hostname'] = conf_data['name']
        pk['ip'] = conf_data['ip']
        pk['ts'] = timestamp(datetime.now())

        conf.update(pk)
        conf['type'] = 'config'
        conf['value'] = dict({
            'host_id': conf_data['data']['config']['host_id'],
            'version': conf_data['data']['config']['version'],
            'licenses': json.dumps(conf_data['data']['config']['licenses']),
            'network': json.dumps(conf_data['data']['config']['network']),
            'ifcfg': json.dumps(conf_data['data']['config']['ifcfg']),
            'namespaces': json.dumps(conf_data['data']['config']['namespaces']),
        })
        db_connection.execute(CONFIG_TABLE_INSERT, conf)

        raw.update(pk)
        raw['type'] = 'raw'
        raw['value'] = dict({
            'dump': json.dumps(conf_data['data']['config']['dump'])
        })
        db_connection.execute(CONFIG_TABLE_INSERT, raw)

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

"""Scheduler task."""
@celery.task
def schedule_events_task():
    import schedule
    from datetime import date, timedelta
    from analyticsengine.dbmanager.mfc import create_daily_tables
    unsync_dev_key = config.get('constants', 'REDIS_UNSYNC_DEV_LIST_KEY')
    unsync_flag = False


    """ Job to create daily DB tables
    calculate the next day's date and pass it to create all the tables for next day.
    """
    def create_daily_cf_job():
        tomorrow = date.today()+timedelta(days=1)
        tomorrow_strf = tomorrow.strftime('%m%d%Y')
        LOG.info("Creating tables for date: " + tomorrow_strf)
        create_daily_tables(tomorrow_strf)

    """ Schedule daily DB table creation.
    Will create DB tables for next day at 23:30 of every day
    """
    schedule.every().day.at("23:30").do(create_daily_cf_job)

    """ Job to recheck un-synced devices
    if unsync_dev_list exist, config request should be sent to see if the device can be moved to sync.
    devices are popped from unsync list as they are prepared for recheck. when device get send to check
    config(Sync check), they get added to unsync if its not able to sync.
    """
    def recheck_unsync_devices():
        unsync_list = List(key=config.get('constants', 'REDIS_UNSYNC_DEV_LIST_KEY'), redis=r)
        recheck_devices = []
        while len(unsync_list) > 0:
            recheck_devices.append(unsync_list.pop())

        LOG.info("Processing unsync device list")
        recheck_task = chain(request_cluster_config.s(recheck_devices), update_unsync_list.s())
        recheck_task.apply_async()

    while True:
        schedule.run_pending()
        gevent.sleep(1)

        if r.exists(unsync_dev_key):
            if not unsync_flag:
                LOG.info("Unsync device list found. will schedule job to recheck the status")
                schedule.every(int(config.get('collector', 'RECHECK_UNSYNC_FREQUENCY'))).minutes.do(recheck_unsync_devices)
                unsync_flag = True
            else:
                LOG.debug("Recheck Unsync devices is already scheduled and is in progress.")
        else:
            if unsync_flag:
                LOG.info("No Unsync devices found. Removing unsync devices rechecking from scheduler")
                schedule.cancel_job(recheck_unsync_devices)
                unsync_flag = False

@celery.task
def update_unsync_list(sync_list):
    unsync_list = List(key=config.get('constants', 'REDIS_UNSYNC_DEV_LIST_KEY'), redis=r)
    for device in unsync_list:
        if device in sync_list:
            unsync_list.remove(device)

"""Task Runner."""
def redis_flush_keys():
    for name, key in r_keys.items():
        if r.exists(key):
            LOG.info("Deleting existing redis key: %s" % key)
            r.delete(key)


def get_device_list(get_dev_from_file=False):
    from analyticsengine import dbmanager
    if get_dev_from_file:
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
            for device in rows:
                LOG.info("Found Device: %s %s %s" % device)
            rows = mysql_cur.fetchmany(500)
        mysql_db.close()
        #mfa_dev_list.extend([('2327015', 'Blaster-vJCE-005', '10.87.132.47'),])


def run_request_fetch():
    LOG.info("Starting request fetch task")

    req_interval = int(config.get('collector', 'MFC_REQUEST_FREQUENCY'))

    """Will split fetch job in to chain with two tasks.

    First request the config for all the MFCs.
    Prepare the list of MFCs that can be accessed. A hashmap of IP with UUID is created
    pass the SYNCd MFCs to fetch the stats.
    """
    if len(mfa_dev_list) > 0:
        collector_task = chain(request_cluster_config.s(list(mfa_dev_list)),
                               request_cluster_stats.s(interval=req_interval),
                               )
        collector_task.apply_async()
    else:
        LOG.error("Devices list not found. Check file or MFA DB")
        sys.exit(0)

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
    parse_cluster_task = parse_cluster_stats.apply_async(args=[], queue='tasks', routing_key='tasks')
    LOG.info("Parse task runner with task ID: " + parse_cluster_task.task_id)


def run_process_counters():
    LOG.info("Starting process counter task")
    process_cluster_task = process_cluster_stats.apply_async(args=[], queue='tasks', routing_key='tasks')
    LOG.info("Process task runner with task ID: " + process_cluster_task.task_id)


def run_store_ingestion():
    LOG.info("Starting Storage task")
    store_mfc_task = store_mfc_stats.apply_async(args=[], queue='store', routing_key='store.mfc_stats')
    store_cluster_task = store_cluster_stats.apply_async(args=[], queue='store', routing_key='store.cluster_stats')
    store_mfc_conf_task = store_mfc_config.apply_async(args=[], queue='store', routing_key='store.mfc_conf')
    LOG.info("Store MFC stats task runner with task ID: " + store_mfc_task.task_id)
    LOG.info("Store Cluster stats task runner with task ID: " + store_cluster_task.task_id)
    LOG.info("Store MFC Conf task runner with task ID: " + store_mfc_conf_task.task_id)


def run_scheduler():
    LOG.info("Starting Scheduler task")
    sched_task = schedule_events_task.apply_async(args=[], queue='tasks', routing_key='tasks')
    LOG.info("Scheduler task with task ID: " + sched_task.task_id)