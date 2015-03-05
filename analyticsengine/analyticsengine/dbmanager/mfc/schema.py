__author__ = 'sarink'

from datetime import date
from cqlengine import columns
from cqlengine.models import Model

from analyticsengine.config import config

today = date.today()

class MfcSummary(Model):
    __keyspace__ = config.get('cassandra', 'keyspace')
    __table_name__ = "test"
    mfcid = columns.Text()
    ip = columns.Text()
    type = columns.Text()
    name = columns.Text()
    ts = columns.DateTime(primary_key=True)
    value = columns.Map(columns.Text, columns.BigInt)


class DailyMFCCounters(Model):
    __keyspace__ = config.get('cassandra', 'keyspace')
    __table_name__ = 'counters_' + today.strftime('%m%d%Y')
    mfcid = columns.Text(primary_key=True, partition_key=True)
    ip = columns.Text()
    type = columns.Text(primary_key=True)
    name = columns.Text(primary_key=True)
    ts = columns.DateTime(primary_key=True)
    value = columns.Map(columns.Text, columns.BigInt)


class DailyClusterCounters(Model):
    __keyspace__ = config.get('cassandra', 'keyspace')
    __table_name__ = 'cluster_' + today.strftime('%m%d%Y')
    name = columns.Text(primary_key=True, partition_key=True)
    ts = columns.DateTime(primary_key=True)
    value = columns.Map(columns.Text, columns.BigInt)
    sample_id = columns.Text()

MFC_STATS_TABLE_NAME = 'mfc_stats_'
MFC_SUMMARY_TABLE_NAME = 'mfc_summary_'
CLUSTER_STATS_TABLE_NAME = 'cluster_stats_'
CLUSTER_SUMMARY_TABLE_NAME = 'cluster_summary_'
CLUSTER_SAMPLE_MAP_TABLE_NAME = 'cluster_sample_map_'
MFC_CONFIG_TABLE_NAME = 'mfc_config'

DAILY_MFC_TABLE_INSERT = "INSERT INTO counters_" + today.strftime('%m%d%Y') + """
(mfcid, ip, ts, type, name, value)
VALUES (%(mfcid)s, %(ip)s, %(ts)s, %(type)s, %(name)s, %(value)s)
"""
