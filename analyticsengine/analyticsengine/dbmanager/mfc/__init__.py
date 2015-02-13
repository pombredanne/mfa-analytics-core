__author__ = 'sarink'

from cassandra import AlreadyExists
from analyticsengine.logging import LOG
from analyticsengine import dbmanager
from schema import (MFC_STATS_TABLE_NAME, MFC_SUMMARY_TABLE_NAME, CLUSTER_STATS_TABLE_NAME, CLUSTER_SUMMARY_TABLE_NAME,
                    CLUSTER_SAMPLE_MAP_TABLE_NAME)


def create_daily_tables():
    db_connection = dbmanager.connect_cassandra()
    daily_tables = dict()
    """This table will store counters from different MFCs per day.

    indexed with mfcid which is UUID as row key
    composite key(mfcid, type, name, ts)
    """
    daily_tables['mfc_stats'] = """
                        CREATE TABLE %s ( mfcid varchar, hostname varchar, ip varchar, type varchar, name varchar,
                        ts timestamp, value map<text, BigInt>, PRIMARY KEY (mfcid, type, name, ts))
                        """ % MFC_STATS_TABLE_NAME
    daily_tables['mfc_summary'] = """
                        CREATE TABLE %s ( mfcid varchar, hostname varchar, ip varchar, ts timestamp, sample_id varchar,
                        value map<text, text>, PRIMARY KEY (mfcid))
                        """ % MFC_SUMMARY_TABLE_NAME
    daily_tables['cluster_stats'] = """
                        CREATE TABLE %s ( name varchar, ts timestamp,
                        value map<text, BigInt>, sample_id varchar, PRIMARY KEY (name, ts))
                        """ % CLUSTER_STATS_TABLE_NAME
    daily_tables['cluster_summary'] = """
                        CREATE TABLE %s ( name varchar, ts timestamp,
                        value map<text, BigInt>, sample_id varchar, PRIMARY KEY (name))
                        """ % CLUSTER_SUMMARY_TABLE_NAME
    daily_tables['cluster_sample_map'] = """
                        CREATE TABLE %s ( sample_id varchar, ts timestamp,
                        ip_list list<text>, PRIMARY KEY (sample_id))
                        """ % CLUSTER_SAMPLE_MAP_TABLE_NAME

    for t_name, create_t in daily_tables.items():
        try:
            db_connection.execute(create_t)
        except AlreadyExists:
            LOG.info("Table already exist for %s" % t_name)

    db_connection.shutdown()