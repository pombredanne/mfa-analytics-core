__author__ = 'sarink'

from datetime import date
from cassandra import AlreadyExists
from analyticsengine.logging import LOG
from analyticsengine import dbmanager
from schema import (MFC_STATS_TABLE_NAME, MFC_SUMMARY_TABLE_NAME, CLUSTER_STATS_TABLE_NAME, CLUSTER_SUMMARY_TABLE_NAME,
                    CLUSTER_SAMPLE_MAP_TABLE_NAME, MFC_CONFIG_TABLE_NAME)


def create_daily_tables(date_str=None):
    db_connection = dbmanager.connect_cassandra()
    daily_tables = dict()

    if date_str is None:
        date_str = date.today().strftime('%m%d%Y')

    """This table will store counters from different MFCs per day.

    indexed with mfcid which is UUID as row key
    composite key(mfcid, type, name, ts)
    """
    daily_tables['mfc_stats'] = """
                        CREATE TABLE %s%s (mfcid varchar, hostname varchar, ip varchar, type varchar, name varchar,
                        ts timestamp, value map<text, BigInt>, PRIMARY KEY (mfcid, type, name, ts)) WITH CLUSTERING
                        ORDER BY(type ASC, name ASC, ts DESC)
                        """ % (MFC_STATS_TABLE_NAME, date_str)
    daily_tables['mfc_summary'] = """
                        CREATE TABLE %s%s (mfcid varchar, hostname varchar, ip varchar, ts timestamp, sample_id varchar,
                        value map<text, text>, PRIMARY KEY (mfcid))
                        """ % (MFC_SUMMARY_TABLE_NAME, date_str)
    daily_tables['cluster_stats'] = """
                        CREATE TABLE %s%s (name varchar, ts timestamp,
                        value map<text, BigInt>, sample_id varchar, PRIMARY KEY (name, ts)) WITH CLUSTERING
                        ORDER BY(ts DESC)
                        """ % (CLUSTER_STATS_TABLE_NAME, date_str)
    daily_tables['cluster_summary'] = """
                        CREATE TABLE %s%s (name varchar, ts timestamp,
                        value map<text, BigInt>, sample_id varchar, PRIMARY KEY (name))
                        """ % (CLUSTER_SUMMARY_TABLE_NAME, date_str)
    daily_tables['cluster_sample_map'] = """
                        CREATE TABLE %s%s (sample_id varchar, ts timestamp,
                        ip_list list<text>, PRIMARY KEY (sample_id))
                        """ % (CLUSTER_SAMPLE_MAP_TABLE_NAME, date_str)

    for t_name, create_t in daily_tables.items():
        try:
            LOG.info("Creating Table: %s" % t_name)
            db_connection.execute(create_t)
        except AlreadyExists:
            LOG.info("Table already exist for %s" % t_name)

    db_connection.shutdown()


def create_cluster_tables():
    db_connection = dbmanager.connect_cassandra()
    main_tables = dict()

    """These tables will store values for life time.

    Indexed with mfcid as row key
    """
    main_tables['mfc_config'] = """
                        CREATE TABLE %s (mfcid varchar, hostname varchar, ip varchar, type varchar, ts timestamp,
                        value map<text, text>, PRIMARY KEY (mfcid, ts, type))
                        """ % MFC_CONFIG_TABLE_NAME

    for t_name, create_t in main_tables.items():
        try:
            LOG.info("Creating Table: %s" % t_name)
            db_connection.execute(create_t)
        except AlreadyExists:
            LOG.info("Table already exist for %s" % t_name)

    db_connection.shutdown()