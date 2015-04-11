# -*- coding: utf-8 -*-

__author__ = 'Juniper Networks'

from cassandra.cluster import Cluster, NoHostAvailable
from cassandra import InvalidRequest
from cassandra.protocol import SyntaxException
import sys

from analyticsengine.logging import LOG
from analyticsengine.config import config

""" MySQL Connection

This will connect to the MFA MySQL DB.
"""


def connect_mysql():
    import MySQLdb
    import _mysql_exceptions
    try:
        mysql_db = MySQLdb.connect(host=config.get('mysql', 'db_host'),
                                   port=int(config.get('mysql', 'db_port')),
                                   user=config.get('mysql', 'db_user'),
                                   passwd=config.get('mysql', 'db_password'),
                                   db=config.get('mysql', 'db_name'),
                                   connect_timeout=config.get('constants', 'MYSQL_CONNECT_TIMEOUT'),
                                   charset='utf8',
                                   use_unicode=True)
        return mysql_db
    except _mysql_exceptions.OperationalError as e:
        LOG.error(e)


""" Cassandra Connection

This will create connection to the cassandra cluster.
"""


def connect_cassandra():
    error = False
    cluster = Cluster([config.get('cassandra', 'db_host')], port=config.get('cassandra', 'db_port'),
                      protocol_version=3, idle_heartbeat_interval=120)
    try:
        LOG.info("Connecting to Cassandra..")
        return cluster.connect(config.get('cassandra', 'keyspace'))
    except NoHostAvailable:
        error = True
        LOG.info("ERROR: Check Cassandra connection settings in conf")
    except InvalidRequest:
        LOG.info("ERROR: Could not find existing Cassandra keyspace. will create new one")
        try:
            db_connection = cluster.connect()
            CREATE_KEYSPACE = """
                              CREATE KEYSPACE %s WITH replication = {'class': '%s', 'replication_factor': %s }
                              """ % (config.get('cassandra', 'keyspace'),
                                     config.get('cassandra', 'replication_strategy'),
                                     config.get('cassandra', 'replication_factor'))
            db_connection.execute(CREATE_KEYSPACE)
            db_connection.set_keyspace(config.get('cassandra', 'keyspace'))
            LOG.info("Created and session set to new keyspace:  %s" % config.get('cassandra', 'keyspace'))
            return db_connection
        except SyntaxException:
            error = True
            LOG.info("ERROR: couldn't create new keyspace. check keyspace settings in conf. Exiting now.")
            raise
    except:
        error = True
        LOG.info("ERROR: something wrong with Cassandra connection")
    finally:
        if error:
            LOG.info("Exiting..")
            sys.exit(0)

