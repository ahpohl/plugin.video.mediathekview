# -*- coding: utf-8 -*-
"""
The PostgreSQL database support module

Copyright 2017-2019, Leo Moll and Dominik Schlösser
"""
# pylint: disable=too-many-lines,line-too-long

import time
import psycopg2

import resources.lib.mvutils as mvutils
import resources.lib.appContext as appContext
from resources.lib.storeQuery import StoreQuery


class StorePostgreSQL(StoreQuery):
    """
    The local PostgreSQL database class
    
    con.query('SET GLOBAL connect_timeout=28800')
    con.query('SET GLOBAL interactive_timeout=28800')
    con.query('SET GLOBAL wait_timeout=28800')
    SET SESSION MAX_EXECUTION_TIME=2000;
    SET GLOBAL MAX_EXECUTION_TIME=2000;
    """

    def __init__(self):
        super(StorePostgreSQL, self).__init__()
        self.logger = appContext.MVLOGGER.get_new_logger('StorePostgreSQL')
        self.notifier = appContext.MVNOTIFIER
        self.settings = appContext.MVSETTINGS
        self.conn = None

    def getConnection(self):
        if self.conn is None:
            self.logger.debug('Using PostgreSQL connector version {}',
                             psycopg2.__version__)
            # TODO Kodi 19 - we can update to mysql connector which supports auth_plugin parameter
            connectargs = {
                'host': self.settings.getDatabaseHost(),
                'port': self.settings.getDatabasePort(),
                'user': self.settings.getDatabaseUser(),
                'password': self.settings.getDatabasePassword(),
                'connect_timeout':24 * 60 * 60,
                'client_encoding':'UTF8'
            }
            self.conn = psycopg2.connect(**connectargs)
            try:
                cursor = self.conn.cursor()
                cursor.execute('SELECT VERSION()')
                (version,) = cursor.fetchone()
                self.logger.debug(
                    'Connected to server {} running {}', self.settings.getDatabaseHost(), version)
            # pylint: disable=broad-except
            except Exception:
                self.logger.debug('Connected to server {}', self.settings.getDatabaseHost())
            # select database
            try:
                self.conn.database = self.settings.getDatabaseSchema()
            except Exception:
                pass
            #
            cursor.close()
        return self.conn

    def execute(self, aStmt, aParams=None):
        aStmt = aStmt.replace('?', '%s')
        return super(StorePostgreSQL, self).execute(aStmt, aParams)

    def executeUpdate(self, aStmt, aParams=None):
        aStmt = aStmt.replace('?', '%s')
        return super(StorePostgreSQL, self).executeUpdate(aStmt, aParams)

    def executemany(self, aStmt, aParams=None):
        aStmt = aStmt.replace('?', '%s')
        return super(StorePostgreSQL, self).executemany(aStmt, aParams)

    def getImportPreparedStmtInsert(self):
        aStmt = super(StorePostgreSQL, self).getImportPreparedStmtInsert()
        aStmt = aStmt.replace('?', '%s')
        return aStmt

    def getImportPreparedStmtUpdate(self):
        aStmt = super(StorePostgreSQL, self).getImportPreparedStmtUpdate()
        aStmt = aStmt.replace('?', '%s')
        return aStmt

    def exit(self):
        if self.conn is not None:
            self.conn.close();
            self.conn = None
