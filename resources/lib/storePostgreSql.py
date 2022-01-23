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
    """

    def __init__(self):
        super(StorePostgreSQL, self).__init__()
        self.logger = appContext.MVLOGGER.get_new_logger('StorePostgreSQL')
        self.notifier = appContext.MVNOTIFIER
        self.settings = appContext.MVSETTINGS
        self.conn = None

    def getConnection(self):
        if self.conn is None:
            self.logger.debug('Using PostgreSQL connector version {}', psycopg2.__version__)
            connectargs = {
                'dbname': self.settings.getDatabaseSchema(),
                'host': self.settings.getDatabaseHost(),
                'port': self.settings.getDatabasePort(),
                'user': self.settings.getDatabaseUser(),
                'password': self.settings.getDatabasePassword(),
                'connect_timeout':24 * 60 * 60,
                'client_encoding':'UTF8'
            }
            try:
                self.conn = psycopg2.connect(**connectargs)
                cursor = self.conn.cursor()
                cursor.execute('SELECT version()')
                (version,) = cursor.fetchone()
                self.logger.debug('Connected to server {} running {}', self.settings.getDatabaseHost(), version)
                cursor.close()
            # pylint: disable=broad-except
            except (Exception, psycopg2.DatabaseError) as error:
                self.logger.error('{}', error)
                self.logger.warn("""You may need to create the database and/or a user role first: psql -U postgres -c "CREATE DATABASE {}"; psql -U postgres -c "CREATE ROLE {} WITH LOGIN ENCRYPTED PASSWORD '{}'" """, self.settings.getDatabaseSchema(), self.settings.getDatabaseUser(), self.settings.getDatabasePassword())
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
