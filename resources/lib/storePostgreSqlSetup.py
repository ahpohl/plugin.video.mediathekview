# -*- coding: utf-8 -*-
"""
The local SQlite database module

Copyright 2017-2019, Leo Moll
SPDX-License-Identifier: MIT
"""
# pylint: disable=too-many-lines,line-too-long

import resources.lib.appContext as appContext
from psycopg2 import sql

class StorePostgreSQLSetup(object):

    def __init__(self, dbCon):
        self.logger = appContext.MVLOGGER.get_new_logger('StorePostgreSQLSetup')
        self.settings = appContext.MVSETTINGS
        self.conn = dbCon
        self._setupSchema = sql.SQL("CREATE DATABASE {}".format(self.settings.getDatabaseSchema()))
        self._setupScript = sql.SQL("""
-- ----------------------------
-- DB V2 
DROP PROCEDURE IF EXISTS ftUpdateStart;
DROP PROCEDURE IF EXISTS ftUpdateEnd;
DROP PROCEDURE IF EXISTS ftInsertShow;
DROP PROCEDURE IF EXISTS ftInsertChannel;
DROP TABLE IF EXISTS status;
DROP TABLE IF EXISTS show;
DROP TABLE IF EXISTS film;
DROP TABLE IF EXISTS channel;
-- ----------------------------
--  Table structure for film
-- ----------------------------
DROP TABLE IF EXISTS film;
CREATE TABLE film (
    idhash         CHAR(32)        NOT NULL,
    dtCreated      INTEGER         NOT NULL,
    touched        SMALLINT        NOT NULL,
    channel        VARCHAR(32)     NOT NULL,
    showid         CHAR(8)         NOT NULL,
    showname       VARCHAR(128)    NOT NULL,
    title          VARCHAR(128)    NOT NULL,
    aired          INTEGER         NOT NULL,
    duration       INTEGER         NOT NULL,
    description    VARCHAR(1024)   NULL,
    url_sub        VARCHAR(384)    NULL,
    url_video      VARCHAR(384)    NULL,
    url_video_sd   VARCHAR(384)    NULL,
    url_video_hd   varchar(384)    NULL
);
-- ----------------------------
--  Table structure for status
-- ----------------------------
DROP TABLE IF EXISTS status;
CREATE TABLE status (
    status          VARCHAR(255)    NOT NULL,
    lastupdate      INTEGER         NOT NULL,
    lastFullUpdate  INTEGER         NOT NULL,
    filmupdate      INTEGER         NOT NULL,
    version         INTEGER         NOT NULL
);
-- ----------------
INSERT INTO status values ('UNINIT',0,0,0,3);
--
""")

    def setupDatabase(self):
        self.logger.debug('Start DB setup for schema {}', self.settings.getDatabaseSchema())
        print('Start DB setup for schema {}'.format(self.settings.getDatabaseSchema()))
        #
        #
        try:
            self.conn.database = self.settings.getDatabaseSchema()
            con = self.conn.getConnection()
            cursor = con.cursor()
            cursor.execute("SELECT exists(SELECT datname FROM pg_database WHERE datname = %s)", (self.settings.getDatabaseSchema(),))
            if cursor.fetchone()[0]:
                self.logger.debug('PostgreSql Schema exists - no action')
                print('PostgreSql Schema exists - no action')
            else:
                raise Exception('DB', 'DB')
        except Exception:
            self.logger.debug('PostgreSql Schema does not exists - setup schema')
            print('PostgreSql Schema does not exists - setup schema')
            con = self.conn.getConnection()
            cursor = con.cursor()
            cursor.execute(self._setupSchema)
            cursor.close()
            self.conn.database = self.settings.getDatabaseSchema()
        self.conn.exit()
        self.conn.database = self.settings.getDatabaseSchema()
        con = self.conn.getConnection()
        con.autocommit = False
        cursor = con.cursor()
        #cursor.execute(self._setupScript)
        cursor.execute("SELECT current_database()")
        print(cursor.fetchone()[0])
        con.commit()
        cursor.close()
        self.logger.debug('End DB setup')
        print('End DB setup')
