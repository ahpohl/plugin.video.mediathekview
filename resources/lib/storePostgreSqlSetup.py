# -*- coding: utf-8 -*-
"""
The local SQlite database module

Copyright 2017-2019, Leo Moll
SPDX-License-Identifier: MIT
"""
# pylint: disable=too-many-lines,line-too-long

import resources.lib.appContext as appContext
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

class StorePostgreSQLSetup(object):

    def __init__(self, dbCon):
        self.logger = appContext.MVLOGGER.get_new_logger('StorePostgreSQLSetup')
        self.settings = appContext.MVSETTINGS
        self.conn = dbCon
        self._setupSchema = sql.SQL("CREATE DATABASE {}".format(self.settings.getDatabaseSchema()))
        self._setupScript = """
-- ----------------------------
-- DB V2 
DROP PROCEDURE IF EXISTS `ftUpdateStart`;
DROP PROCEDURE IF EXISTS `ftUpdateEnd`;
DROP PROCEDURE IF EXISTS `ftInsertShow`;
DROP PROCEDURE IF EXISTS `ftInsertChannel`;
DROP TABLE IF EXISTS `status`;
DROP TABLE IF EXISTS `show`;
DROP TABLE IF EXISTS `film`;
DROP TABLE IF EXISTS `channel`;
-- ----------------------------
--  Table structure for film
-- ----------------------------
DROP TABLE IF EXISTS film;
CREATE TABLE film (
    idhash         char(32)        NOT NULL,
    dtCreated      integer(11)     NOT NULL,
    touched        smallint(1)     NOT NULL,
    channel        varchar(32)     NOT NULL,
    showid         char(8)         NOT NULL,
    showname       varchar(128)    NOT NULL,
    title          varchar(128)    NOT NULL,
    aired          integer(11)     NOT NULL,
    duration       integer(11)     NOT NULL,
    description    varchar(1024)   NULL,
    url_sub        varchar(384)    NULL,
    url_video      varchar(384)    NULL,
    url_video_sd   varchar(384)    NULL,
    url_video_hd   varchar(384)    NULL
) ENGINE=InnoDB CHARSET=utf8;
--
CREATE INDEX idx_idhash ON film (idhash);
-- ----------------------------
--  Table structure for status
-- ----------------------------
DROP TABLE IF EXISTS status;
CREATE TABLE status (
    status          varchar(255)    NOT NULL,
    lastupdate      int(11)         NOT NULL,
    lastFullUpdate  int(11)         NOT NULL,
    filmupdate      int(11)         NOT NULL,
    version         int(11)         NOT NULL
) ENGINE=InnoDB;
-- ----------------
INSERT INTO status values ('UNINIT',0,0,0,3);
--
"""

    def setupDatabase(self):
        self.logger.debug('Start DB setup for schema {}', self.settings.getDatabaseSchema())
        print('Start DB setup for schema {}'.format(self.settings.getDatabaseSchema()))
        #
        #
        try:
            self.conn.database = self.settings.getDatabaseSchema()
            cursor = self.conn.getConnection().cursor()
            cursor.execute(sql.SQL("SELECT datname FROM pg_database where datname = '{}'").format(sql.Identifier(self.settings.getDatabaseSchema())))
            if cursor.rowcount > 0:
                self.logger.debug('PostgreSql Schema exists - no action')
                print('PostgreSql Schema exists - no action')
            else:
                raise Exception('DB', 'DB')
        except Exception:
            self.logger.debug('PostgreSql Schema does not exists - setup schema')
            print('PostgreSql Schema does not exists - setup schema')
            cursor = self.conn.getConnection().cursor()
            cursor.execute(self._setupSchema)
            cursor.close()
            self.conn.database = self.settings.getDatabaseSchema()
        #
        con = self.conn.getConnection()
        cursor = con.cursor()
        for result in cursor.execute(self._setupScript, multi=True):
          if result.with_rows:
            self.logger.debug("Rows produced by statement '{}':".format(result.statement))
            self.logger.debug(result.fetchall())
          else:
            self.logger.debug("Number of rows affected by statement '{}': {}".format(result.statement, result.rowcount))
        cursor.close()
        con.commit()
        self.logger.debug('End DB setup')
