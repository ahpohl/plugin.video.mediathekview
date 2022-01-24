# -*- coding: utf-8 -*-
"""
The local SQlite database module

Copyright 2017-2019, Leo Moll
SPDX-License-Identifier: MIT
"""
# pylint: disable=too-many-lines,line-too-long

import resources.lib.appContext as appContext
import psycopg2
from psycopg2.sql import SQL
import sys

class StorePostgreSQLSetup(object):

    def __init__(self, dbCon):
        self.logger = appContext.MVLOGGER.get_new_logger('StorePostgreSQLSetup')
        self.settings = appContext.MVSETTINGS
        self.conn = dbCon
        self._setupScript = SQL("""
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
DROP INDEX IF EXISTS idx_idhash;
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
CREATE INDEX idx_idhash ON film (idhash);
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
        try:
            self.conn.getConnection().commit()
            cursor = self.conn.getConnection().cursor()
            cursor.execute(self._setupScript)
            cursor.close()
            self.conn.getConnection().commit()
            self.logger.debug('End DB setup')
        except (Exception, psycopg2.Error) as error:
            self.logger.error('{}', error)
