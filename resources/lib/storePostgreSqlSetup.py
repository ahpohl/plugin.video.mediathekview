# -*- coding: utf-8 -*-
"""
The local SQlite database module

Copyright 2017-2019, Leo Moll
SPDX-License-Identifier: MIT
"""
# pylint: disable=too-many-lines,line-too-long

import resources.lib.appContext as appContext
import pg8000.dbapi
import sys

class StorePostgreSQLSetup(object):

    def __init__(self, dbCon):
        self.logger = appContext.MVLOGGER.get_new_logger('StorePostgreSQLSetup')
        self.settings = appContext.MVSETTINGS
        self.conn = dbCon
        self._setupScript = """
-- ----------------------------
-- DB V2 
DROP FUNCTION IF EXISTS unix_timestamp();
DROP PROCEDURE IF EXISTS ftUpdateStart;
DROP PROCEDURE IF EXISTS ftUpdateEnd;
DROP PROCEDURE IF EXISTS ftInsertShow;
DROP PROCEDURE IF EXISTS ftInsertChannel;
DROP TABLE IF EXISTS status;
DROP TABLE IF EXISTS show;
DROP TABLE IF EXISTS film;
DROP TABLE IF EXISTS channel;
DROP INDEX IF EXISTS idx_idhash;
DROP INDEX IF EXISTS idx_showname;
DROP INDEX IF EXISTS idx_title;
DROP INDEX IF EXISTS idx_description;
-- ----------------------------
--  Table structure for film
-- ----------------------------
CREATE TABLE film (
    idhash         CHAR(32)        NOT NULL,
    dtCreated      INTEGER         NOT NULL,
    touched        SMALLINT        NOT NULL,
    channel        VARCHAR         NOT NULL,
    showid         CHAR(8)         NOT NULL,
    showname       VARCHAR         NOT NULL,
    title          VARCHAR         NOT NULL,
    aired          INTEGER         NOT NULL,
    duration       INTEGER         NOT NULL,
    description    VARCHAR         NULL,
    url_sub        VARCHAR         NULL,
    url_video      VARCHAR         NULL,
    url_video_sd   VARCHAR         NULL,
    url_video_hd   VARCHAR         NULL
);
CREATE INDEX idx_idhash ON film (idhash);
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX idx_showname ON film USING gin (showname gin_trgm_ops);
CREATE INDEX idx_title ON film USING gin (title gin_trgm_ops);
CREATE INDEX idx_description ON film USING gin (description gin_trgm_ops);
-- ----------------------------
--  Table structure for status
-- ----------------------------
CREATE TABLE status (
    status          VARCHAR         NOT NULL,
    lastupdate      INTEGER         NOT NULL,
    lastFullUpdate  INTEGER         NOT NULL,
    filmupdate      INTEGER         NOT NULL,
    version         INTEGER         NOT NULL
);
-- ----------------
INSERT INTO status values ('UNINIT',0,0,0,3);
--
CREATE OR REPLACE FUNCTION UNIX_TIMESTAMP()
RETURNS bigint AS $$
BEGIN
  RETURN extract(epoch FROM now())::bigint;
END
$$ LANGUAGE plpgsql;
"""

    def setupDatabase(self):
        self.logger.debug('Start DB setup for schema {}', self.settings.getDatabaseSchema())
        try:
            self.conn.getConnection().commit()
            cursor = self.conn.getConnection().cursor()
            cursor.execute(self._setupScript)
            cursor.close()
            self.conn.getConnection().commit()
            self.logger.debug('End DB setup')
        except (Exception, pg8000.native.Error) as error:
            self.logger.error('{}', error)
