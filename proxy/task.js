/**!
 * cnpmjs.org - proxy/task.js
 *
 * Copyright(c) fengmk2 and other contributors.
 * MIT Licensed
 *
 * Authors:
 *   fengmk2 <fengmk2@gmail.com> (http://fengmk2.github.com)
 */

'use strict';

/**
 * Module dependencies.
 */

var mysql = require('../common/mysql');

var GET_FREE_SQL = 'UPDATE task SET status=1, id=LAST_INSERT_ID(id), worker=?, gmt_modified=now() \
  WHERE status = 0 LIMIT 1; \
  SELECT id, json FROM task WHERE ROW_COUNT() > 0 and id = LAST_INSERT_ID();';

exports.getFree = function* (worker) {
  var row = yield mysql.queryOne(GET_FREE_SQL, [worker]);
  if (row) {
    row.json = JSON.parse(row.json);
  }
  return row;
};

var LIST_TIMEOUT_SQL = 'SELECT id, json FROM task \
  WHERE status = 1 and gmt_modified < DATE_SUB(now(), INTERVAL ? SECOND);';

exports.listTimeouts = function* (timeout) {
  var rows = yield mysql.query(LIST_TIMEOUT_SQL, [timeout]);
  return rows.map(function (row) {
    row.json = JSON.parse(row.json);
    return row;
  });
};

var RESET_SQL = 'UPDATE task SET status=0, gmt_modified=now() WHERE id in (?);';

exports.reset = function* (ids) {
  return yield mysql.query(RESET_SQL, [ids]);
};

var UPDATE_SQL = 'UPDATE task SET status=?, progress=?, error=?, gmt_modified=now() \
  WHERE id=?;';

exports.update = function* (id, info) {
  return yield mysql.query(UPDATE_SQL, [
    info.status, info.progress, info.error, id
  ]);
};
