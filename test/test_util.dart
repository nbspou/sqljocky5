library sqljocky.test.test_util;

import 'dart:async';

import 'package:sqljocky5/sqljocky.dart';
import 'package:sqljocky5/utils.dart';
import 'package:test/test.dart';

Future<void> setup(ConnectionPool pool, String tableName, String createSql,
    [String insertSql]) async {
  await new TableDropper(pool, [tableName]).dropTables();
  var result = await pool.query(createSql);
  expect(result, isNotNull);
  if (insertSql != null) {
    await pool.query(insertSql);
  }
}
